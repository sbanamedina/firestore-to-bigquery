import os
import re
import json
import math
import threading
import concurrent.futures
from decimal import Decimal
from datetime import datetime, timezone, timedelta
import ssl

import functions_framework
from google.cloud import firestore, bigquery, secretmanager
from google.oauth2 import service_account
import sys
import logging
from google.api_core.retry import Retry
from google.api_core.exceptions import GoogleAPICallError
import time
from google.api_core.exceptions import ServiceUnavailable, DeadlineExceeded, GoogleAPICallError, RetryError
from google.cloud.firestore_v1.base_query import FieldFilter
import unicodedata
from dateutil import parser


def safe_sql_string(value):
    """
    Limpia cadenas para evitar errores de codificación o caracteres ilegales en SQL.
    """
    if isinstance(value, str):
        # Asegura UTF-8 válido y escapa comillas simples
        clean = value.encode("utf-8", errors="ignore").decode("utf-8")
        clean = clean.replace("'", "\\'").replace("\n", " ").replace("\r", " ")
        return clean
    return value

def normalize_field_name(name):
    # Elimina acentos y caracteres especiales
    nfkd = unicodedata.normalize('NFKD', name)
    name = "".join([c for c in nfkd if not unicodedata.combining(c)])
    name = re.sub(r'\W+', '_', name)  # Sustituye todo lo no alfanumérico por "_"
    return name.lower()


def safe_stream(query, max_attempts=5, base_backoff=1.0):
    """Ejecuta un query Firestore con reintento y backoff exponencial."""
    for attempt in range(1, max_attempts + 1):
        try:
            return list(query.stream())

        except (ServiceUnavailable, DeadlineExceeded, GoogleAPICallError, ssl.SSLError) as e:
            backoff = base_backoff * (2 ** (attempt - 1))
            print(f"⚠️ safe_stream: intento {attempt}/{max_attempts} falló ({e}). Reintentando en {backoff}s...")
            sys.stdout.flush()
            time.sleep(backoff)
    raise RuntimeError("❌ safe_stream: todos los intentos fallaron")

def safe_list_subcollections(doc_ref, max_attempts=3, base_backoff=1.0):
    """Lista subcolecciones con reintentos para evitar timeouts/EOF."""
    for attempt in range(1, max_attempts + 1):
        try:
            return list(doc_ref.collections())
        except (ServiceUnavailable, DeadlineExceeded, GoogleAPICallError, ssl.SSLError) as e:
            backoff = base_backoff * (2 ** (attempt - 1))
            print(f"⚠️ safe_list_subcollections: intento {attempt}/{max_attempts} falló ({e}). Reintentando en {backoff}s...")
            sys.stdout.flush()
            time.sleep(backoff)
    print(f"❌ safe_list_subcollections: todos los intentos fallaron para {doc_ref.path}")
    sys.stdout.flush()
    return []

def flush_batch_to_bq(file_path, bigquery_client, table_ref, current_fields):
    """Carga el lote actual a BigQuery permitiendo columnas nuevas y limpia el archivo."""
    schema = [bigquery.SchemaField(f, "STRING", mode="NULLABLE") for f in current_fields]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        autodetect=False,
        max_bad_records=500,
        ignore_unknown_values=True,
    )
    try:
        with open(file_path, "rb") as source_file:
            print("🚀 Enviando lote a BigQuery y liberando RAM...")
            sys.stdout.flush()
            bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config).result()
    except Exception as e:
        print(f"⚠️ Error al cargar el lote en BigQuery: {e}")
        sys.stdout.flush()
    # Limpia el archivo para el siguiente lote
    open(file_path, "w").close()

def ensure_table_fields(bigquery_client, table_ref, fields):
    """Agrega columnas faltantes en la tabla destino antes del MERGE."""
    try:
        table = bigquery_client.get_table(table_ref)
        existing = {schema_field.name for schema_field in table.schema}
        missing = [f for f in fields if f not in existing]
        if not missing:
            return
        add_clause = ", ".join([f"ADD COLUMN IF NOT EXISTS `{c}` STRING" for c in missing])
        ddl = f"ALTER TABLE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` {add_clause}"
        print(f"🧭 Agregando columnas faltantes: {missing}")
        sys.stdout.flush()
        bigquery_client.query(ddl).result()
    except Exception as e:
        print(f"⚠️ No se pudieron agregar columnas faltantes: {e}")
        sys.stdout.flush()
# -------------------------------
# Acceso a secretos
# -------------------------------
def access_secret_version(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# -------------------------------
# Serialización segura para BigQuery
# -------------------------------
def serialize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, list):
        return [serialize_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    elif isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value
    elif isinstance(value, str):
        value = re.sub(r'[\x00-\x1f\x7f]', ' ', value)
        value = re.sub(r'[\u200B-\u200D\uFEFF\u2028\u2029]', ' ', value)
        return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').strip()
    elif value is None:
        return None
    else:
        try:
            json.dumps(value)
            return value
        except Exception:
            return str(value)

# -------------------------------
# Flatten de documentos
# -------------------------------
def flatten_dict(d, parent_key='', sep='_', level=1, max_level=2):
    items = {}
    if level > max_level:
        items[parent_key] = json.dumps(d)
        return items

    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        new_key = re.sub(r'\W+', '_', new_key).lower()
        v = serialize_value(v)
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep, level+1, max_level))
        elif isinstance(v, list):
            items[new_key] = json.dumps(v)
        else:
            items[new_key] = v
    return items

# -------------------------------
# Procesamiento de documentos y colecciones
# -------------------------------

def process_document(firestore_client, doc_snapshot, parent_path='', sep='_', max_level=2,
                     handle_subcollections=False, updated_after=None, updated_before=None, updated_field=None):
    fields = set()
    example_docs = []

    # Usamos el snapshot ya obtenido para evitar llamadas extra
    doc = doc_snapshot
    doc_ref = doc.reference

    if not doc.exists:
        return example_docs, fields
    doc_data = doc.to_dict()

    # Validación incremental robusta (incluye updated_before y múltiples formatos)
    if (updated_after or updated_before) and updated_field:
        if updated_field in doc_data and doc_data[updated_field]:
            doc_value = doc_data[updated_field]

            parsed_dt = None
            if isinstance(doc_value, datetime):
                parsed_dt = doc_value
            elif isinstance(doc_value, (int, float)):
                # Interpretar numérico como epoch (ms si es muy grande)
                ts = float(doc_value)
                if ts > 1e12:  # milisegundos
                    ts = ts / 1000.0
                try:
                    parsed_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                except Exception:
                    parsed_dt = None
            else:
                try:
                    parsed_dt = parser.parse(str(doc_value), fuzzy=True, dayfirst=True)
                except Exception:
                    print(f"⚠️ No se pudo parsear '{doc_value}' en {doc_ref.id}; se incluirá por seguridad.")
                    parsed_dt = None

            if parsed_dt is not None:
                # Normalizar zona horaria
                if parsed_dt.tzinfo is None:
                    parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                else:
                    parsed_dt = parsed_dt.astimezone(timezone.utc)

                if updated_after:
                    ua = updated_after if updated_after.tzinfo else updated_after.replace(tzinfo=timezone.utc)
                    if parsed_dt <= ua:
                        return example_docs, fields
                if updated_before:
                    ub = updated_before if updated_before.tzinfo else updated_before.replace(tzinfo=timezone.utc)
                    if parsed_dt > ub:
                        return example_docs, fields

    doc_data['id'] = doc.id
    doc_data['document_path'] = parent_path + sep + doc.id
    flattened_data = flatten_dict(doc_data, sep=sep, max_level=max_level)
    example_docs.append(flattened_data)
    fields.update(flattened_data.keys())

    if handle_subcollections:
        try:
            for subcollection in safe_list_subcollections(doc_ref):
                subcollection_path = f"{parent_path}{sep}{subcollection.id}" if parent_path else subcollection.id
                try:
                    for sub_doc in safe_stream(subcollection):
                        sub_docs, sub_fields = process_document(
                            firestore_client,
                            sub_doc,
                            parent_path=subcollection_path,
                            sep=sep,
                            max_level=max_level,
                            handle_subcollections=True,
                            updated_after=updated_after,
                            updated_field=updated_field
                        )
                        example_docs.extend(sub_docs)
                        fields.update(sub_fields)
                except Exception as e:
                    print(f"⚠️ Error al procesar subcollection {subcollection.id} de {doc_ref.path}: {e}")
                    sys.stdout.flush()
                    continue
        except Exception as e:
            print(f"⚠️ Error al listar subcollections de {doc_ref.path}: {e}")
            sys.stdout.flush()

    return example_docs, fields

def process_collection(
    firestore_client,
    collection_name,
    sep="_",
    max_level=2,
    page_size=500,
    handle_subcollections=False,
    updated_after=None,
    updated_before=None,
    updated_field=None,
    max_workers=32,
    temp_file_path=None,
    bq_client=None,
    temp_table_ref=None,
    batch_size=50000,
    name_prefix=None,
    name_range_start=None,
    name_range_end=None,
):
    fields = set()
    total_docs = 0
    records_in_current_batch = 0
    collection_ref = firestore_client.collection(collection_name)
    range_start = None
    range_end = None
    last_doc = None
    can_order_by_updated_field = True
    fallback_by_name = False

    # -----------------------
    # Filtros por nombre (__name__) para particionar (usaremos start_at / end_at)
    # -----------------------
    if updated_field and (name_prefix or name_range_start or name_range_end):
        print("⚠️ Se ignoran name_prefix/name_range porque hay filtro por updated_field (restricción Firestore).")
        sys.stdout.flush()
        range_start = None
        range_end = None
        can_order_by_updated_field = True
        fallback_by_name = False
    else:
        if name_prefix:
            range_start = name_prefix
            range_end = name_prefix + "\uf8ff"
            can_order_by_updated_field = False
            fallback_by_name = True
        if name_range_start or name_range_end:
            range_start = name_range_start
            range_end = name_range_end
            can_order_by_updated_field = False
            fallback_by_name = True

    # -----------------------
    # Filtro incremental robusto
    # -----------------------
    if updated_field and (updated_after or updated_before):
        try:
            # Si no hay updated_before, usamos "ahora" como límite superior
            if updated_after and not updated_before:
                updated_before = datetime.now(timezone.utc)
                print(f"🔹 No se proporcionó updated_before, se usará ahora como límite superior: {updated_before}")
                sys.stdout.flush()

            # Obtener un valor de ejemplo
            sample_doc = next(firestore_client.collection(collection_name).limit(1).stream(), None)
            sample_value = sample_doc.to_dict().get(updated_field) if sample_doc else None

            if isinstance(sample_value, datetime):
                # Campo tipo Timestamp
                if updated_after:
                    collection_ref = collection_ref.where(filter=FieldFilter(updated_field, ">", updated_after))
                if updated_before:
                    collection_ref = collection_ref.where(filter=FieldFilter(updated_field, "<=", updated_before))
                #collection_ref = collection_ref.order_by(updated_field).order_by("__name__")
                print(f"🧭 Campo {updated_field} detectado como TIMESTAMP, filtro aplicado.")
                sys.stdout.flush()
            elif isinstance(sample_value, str):
                # Campo tipo STRING con formato SQL "YYYY-MM-DD HH:MM:SS"
                if updated_after:
                    updated_after_str = updated_after.strftime("%Y-%m-%d %H:%M:%S")
                    collection_ref = collection_ref.where(filter=FieldFilter(updated_field, ">", updated_after_str))
                if updated_before:
                    updated_before_str = updated_before.strftime("%Y-%m-%d %H:%M:%S")
                    collection_ref = collection_ref.where(filter=FieldFilter(updated_field, "<=", updated_before_str))
                #collection_ref = collection_ref.order_by(updated_field)
                print(f"🧭 Campo {updated_field} detectado como STRING con formato SQL, filtro aplicado: {updated_after_str} → {updated_before_str}")
                sys.stdout.flush()
            else:
                print(f"⚠️ Tipo de campo {updated_field} no soportado, filtro omitido.")
                sys.stdout.flush()
                can_order_by_updated_field = False
                fallback_by_name = True
        except Exception as e:
            print(f"⚠️ No se pudo aplicar filtro por {updated_field}: {e}")
            sys.stdout.flush()
    # -----------------------
    # Paginación y procesamiento de documentos
    # -----------------------
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        effective_page_size = page_size if can_order_by_updated_field else min(page_size, 500)
        if fallback_by_name:
            print(f"🔁 Fallback activo: ordenando por __name__ con page_size={effective_page_size}")
            sys.stdout.flush()
        while True:
            # Si el tipo del campo no es confiable, ordena por __name__ para garantizar resultados
            if updated_field and can_order_by_updated_field:
                query = collection_ref.order_by(updated_field).order_by("__name__").limit(effective_page_size)
            else:
                query = collection_ref.order_by("__name__").limit(effective_page_size)

            if range_start:
                query = query.start_at([range_start])
            if range_end:
                query = query.end_at([range_end])

            if last_doc:
                # Paginar usando el snapshot para evitar cursores compuestos
                query = query.start_after(last_doc)

            docs = safe_stream(query)
            if not docs:
                break

            batch_count = 0
            futures = [
                executor.submit(
                    process_document,
                    firestore_client,
                    doc,
                    f"{collection_name}{sep}{doc.id}",
                    sep,
                    max_level,
                    handle_subcollections,
                    updated_after,
                    updated_before,
                    updated_field,
                )
                for doc in docs
            ]

            with open(temp_file_path, "a", encoding="utf-8") as stream_file:
                for future in concurrent.futures.as_completed(futures):
                    try:
                        doc_docs, doc_fields = future.result()
                    except Exception as e:
                        print(f"⚠️ Error procesando batch en {collection_name}: {e}")
                        sys.stdout.flush()
                        continue

                    fields.update(normalize_field_name(k) for k in doc_fields)
                    batch_count += len(doc_docs)
                    records_in_current_batch += len(doc_docs)
                    if temp_file_path and bq_client and temp_table_ref:
                        for doc in doc_docs:
                            clean_doc = {normalize_field_name(k): safe_sql_string(serialize_value(v)) for k, v in doc.items()}
                            stream_file.write(json.dumps(clean_doc, ensure_ascii=False) + '\n')

            total_docs += batch_count
            if total_docs % 500 == 0 or len(docs) < page_size:
                print(f"📊 Progreso: {total_docs} documentos procesados hasta ahora...")
                sys.stdout.flush()

            if temp_file_path and bq_client and temp_table_ref and records_in_current_batch >= batch_size:
                current_fields = list(set(normalize_field_name(f) for f in fields))
                flush_batch_to_bq(temp_file_path, bq_client, temp_table_ref, current_fields)
                records_in_current_batch = 0

            last_doc = docs[-1]
            if len(docs) < page_size:
                break

    if temp_file_path and bq_client and temp_table_ref and records_in_current_batch > 0:
        current_fields = list(set(normalize_field_name(f) for f in fields))
        flush_batch_to_bq(temp_file_path, bq_client, temp_table_ref, current_fields)
    return total_docs, fields

# -------------------------------
# Control de ejecución duplicada en BigQuery
# -------------------------------
def was_recently_executed_bq(collection_name: str, database_name: str, window_minutes: int = 5) -> bool:
    client = bigquery.Client(project='sb-operacional-zone')
    dataset_id = "dataops"
    table_id = "t_firestore_log_function_locks"
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"
    try:
        client.get_table(full_table_id)
    except Exception:
        schema = [
            bigquery.SchemaField("collection", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("database", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("execution_time", "TIMESTAMP", mode="REQUIRED"),
        ]
        client.create_table(bigquery.Table(full_table_id, schema=schema))
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    #window_start = now - timedelta(minutes=window_minutes)
    window_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    query = f"""
        SELECT COUNT(*) as total
        FROM `{full_table_id}`
        WHERE collection = @collection AND database = @database AND execution_time > @window_start
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("collection", "STRING", collection_name),
            bigquery.ScalarQueryParameter("database", "STRING", database_name),
            bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
        ]
    )
    result = list(client.query(query, job_config=job_config).result())
    if result and result[0].total > 0:
        return True
    # Insertar registro de ejecución
    client.insert_rows_json(client.dataset(dataset_id).table(table_id), [{"collection": collection_name, "database": database_name, "execution_time": now.isoformat()}])
    return False

def get_last_updated_field_from_bq(dataset: str, table_name: str, updated_field: str) -> datetime:
    """
    Devuelve el valor máximo del campo `updated_field` en la tabla BigQuery indicada.
    """
    client = bigquery.Client(project='sb-operacional-zone')
    full_table_id = f"{client.project}.{dataset}.{table_name}"

    query = f"""
        SELECT MAX({updated_field}) AS last_updated
        FROM `{full_table_id}`
    """

    result = list(client.query(query).result())
    if result and result[0].last_updated:
        # Retornamos como datetime con timezone UTC
        if isinstance(result[0].last_updated, datetime):
            return result[0].last_updated.replace(tzinfo=timezone.utc)
        else:
            # En caso de que venga como string
            return datetime.fromisoformat(str(result[0].last_updated)).replace(tzinfo=timezone.utc)
    return None

def get_all_ids_paged(collection_ref, page_size=500, max_attempts=5, base_backoff=1.0):
    all_ids = []
    last_doc = None
    attempt = 0

    while True:
        try:
            attempt = 0
            q = collection_ref.limit(page_size)
            if last_doc:
                q = q.start_after(last_doc)
            docs = list(q.stream())  
            if not docs:
                break
            for d in docs:
                all_ids.append({'id': d.id})
            last_doc = docs[-1]
            if len(docs) < page_size:
                break
        except (ServiceUnavailable, DeadlineExceeded, GoogleAPICallError) as e:
            attempt += 1
            if attempt > max_attempts:
                print(f"❌ get_all_ids_paged: max attempts reached: {e}")
                sys.stdout.flush()
                raise
            backoff = base_backoff * (2 ** (attempt - 1))
            print(f"⚠️ get_all_ids_paged: excepción {e}. Reintentando en {backoff}s (intento {attempt}/{max_attempts})")
            sys.stdout.flush()
            time.sleep(backoff)
            continue
    return all_ids

# -------------------------------
# Función HTTP principal
# -------------------------------
@functions_framework.http
def export_firestore_to_bigquery(request):
    logging.info("✅ Request entró a la función")
    start_time = datetime.now(timezone.utc)
    request_json = request.get_json(silent=True)
    print(f'🔹 Payload recibido: {request_json}')
    sys.stdout.flush()

    if not request_json or 'collection' not in request_json or 'table' not in request_json:
        return ({'error': 'Missing collection or table parameter in request'}), 400

    target_project_id = request_json.get('project_id', 'sb-xops-prod')

    PROJECT_CONFIG = {
        'sb-xops-prod': {
            # Si en xops NO tiene permisos IAM directos, se mantiene el secreto:
            'method': 'secret',
            'secret_name': 'sb-xops-prod_appspot_gserviceaccount'
        },
        'sb-iacorredores-prod': {
            # Se usa 'adc' porque ya tiene permisos IAM
            'method': 'adc', 
            'secret_name': None 
        },
        'sb-iadaia-cap-dev': {
            'method': 'adc',
            'secret_name': None
        }
    }

    if target_project_id not in PROJECT_CONFIG:
        # Si llega un proyecto no configurado, intentamos usar ADC por defecto
        print(f"⚠️ Proyecto {target_project_id} no explícito en config. Intentando usar ADC...")
        config = {'method': 'adc'}
    else:
        config = PROJECT_CONFIG[target_project_id]

    var_main_collection = request_json['collection']
    var_table_id = request_json['table']
    handle_subcollections = request_json.get('handle_subcollections', False)
    var_database = request_json.get('database', '(default)')
    updated_field = request_json.get('updated_field')  # Nombre del campo en Firestore
    full_export = request_json.get('full_export', False)
    page_size = request_json.get('page_size', 2000)
    max_workers = request_json.get('max_workers', 32)
    chunk_hours = request_json.get('chunk_hours')  # Opcional: particionar por ventanas de tiempo (requiere updated_field)
    name_prefixes = request_json.get('name_prefixes')  # Opcional: lista de prefijos para particionar por __name__
    name_ranges = request_json.get('name_ranges')  # Opcional: lista de rangos {start, end} para particionar por __name__
    batch_size = request_json.get('batch_size', 50000)  # Tamaño de lote para flush a BigQuery
    print(f'🟢 Parámetros -> Project: {target_project_id}, Collection: {var_main_collection}, Table: {var_table_id}, Subcollections: {handle_subcollections}, DB: {var_database}')
    sys.stdout.flush()
    
    # if was_recently_executed_bq(var_main_collection, var_database):
    #     print(f"⛔ Ya se ejecutó recientemente para la colección: {var_main_collection}. Cancelando ejecución.")
    #     sys.stdout.flush()
    #     return f"Duplicate execution for collection {var_main_collection}. Skipping.", 200    

    updated_after = None
    updated_before = None

    updated_after_str = request_json.get('updated_after')
    updated_before_str = request_json.get('updated_before')

    # Solo consultar BigQuery si no se pasaron manualmente las fechas
    if not full_export and updated_field and not updated_after_str and not updated_before_str:
        updated_after = get_last_updated_field_from_bq(
            dataset='firestore',
            table_name=var_table_id,
            updated_field=updated_field
        )
        print(f"🔹 Última fecha de actualización en BigQuery: {updated_after}")
        sys.stdout.flush()

    # Convertir manualmente los valores enviados
    if updated_after_str:
        try:
            updated_after = datetime.fromisoformat(updated_after_str.replace("Z", "+00:00"))
        except ValueError:
            print("⚠️ No se pudo convertir updated_after, se ignora.")
            updated_after = None

    if updated_before_str:
        try:
            updated_before = datetime.fromisoformat(updated_before_str.replace("Z", "+00:00"))
        except ValueError:
            print("⚠️ No se pudo convertir updated_before, se ignora.")
            updated_before = None

    print(f"🔹 Filtro de fechas -> updated_after: {updated_after}, updated_before: {updated_before}")
    sys.stdout.flush()
  

    # Cargar secretos y crear clientes
    #print("🔹 Cargando secretos y creando clientes")
    #sys.stdout.flush()
    #service_account_info = json.loads(access_secret_version("sb-operacional-zone", selected_config['secret_name']))
    #firestore_client = firestore.Client(credentials=service_account.Credentials.from_service_account_info(service_account_info), project=target_project_id, database=var_database)
    
    print(f"🔹 Conectando a Firestore en: {target_project_id} usando método: {config['method']}")
    sys.stdout.flush()

    try:
        if config['method'] == 'secret':
            # Usando archivo JSON desde Secret Manager
            secret_payload = access_secret_version("sb-operacional-zone", config['secret_name'])
            service_account_info = json.loads(secret_payload)
            creds = service_account.Credentials.from_service_account_info(service_account_info)
            
            firestore_client = firestore.Client(
                credentials=creds, 
                project=target_project_id, 
                database=var_database
            )
        else:
            # MODO MODERNO (ADC): Usando los permisos IAM de la Cloud Run
            # No pasamos 'credentials', la librería usa la identidad de service-account-oper-zone
            firestore_client = firestore.Client(
                project=target_project_id, 
                database=var_database
            )
            
    except Exception as e:
        print(f"❌ Error inicializando Firestore para {target_project_id}: {e}")
        return ({'error': f'Auth failed for {target_project_id}'}), 500

    bigquery_client = bigquery.Client(project='sb-operacional-zone')
    var_dataset_id = 'firestore'
    temp_table_id = var_table_id + "_temp"
    temp_table_ref = bigquery_client.dataset(var_dataset_id).table(temp_table_id)
    try:
        bigquery_client.delete_table(temp_table_ref, not_found_ok=True)
    except Exception as e:
        print(f"⚠️ No se pudo eliminar tabla temporal previa: {e}")
        sys.stdout.flush()
    bigquery_client.create_table(bigquery.Table(temp_table_ref, schema=[bigquery.SchemaField("id", "STRING")]), exists_ok=True)

    # Procesar colección
    print(f"🔍 Procesando colección: {var_main_collection}")
    sys.stdout.flush()
    temp_file_path = '/tmp/firestore_data.json'
    open(temp_file_path, "w").close()  # limpiar archivo
    total_docs = 0
    fields = set()
    if name_ranges:
        for r in name_ranges:
            start = r.get("start")
            end = r.get("end")
            print(f"🔀 Rango __name__: {start} -> {end}")
            sys.stdout.flush()
            part_docs, part_fields = process_collection(
                firestore_client,
                var_main_collection,
                page_size=page_size,
                handle_subcollections=handle_subcollections,
                updated_after=updated_after,
                updated_before=updated_before,
                updated_field=updated_field,
                max_workers=max_workers,
                temp_file_path=temp_file_path,
                bq_client=bigquery_client,
                temp_table_ref=temp_table_ref,
                batch_size=batch_size,
                name_range_start=start,
                name_range_end=end
            )
            total_docs += part_docs
            fields.update(part_fields)
    elif name_prefixes:
        for prefix in name_prefixes:
            print(f"🔀 Prefijo __name__: {prefix}")
            sys.stdout.flush()
            part_docs, part_fields = process_collection(
                firestore_client,
                var_main_collection,
                page_size=page_size,
                handle_subcollections=handle_subcollections,
                updated_after=updated_after,
                updated_before=updated_before,
                updated_field=updated_field,
                max_workers=max_workers,
                temp_file_path=temp_file_path,
                bq_client=bigquery_client,
                temp_table_ref=temp_table_ref,
                batch_size=batch_size,
                name_prefix=prefix
            )
            total_docs += part_docs
            fields.update(part_fields)
    elif chunk_hours and updated_field:
        end_time = updated_before or datetime.now(timezone.utc)
        cursor_time = updated_after or datetime(1970, 1, 1, tzinfo=timezone.utc)
        while cursor_time < end_time:
            window_end = min(cursor_time + timedelta(hours=chunk_hours), end_time)
            print(f"⏱️ Ventana: {cursor_time} -> {window_end}")
            sys.stdout.flush()
            window_docs, window_fields = process_collection(
                firestore_client,
                var_main_collection,
                page_size=page_size,
                handle_subcollections=handle_subcollections,
                updated_after=cursor_time,
                updated_before=window_end,
                updated_field=updated_field,
                max_workers=max_workers,
                temp_file_path=temp_file_path,
                bq_client=bigquery_client,
                temp_table_ref=temp_table_ref,
                batch_size=batch_size
            )
            total_docs += window_docs
            fields.update(window_fields)
            cursor_time = window_end
    else:
        total_docs, fields = process_collection(
            firestore_client,
            var_main_collection,
            page_size=page_size,
            handle_subcollections=handle_subcollections,
            updated_after=updated_after,
            updated_before=updated_before,
            updated_field=updated_field,
            max_workers=max_workers,
            temp_file_path=temp_file_path,
            bq_client=bigquery_client,
            temp_table_ref=temp_table_ref,
            batch_size=batch_size
        )

    ###### Para colecciones que se bloquean
    # total_docs, fields = process_collection(firestore_client, bigquery_client,var_dataset_id,var_table_id,var_main_collection, page_size=page_size, handle_subcollections=handle_subcollections,updated_after=updated_after,updated_before=updated_before,updated_field=updated_field)
    ###############################
    if total_docs == 0:
        print(f"⛔ No se encontraron documentos en la colección: {var_main_collection}")
        sys.stdout.flush()
        return ({'error': 'No documents found in the Firestore collection'}), 200

    print(f"✅ Documentos extraídos: {total_docs}")
    sys.stdout.flush()

    # Normalizar nombres de campos para evitar errores en BigQuery
    fields = list(set(normalize_field_name(f) for f in fields))
    schema = [bigquery.SchemaField(f, "STRING", mode="NULLABLE") for f in fields]
    table_ref = bigquery_client.dataset(var_dataset_id).table(var_table_id)
    bigquery_client.create_table(bigquery.Table(table_ref, schema=schema), exists_ok=True)
    ensure_table_fields(bigquery_client, table_ref, fields)

    # -----------------------
    # MERGE / DELETE si incremental
    # -----------------------
    if not full_export:
        print('📝 Merge / Delete si incremental...')
        sys.stdout.flush()
        ensure_table_fields(bigquery_client, table_ref, fields)
        merge_sql = f"""
            MERGE `{var_dataset_id}.{var_table_id}` T
            USING `{var_dataset_id}.{temp_table_id}` S
            ON T.id = S.id
            WHEN MATCHED THEN UPDATE SET {', '.join([f'T.{f} = S.{f}' for f in fields])}
            WHEN NOT MATCHED THEN INSERT ({', '.join(fields)}) VALUES ({', '.join([f'S.{f}' for f in fields])})
        """
        merge_job = bigquery_client.query(merge_sql)
        merge_result = merge_job.result()
        print(f"✅ Merge completado: {merge_result.num_dml_affected_rows} filas afectadas (insert/update)")
        sys.stdout.flush()

        # print("📝 Obteniendo todos los IDs actuales de Firestore para manejar eliminados (paginado)...")
        # sys.stdout.flush()
        # collection_ref = firestore_client.collection(var_main_collection)
        # all_ids = get_all_ids_paged(collection_ref, page_size=page_size)
        # print(f"✅ IDs obtenidos: {len(all_ids)}")
        # sys.stdout.flush()

        # # Crear tabla temporal de IDs
        # all_ids_table_id = var_table_id + "_all_ids_temp"
        # all_ids_table_ref = bigquery_client.dataset(var_dataset_id).table(all_ids_table_id)
        # schema_ids = [bigquery.SchemaField("id", "STRING", mode="REQUIRED")]
        # bigquery_client.create_table(bigquery.Table(all_ids_table_ref, schema=schema_ids), exists_ok=True)

        # # Guardar IDs en archivo temporal JSONL
        # temp_ids_path = '/tmp/all_ids.json'
        # with open(temp_ids_path, 'w', encoding='utf-8') as f:
        #     for row in all_ids:
        #         f.write(json.dumps(row, ensure_ascii=False) + '\n')

        # # Cargar en BigQuery sobrescribiendo la tabla
        # job_config = bigquery.LoadJobConfig(
        #     schema=schema_ids,
        #     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        #     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        # )
        # with open(temp_ids_path, "rb") as source_file:
        #     bigquery_client.load_table_from_file(source_file, all_ids_table_ref, job_config=job_config).result()

        # print(f"✅ IDs actuales de Firestore cargados en la tabla {all_ids_table_id} en BigQuery (full refresh)")
        # sys.stdout.flush()


        # delete_sql = f"""
        # DELETE FROM `{var_dataset_id}.{var_table_id}`
        # WHERE id NOT IN (SELECT id FROM `{var_dataset_id}.{all_ids_table_id}`)
        # """
        # delete_job = bigquery_client.query(delete_sql)
        # delete_result = delete_job.result()
        # print(f"✅ Delete completado: {delete_result.num_dml_affected_rows} filas eliminadas")
        # sys.stdout.flush()


        print(f"✅ Datos cargados en la tabla {var_table_id} en BigQuery")
        sys.stdout.flush()

    else:
        print('📝 Full export: copiando tabla temporal a destino...')
        sys.stdout.flush()
        copy_config = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        copy_job = bigquery_client.copy_table(temp_table_ref, table_ref, job_config=copy_config)
        copy_job.result()
        print(f"✅ Datos cargados en la tabla {var_table_id} en BigQuery (full export)")
        sys.stdout.flush()

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"✅ Tiempo de ejecución: {duration} segundos")
    sys.stdout.flush()

    return ({'message': f'{total_docs} documentos cargados en {var_table_id}.', 'duration_seconds': round(duration, 2)}), 200



