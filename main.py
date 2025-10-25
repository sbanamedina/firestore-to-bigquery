import os
import re
import json
import math
import threading
import concurrent.futures
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import functions_framework
from google.cloud import firestore, bigquery, secretmanager
from google.oauth2 import service_account
import sys
import logging
from google.api_core.retry import Retry
from google.api_core.exceptions import GoogleAPICallError

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

def process_document(firestore_client, doc_ref, parent_path='', sep='_', max_level=2,
                     handle_subcollections=False, updated_after=None, updated_field=None):
    fields = set()
    example_docs = []

    try:
        doc = doc_ref.get()
    except GoogleAPICallError as e:
        print(f"⚠️ Error al obtener doc {doc_ref.path}: {e}")
        return example_docs, fields

    if not doc.exists:
        return example_docs, fields
    doc_data = doc.to_dict()

    # Validar incremental
    if updated_after and updated_field and updated_field in doc_data:
        doc_value = doc_data[updated_field]
        doc_dt = doc_value if isinstance(doc_value, datetime) else datetime.fromisoformat(str(doc_value))
        if doc_dt <= updated_after:
            return example_docs, fields

    doc_data['id'] = doc.id
    doc_data['document_path'] = parent_path + sep + doc.id
    flattened_data = flatten_dict(doc_data, sep=sep, max_level=max_level)
    example_docs.append(flattened_data)
    fields.update(flattened_data.keys())

    if handle_subcollections:
        try:
            # Usar retry con deadline extendido
            for subcollection in doc_ref.collections(retry=Retry(deadline=300)):
                subcollection_path = f"{parent_path}{sep}{subcollection.id}" if parent_path else subcollection.id
                for sub_doc in subcollection.stream():
                    sub_docs, sub_fields = process_document(
                        firestore_client,
                        sub_doc.reference,
                        parent_path=subcollection_path,
                        sep=sep,
                        max_level=max_level,
                        handle_subcollections=True,
                        updated_after=updated_after,
                        updated_field=updated_field
                    )
                    example_docs.extend(sub_docs)
                    fields.update(sub_fields)
        except GoogleAPICallError as e:
            print(f"⚠️ Error al listar subcollections de {doc_ref.path}: {e}")

    return example_docs, fields

def process_collection(firestore_client, collection_name, sep='_', max_level=2, page_size=500, handle_subcollections=False, updated_after=None, updated_field=None):
    fields = set()
    example_docs = []
    collection_ref = firestore_client.collection(collection_name)
    last_doc = None

    if updated_after and updated_field:
        try:
            # Prueba primero como timestamp (nativo)
            collection_ref = collection_ref.where(updated_field, ">", updated_after)
            collection_ref = collection_ref.order_by(updated_field).order_by("__name__")
            print(f"🧭 Filtro aplicado como TIMESTAMP: {updated_field} > {updated_after}")
            sys.stdout.flush()
        except Exception as e:
            # Si falla (por tipo string), usa comparación de texto
            updated_after_str = updated_after.strftime("%Y-%m-%d %H:%M:%S")
            collection_ref = collection_ref.where(updated_field, ">", updated_after_str)
            collection_ref = collection_ref.order_by(updated_field).order_by("__name__")
            print(f"🧭 Filtro aplicado como STRING: {updated_field} > '{updated_after_str}'")
            sys.stdout.flush()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            query = collection_ref.limit(page_size)  # <- No volver a order_by
            if last_doc:
                query = query.start_after(last_doc)
            docs = list(query.stream())
            batch_docs = []
            futures = [
                executor.submit(
                    process_document,
                    firestore_client,
                    doc.reference,
                    f"{collection_name}{sep}{doc.id}",
                    sep,
                    max_level,
                    handle_subcollections,
                    updated_after,
                    updated_field
                )
                for doc in docs
            ]
            for future in concurrent.futures.as_completed(futures):
                doc_docs, doc_fields = future.result()
                batch_docs.extend(doc_docs)
                fields.update(doc_fields)
            if docs:
                last_doc = docs[-1]
            example_docs.extend(batch_docs)
            if len(docs) < page_size:  # <- comparar con docs, no batch_docs
                break

    return example_docs, fields


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

    var_main_collection = request_json['collection']
    var_table_id = request_json['table']
    handle_subcollections = request_json.get('handle_subcollections', False)
    var_database = request_json.get('database', '(default)')
    updated_field = request_json.get('updated_field')  # Nombre del campo en Firestore
    full_export = request_json.get('full_export', False)
    page_size = request_json.get('page_size', 500)
    print(f'🟢 Parámetros -> Collection: {var_main_collection}, Table: {var_table_id}, Subcollections: {handle_subcollections}, DB: {var_database}')
    sys.stdout.flush()
    
    # if was_recently_executed_bq(var_main_collection, var_database):
    #     print(f"⛔ Ya se ejecutó recientemente para la colección: {var_main_collection}. Cancelando ejecución.")
    #     sys.stdout.flush()
    #     return f"Duplicate execution for collection {var_main_collection}. Skipping.", 200    

    updated_after = None
    if not full_export and updated_field:
        updated_after = get_last_updated_field_from_bq(
            dataset='firestore', 
            table_name=var_table_id, 
            updated_field=updated_field
        )
        print(f"🔹 Última fecha de actualización en BigQuery: {updated_after}")
        sys.stdout.flush()


    # Cargar secretos y crear clientes
    print("🔹 Cargando secretos y creando clientes")
    sys.stdout.flush()
    service_account_info = json.loads(access_secret_version("sb-operacional-zone", "sb-xops-prod_appspot_gserviceaccount"))
    firestore_client = firestore.Client(credentials=service_account.Credentials.from_service_account_info(service_account_info), project='sb-xops-prod', database=var_database)
    bigquery_client = bigquery.Client(project='sb-operacional-zone')
    var_dataset_id = 'firestore'

    # Procesar colección
    print(f"🔍 Procesando colección: {var_main_collection}")
    sys.stdout.flush()
    example_docs, fields = process_collection(firestore_client, var_main_collection, page_size=page_size, handle_subcollections=handle_subcollections,updated_after=updated_after,updated_field=updated_field)
    if not example_docs:
        print(f"⛔ No se encontraron documentos en la colección: {var_main_collection}")
        sys.stdout.flush()
        return ({'error': 'No documents found in the Firestore collection'}), 404

    print(f"✅ Documentos extraídos: {len(example_docs)}")
    sys.stdout.flush()

    temp_file_path = '/tmp/firestore_data.json'
    print('📝 Creando archivo JSON temporal...')
    sys.stdout.flush()
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        for doc in example_docs:
            temp_file.write(json.dumps({k: serialize_value(v) for k, v in doc.items()}, ensure_ascii=False) + '\n')
    print('📝 Archivo JSON temporal creado:', temp_file_path)
    sys.stdout.flush()

    # Crear esquema y tabla BigQuery
    fields = list(set(f.lower() for f in fields))
    schema = [bigquery.SchemaField(f, "STRING", mode="NULLABLE") for f in fields]
    table_ref = bigquery_client.dataset(var_dataset_id).table(var_table_id)
    bigquery_client.create_table(bigquery.Table(table_ref, schema=schema), exists_ok=True)

    # -----------------------
    # Cargar tabla temporal
    # -----------------------
    print('📝 Creando tabla temporal...')
    sys.stdout.flush()
    temp_table_id = var_table_id + "_temp"
    temp_table_ref = bigquery_client.dataset(var_dataset_id).table(temp_table_id)
    bigquery_client.create_table(bigquery.Table(temp_table_ref, schema=schema), exists_ok=True)

    job_config_temp = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        max_bad_records=50
    )
    with open(temp_file_path, "rb") as source_file:
        print('📝 Cargando tabla temporal...')
        sys.stdout.flush()
        bigquery_client.load_table_from_file(source_file, temp_table_ref, job_config=job_config_temp).result()

    # -----------------------
    # MERGE / DELETE si incremental
    # -----------------------
    if not full_export:
        print('📝 Merge / Delete si incremental...')
        sys.stdout.flush()
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

        print("📝 Obteniendo todos los IDs actuales de Firestore para manejar eliminados...")
        all_ids = []
        for doc in firestore_client.collection(var_main_collection).stream():
            all_ids.append({'id': doc.id})

        # Crear tabla temporal de IDs
        all_ids_table_id = var_table_id + "_all_ids_temp"
        all_ids_table_ref = bigquery_client.dataset(var_dataset_id).table(all_ids_table_id)
        schema_ids = [bigquery.SchemaField("id", "STRING", mode="REQUIRED")]
        bigquery_client.create_table(bigquery.Table(all_ids_table_ref, schema=schema_ids), exists_ok=True)

        # Guardar IDs en archivo temporal JSONL
        temp_ids_path = '/tmp/all_ids.json'
        with open(temp_ids_path, 'w', encoding='utf-8') as f:
            for row in all_ids:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')

        # Cargar en BigQuery sobrescribiendo la tabla
        job_config = bigquery.LoadJobConfig(
            schema=schema_ids,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        with open(temp_ids_path, "rb") as source_file:
            bigquery_client.load_table_from_file(source_file, all_ids_table_ref, job_config=job_config).result()

        print(f"✅ IDs actuales de Firestore cargados en la tabla {all_ids_table_id} en BigQuery (full refresh)")
        sys.stdout.flush()


        delete_sql = f"""
        DELETE FROM `{var_dataset_id}.{var_table_id}`
        WHERE id NOT IN (SELECT id FROM `{var_dataset_id}.{all_ids_table_id}`)
        """
        delete_job = bigquery_client.query(delete_sql)
        delete_result = delete_job.result()
        print(f"✅ Delete completado: {delete_result.num_dml_affected_rows} filas eliminadas")
        sys.stdout.flush()


        print(f"✅ Datos cargados en la tabla {var_table_id} en BigQuery")
        sys.stdout.flush()

    else:
        print('📝 Full export: sobrescribe la tabla...')
        sys.stdout.flush()
        # Full export: sobrescribe la tabla
        job_config_full = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            max_bad_records=50
        )
        with open(temp_file_path, "rb") as source_file:
            bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config_full).result()
        print(f"✅ Datos cargados en la tabla {var_table_id} en BigQuery")
        sys.stdout.flush()

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"✅ Tiempo de ejecución: {duration} segundos")
    sys.stdout.flush()

    return ({'message': f'{len(example_docs)} documentos cargados en {var_table_id}.', 'duration_seconds': round(duration, 2)}), 200



