import os
import re
import json
import math
import threading
import concurrent.futures
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import functions_framework
from flask import jsonify
from google.cloud import firestore, bigquery, secretmanager
from google.oauth2 import service_account

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
def process_document(firestore_client, doc_ref, parent_path='', sep='_', max_level=2, handle_subcollections=False):
    fields = set()
    example_docs = []
    doc = doc_ref.get()
    if not doc.exists:
        return example_docs, fields
    doc_data = doc.to_dict()
    doc_data['id'] = doc.id
    doc_data['document_path'] = parent_path + sep + doc.id
    flattened_data = flatten_dict(doc_data, sep=sep, max_level=max_level)
    example_docs.append(flattened_data)
    fields.update(flattened_data.keys())
    if handle_subcollections:
        for subcollection in doc_ref.collections():
            subcollection_path = f"{parent_path}{sep}{subcollection.id}"
            for sub_doc in subcollection.stream():
                sub_docs, sub_fields = process_document(firestore_client, sub_doc.reference, subcollection_path, sep, max_level, handle_subcollections)
                example_docs.extend(sub_docs)
                fields.update(sub_fields)
    return example_docs, fields

def process_collection(firestore_client, collection_name, sep='_', max_level=2, page_size=500, handle_subcollections=False, updated_after=None, updated_field=None):
    fields = set()
    example_docs = []
    collection_ref = firestore_client.collection(collection_name)
    last_doc = None

    if updated_after and updated_field:
        collection_ref = collection_ref.where(updated_field, ">", updated_after)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            query = collection_ref.order_by("__name__").limit(page_size)
            if last_doc:
                query = query.start_after(last_doc)
            docs = list(query.stream())
            batch_docs = []
            futures = [executor.submit(process_document, firestore_client, doc.reference, f"{collection_name}{sep}{doc.id}", sep, max_level, handle_subcollections) for doc in docs]
            for future in concurrent.futures.as_completed(futures):
                doc_docs, doc_fields = future.result()
                batch_docs.extend(doc_docs)
                fields.update(doc_fields)
            if docs:
                last_doc = docs[-1]
            example_docs.extend(batch_docs)
            if len(batch_docs) < page_size:
                break
    return example_docs, fields

# -------------------------------
# Control de ejecución duplicada en BigQuery
# -------------------------------
def was_recently_executed_bq(collection_name: str, database_name: str, window_minutes: int = 30) -> bool:
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
    window_start = now - timedelta(minutes=window_minutes)
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

def get_last_execution_time_from_bq(collection_name: str, database_name: str) -> datetime:
    client = bigquery.Client(project='sb-operacional-zone')
    dataset_id = "dataops"
    table_id = "t_firestore_log_function_locks"
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"
    query = f"""
        SELECT MAX(execution_time) AS last_execution
        FROM `{full_table_id}`
        WHERE collection = @collection AND database = @database
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("collection", "STRING", collection_name),
            bigquery.ScalarQueryParameter("database", "STRING", database_name),
        ]
    )
    result = list(client.query(query, job_config=job_config).result())
    if result and result[0].last_execution:
        return result[0].last_execution.replace(tzinfo=timezone.utc)
    return None

# -------------------------------
# Función HTTP principal
# -------------------------------
@functions_framework.http
def export_firestore_to_bigquery(request):
    start_time = datetime.now(timezone.utc)
    request_json = request.get_json(silent=True)
    print(f'🔹 Payload recibido: {request_json}')

    if not request_json or 'collection' not in request_json or 'table' not in request_json:
        return jsonify({'error': 'Missing collection or table parameter in request'}), 400

    var_main_collection = request_json['collection']
    var_table_id = request_json['table']
    handle_subcollections = request_json.get('handle_subcollections', False)
    var_database = request_json.get('database', '(default)')
    updated_field = request_json.get('updated_field')  # Nombre del campo en Firestore
    full_export = request_json.get('full_export', False)
    page_size = request_json.get('page_size', 500)
    print(f'🟢 Parámetros -> Collection: {var_main_collection}, Table: {var_table_id}, Subcollections: {handle_subcollections}, DB: {var_database}')

    if was_recently_executed_bq(var_main_collection, var_database):
        return f"Duplicate execution for collection {var_main_collection}. Skipping.", 200

    # Lock temporal
    lock_file_path = f"/tmp/lock_{var_main_collection}_{start_time.strftime('%Y-%m-%d %H:%M')}.txt"
    if os.path.exists(lock_file_path):
        return jsonify({'warning': 'Execution skipped to avoid duplicate run'}), 429
    with open(lock_file_path, "w") as lock_file:
        lock_file.write("lock")
    
    updated_after = None
    if not full_export and updated_field:
        updated_after = get_last_execution_time_from_bq(var_main_collection, var_database)

    # Cargar secretos y crear clientes
    service_account_info = json.loads(access_secret_version("sb-operacional-zone", "sb-xops-prod_appspot_gserviceaccount"))
    firestore_client = firestore.Client(credentials=service_account.Credentials.from_service_account_info(service_account_info), project='sb-xops-prod', database=var_database)
    bigquery_client = bigquery.Client(project='sb-operacional-zone')
    var_dataset_id = 'firestore'

    # Procesar colección
    print(f"🔍 Procesando colección: {var_main_collection}")
    example_docs, fields = process_collection(firestore_client, var_main_collection, page_size=page_size, handle_subcollections=handle_subcollections,updated_after=updated_after,updated_field=updated_field)
    if not example_docs:
        return jsonify({'error': 'No documents found in the Firestore collection'}), 404

    print(f"✅ Documentos extraídos: {len(example_docs)}")

    temp_file_path = '/tmp/firestore_data.json'
    print('📝 Creando archivo JSON temporal...')
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        for doc in example_docs:
            temp_file.write(json.dumps({k: serialize_value(v) for k, v in doc.items()}, ensure_ascii=False) + '\n')
    print('📝 Archivo JSON temporal creado:', temp_file_path)

    # Crear esquema y tabla BigQuery
    fields = list(set(f.lower() for f in fields))
    schema = [bigquery.SchemaField(f, "STRING", mode="NULLABLE") for f in fields]
    table_ref = bigquery_client.dataset(var_dataset_id).table(var_table_id)
    bigquery_client.create_table(bigquery.Table(table_ref, schema=schema), exists_ok=True)

    # job_config = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True, max_bad_records=50)
    # with open(temp_file_path, "rb") as source_file:
    #     bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config).result()

    # duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    # return jsonify({'message': f'{len(example_docs)} documentos cargados en {var_table_id}.', 'duration_seconds': round(duration, 2)}), 200

    # -----------------------
    # Cargar tabla temporal
    # -----------------------
    print('📝 Creando tabla temporal...')
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
        bigquery_client.load_table_from_file(source_file, temp_table_ref, job_config=job_config_temp).result()

    # -----------------------
    # MERGE / DELETE si incremental
    # -----------------------
    if not full_export:
        print('📝 Merge / Delete si incremental...')
        merge_sql = f"""
            MERGE `{var_dataset_id}.{var_table_id}` T
            USING `{var_dataset_id}.{temp_table_id}` S
            ON T.id = S.id
            WHEN MATCHED THEN UPDATE SET {', '.join([f'T.{f} = S.{f}' for f in fields])}
            WHEN NOT MATCHED THEN INSERT ({', '.join(fields)}) VALUES ({', '.join([f'S.{f}' for f in fields])})
        """
        bigquery_client.query(merge_sql).result()

        delete_sql = f"""
            DELETE FROM `{var_dataset_id}.{var_table_id}`
            WHERE id NOT IN (SELECT id FROM `{var_dataset_id}.{temp_table_id}`)
        """
        bigquery_client.query(delete_sql).result()
        print(f"✅ Datos cargados en la tabla {var_table_id} en BigQuery")

    else:
        print('📝 Full export: sobrescribe la tabla...')
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

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    return jsonify({'message': f'{len(example_docs)} documentos cargados en {var_table_id}.', 'duration_seconds': round(duration, 2)}), 200



