import os
import re
import json
import math
from decimal import Decimal
from datetime import datetime, timezone
import concurrent.futures

import functions_framework
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

def process_collection(firestore_client, collection_name, sep='_', max_level=2, page_size=500, handle_subcollections=False):
    fields = set()
    example_docs = []
    collection_ref = firestore_client.collection(collection_name)
    last_doc = None

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
# Función HTTP principal
# -------------------------------
@functions_framework.http
def export_firestore_to_bigquery(request):
    start_time = datetime.now(timezone.utc)
    request_json = request.get_json(silent=True)

    if not request_json or 'collection' not in request_json or 'table' not in request_json:
        return ({'error': 'Missing collection or table parameter'}), 400

    collection_name = request_json['collection']
    table_id = request_json['table']
    handle_subcollections = request_json.get('handle_subcollections', False)
    full_export = request_json.get('full_export', True)

    service_account_info = json.loads(access_secret_version("sb-operacional-zone", "sb-xops-prod_appspot_gserviceaccount"))
    firestore_client = firestore.Client(credentials=service_account.Credentials.from_service_account_info(service_account_info), project='sb-xops-prod')
    bigquery_client = bigquery.Client(project='sb-operacional-zone')
    dataset_id = 'firestore'

    # Procesar colección
    example_docs, fields = process_collection(firestore_client, collection_name, handle_subcollections=handle_subcollections)
    if not example_docs:
        return ({'error': 'No documents found'}), 404

    temp_file_path = '/tmp/firestore_data.json'
    with open(temp_file_path, 'w', encoding='utf-8') as f:
        for doc in example_docs:
            f.write(json.dumps({k: serialize_value(v) for k, v in doc.items()}, ensure_ascii=False) + '\n')

    # Crear tabla y cargar
    fields = list(set(f.lower() for f in fields))
    schema = [bigquery.SchemaField(f, "STRING", mode="NULLABLE") for f in fields]
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    with open(temp_file_path, 'rb') as source_file:
        bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config).result()

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    return {'message': f'{len(example_docs)} documentos cargados en {table_id}', 'duration_seconds': round(duration, 2)}, 200
