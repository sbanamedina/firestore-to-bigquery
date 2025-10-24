"""
Export de Firestore a BigQuery.
Características principales:
- Soporta export incremental usando un campo `updated_at` (configurable). Si no existe, puede correr full export.
- Evita ejecuciones duplicadas mediante tabla de lock/checkpoint en BigQuery.
- Usa ThreadPoolExecutor con límite de workers configurable.
- Escribe NDJSON a Google Cloud Storage y usa load_table_from_uri para carga a BigQuery (WRITE_APPEND por defecto).
- Inferencia simple de tipos para esquema (opcional).
- Logging estructurado y manejo de errores.

"""

import os
import re
import json
import math
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Tuple, List, Dict, Set, Optional

import functions_framework
from google.cloud import firestore, bigquery, storage, secretmanager
from google.oauth2 import service_account
import concurrent.futures

# ---------------------------
# CONFIG
# ---------------------------
PROJECT_SECRETS_PROJECT = os.environ.get("SECRETS_PROJECT", "sb-operacional-zone")
CREDENTIAL_SECRET_ID = os.environ.get("CREDENTIAL_SECRET_ID", "sb-xops-prod_appspot_gserviceaccount")
FIRESTORE_PROJECT = os.environ.get("FIRESTORE_PROJECT", "sb-xops-prod")
BQ_PROJECT = os.environ.get("BQ_PROJECT", "sb-operacional-zone")
BQ_DATASET = os.environ.get("BQ_DATASET", "firestore")
BQ_LOCK_TABLE = os.environ.get("BQ_LOCK_TABLE", "t_firestore_log_function_locks")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "sb-temp-exports")
DEFAULT_PAGE_SIZE = int(os.environ.get("PAGE_SIZE", 500))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", min(20, (os.cpu_count() or 2) * 5)))
INCREMENTAL_FIELD = os.environ.get("INCREMENTAL_FIELD", "updated_at")  # campo en Firestore para incremental
LOCK_WINDOW_MINUTES = int(os.environ.get("LOCK_WINDOW_MINUTES", 5))

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("firestore_export")

# ---------------------------
# Helpers: secrets, clients
# ---------------------------

def access_secret_version(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def build_clients_from_secret(secret_json: str) -> Tuple[firestore.Client, bigquery.Client, storage.Client]:
    sa_info = json.loads(secret_json)
    creds = service_account.Credentials.from_service_account_info(sa_info)
    fs = firestore.Client(project=FIRESTORE_PROJECT, credentials=creds)
    bq = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    gcs = storage.Client(project=BQ_PROJECT, credentials=creds)
    return fs, bq, gcs

# ---------------------------
# Serialization / flatten
# ---------------------------

def serialize_value(value):
    if isinstance(value, datetime):
        # ensure UTC and ISO
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value
    if isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [serialize_value(v) for v in value]
    if isinstance(value, str):
        v = re.sub(r'[\x00-\x1f\x7f]', ' ', value)
        v = re.sub(r'[\u200B-\u200D\uFEFF\u2028\u2029]', ' ', v)
        v = v.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        return v.strip()
    if value is None:
        return None
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


def flatten_dict(d: dict, parent_key: str = '', sep: str = '_', level: int = 1, max_level: int = 2) -> dict:
    items = {}
    if level > max_level:
        return {parent_key: json.dumps(d)} if parent_key else {"value": json.dumps(d)}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        new_key = re.sub(r'\W+', '_', new_key).lower()
        v = serialize_value(v)
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep, level=level+1, max_level=max_level))
        elif isinstance(v, list):
            items[new_key] = json.dumps(v)
        else:
            items[new_key] = v
    return items

# ---------------------------
# BigQuery bookkeeping: lock and checkpoints
# ---------------------------

def ensure_lock_table(bq_client: bigquery.Client):
    full_table = f"{bq_client.project}.{BQ_DATASET}.{BQ_LOCK_TABLE}"
    try:
        bq_client.get_table(full_table)
    except Exception:
        logger.info("Lock table does not exist. Creating %s", full_table)
        schema = [
            bigquery.SchemaField("collection", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("database", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("execution_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("meta", "STRING", mode="NULLABLE"),
        ]
        table = bigquery.Table(full_table, schema=schema)
        bq_client.create_table(table)
        logger.info("Lock table created: %s", full_table)


def was_recently_executed_bq(bq_client: bigquery.Client, collection_name: str, database_name: str, window_minutes: int = LOCK_WINDOW_MINUTES) -> bool:
    ensure_lock_table(bq_client)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    window_start = now - timedelta(minutes=window_minutes)
    full_table = f"{bq_client.project}.{BQ_DATASET}.{BQ_LOCK_TABLE}"
    query = f"""
    SELECT COUNT(*) as total
    FROM `{full_table}`
    WHERE collection = @collection
      AND database = @database
      AND execution_time > @window_start
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("collection", "STRING", collection_name),
            bigquery.ScalarQueryParameter("database", "STRING", database_name),
            bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
        ]
    )
    res = bq_client.query(query, job_config=job_config).result()
    rows = list(res)
    total = rows[0].total if rows else 0
    logger.info("Found %s recent executions for %s.%s since %s", total, collection_name, database_name, window_start)
    if total > 0:
        return True
    # Insert lock row
    table_ref = f"{bq_client.project}.{BQ_DATASET}.{BQ_LOCK_TABLE}"
    rows_to_insert = [{
        "collection": collection_name,
        "database": database_name,
        "execution_time": now.isoformat(),
        "meta": json.dumps({"started_at": now.isoformat()})
    }]
    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        logger.warning("Errors inserting lock row: %s", errors)
    return False

# ---------------------------
# Checkpoint management for incremental exports
# ---------------------------

def get_last_export_time(bq_client: bigquery.Client, collection_name: str) -> Optional[datetime]:
    ensure_lock_table(bq_client)
    query = f"SELECT MAX(execution_time) as last FROM `{bq_client.project}.{BQ_DATASET}.{BQ_LOCK_TABLE}` WHERE collection = @collection"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("collection", "STRING", collection_name)]
    )
    res = bq_client.query(query, job_config=job_config).result()
    rows = list(res)
    if rows and rows[0].last:
        return rows[0].last
    return None

# ---------------------------
# Firestore processing
# ---------------------------

def process_document(doc_snapshot, parent_path: str = '', sep: str = '_', max_level: int = 2, handle_subcollections: bool = False):
    try:
        doc_data = doc_snapshot.to_dict() or {}
        doc_data['id'] = doc_snapshot.id
        doc_data['document_path'] = f"{parent_path}{sep}{doc_snapshot.id}" if parent_path else doc_snapshot.id
        flattened = flatten_dict(doc_data, parent_key='', sep=sep, max_level=max_level)
        results = [flattened]
        fields = set(flattened.keys())
        if handle_subcollections:
            for subcol in doc_snapshot.reference.collections():
                for subdoc in subcol.stream():
                    sub_results, sub_fields = process_document(subdoc, parent_path=f"{parent_path}{sep}{subcol.id}", sep=sep, max_level=max_level, handle_subcollections=False)
                    results.extend(sub_results)
                    fields.update(sub_fields)
        return results, fields
    except Exception as e:
        logger.exception("Error processing document %s: %s", doc_snapshot.id, e)
        return [], set()


def process_collection(
    firestore_client: firestore.Client,
    collection_name: str,
    sep: str = '_',
    max_level: int = 2,
    page_size: int = DEFAULT_PAGE_SIZE,
    handle_subcollections: bool = False,
    query=None,
    full_export: bool = False  # <-- agregar aquí
):
    fields = set()
    example_docs = []
    collection_ref = firestore_client.collection(collection_name)
    last_doc = None

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            if query is not None:
                docs = list(query.stream())
            elif full_export:
                # Full export: tomar todos los documentos sin limitar ni order_by
                docs = list(collection_ref.stream())
                if not docs:
                    break
            else:
                q = collection_ref.order_by("__name__").limit(page_size)
                if last_doc:
                    q = q.start_after(last_doc)
                docs = list(q.stream())

            if not docs:
                break

            futures = [executor.submit(process_document, doc, f"{collection_name}{sep}{doc.id}", sep, max_level, handle_subcollections) for doc in docs]
            for future in concurrent.futures.as_completed(futures):
                doc_docs, doc_fields = future.result()
                example_docs.extend(doc_docs)
                fields.update(doc_fields)

            if not full_export:
                last_doc = docs[-1]
                if len(docs) < page_size:
                    break
            else:
                # Full export procesado
                break

    return example_docs, fields

# ---------------------------
# GCS helpers
# ---------------------------

def upload_ndjson_to_gcs(gcs_client: storage.Client, bucket_name: str, local_path: str, gcs_path: str):
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    logger.info("Uploaded %s to gs://%s/%s", local_path, bucket_name, gcs_path)
    return f"gs://{bucket_name}/{gcs_path}"

# ---------------------------
# Entrypoint (Cloud Function)
# ---------------------------

@functions_framework.http
def export_firestore_to_bigquery(request):
    start_time = datetime.now(timezone.utc)
    logger.info("Function started")

    request_json = request.get_json(silent=True) or {}
    collection = request_json.get('collection')
    table = request_json.get('table')
    handle_subcollections = request_json.get('handle_subcollections', False)
    database = request_json.get('database', '(default)')
    page_size = int(request_json.get('page_size', DEFAULT_PAGE_SIZE))
    full_export = request_json.get('full_export', False)

    if not collection or not table:
        return ({'error': 'Missing collection or table parameter in request'}, 400)

    try:
        secret = access_secret_version(PROJECT_SECRETS_PROJECT, CREDENTIAL_SECRET_ID)
        fs_client, bq_client, gcs_client = build_clients_from_secret(secret)

        # Check dedupe/lock in BigQuery
        if was_recently_executed_bq(bq_client, collection, database, window_minutes=LOCK_WINDOW_MINUTES):
            logger.info("Duplicate execution detected. Skipping for collection %s", collection)
            return ({'warning': 'Duplicate execution; skipping'}, 200)

        # Decide incremental query
        query = None
        if not full_export:
            last_export = get_last_export_time(bq_client, collection)
            if last_export:
                logger.info("Last export for %s was %s. Running incremental from that timestamp.", collection, last_export)
                # Firestore expects naive UTC or with tz; we store as iso. Use where if field exists.
                # If the field doesn't exist in docs, this will just return none.
                try:
                    query = fs_client.collection(collection).where(INCREMENTAL_FIELD, '>', last_export).order_by(INCREMENTAL_FIELD).limit(page_size)
                except Exception as e:
                    logger.warning("Could not build incremental query on field %s: %s. Falling back to full export.", INCREMENTAL_FIELD, e)
                    query = None
            else:
                logger.info("No checkpoint found for %s; running full export.", collection)
        
        # --- Debug Firestore Connection ---
        logger.info("🔍 Verificando conexión a Firestore...")
        logger.info("Firestore project configurado: %s", FIRESTORE_PROJECT)
        logger.info("Database usada (solo informativo): %s", database)
        logger.info("Colección solicitada: %s", collection)

        try:
            test_collection = fs_client.collection(collection)
            test_docs = list(test_collection.limit(3).stream())
            logger.info("✅ Firestore accesible. Primeros %d documentos detectados en '%s'.", len(test_docs), collection)
            for doc in test_docs:
                logger.info("   • Documento ID: %s", doc.id)
        except Exception as e:
            logger.exception("❌ Error accediendo a la colección %s: %s", collection, e)

        logger.info("Probar stream directo:")
        for doc in fs_client.collection(collection).limit(5).stream():
            logger.info("   • Documento ID directo: %s", doc.id)

        # Process collection
        docs, fields = process_collection(
            fs_client,
            collection,
            sep='_',
            max_level=2,
            page_size=page_size,
            handle_subcollections=handle_subcollections,
            query=query,
            full_export=full_export 
        )

        if not docs:
            logger.info("No documents to export for %s", collection)
            return ({'message': 'No documents found'}, 200)

        # Write NDJSON to /tmp then upload to GCS
        tmp_path = f"/tmp/{collection}_export_{start_time.strftime('%Y%m%dT%H%M%SZ')}.ndjson"
        invalid_count = 0
        with open(tmp_path, 'w', encoding='utf-8') as f:
            for doc in docs:
                try:
                    cleaned = {k: serialize_value(v) for k, v in doc.items()}
                    json.dump(cleaned, f, ensure_ascii=False)
                    f.write('\n')
                except Exception:
                    invalid_count += 1
        logger.info("Wrote NDJSON to %s with %s invalid docs", tmp_path, invalid_count)

        # Upload to GCS
        gcs_path = f"firestore_exports/{collection}/{os.path.basename(tmp_path)}"
        uri = upload_ndjson_to_gcs(gcs_client, GCS_BUCKET, tmp_path, gcs_path)

        # Load into BigQuery
        dataset_ref = bigquery.DatasetReference(BQ_PROJECT, BQ_DATASET)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
            max_bad_records=50,
        )
        load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
        logger.info("Started BigQuery load job %s", load_job.job_id)
        load_job.result()
        logger.info("BigQuery load job completed")

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        return ({'message': f'Loaded {len(docs)} documents into {table}', 'duration_seconds': duration}, 200)

    except Exception as e:
        logger.exception("Unhandled error: %s", e)
        return ({'error': str(e)}, 500)
