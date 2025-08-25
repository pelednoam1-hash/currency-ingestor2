import os, logging
from google.cloud import bigquery
logger = logging.getLogger("ingestor")

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "Analytics")
TABLE = os.getenv("BQ_TABLE", "exchange_rates")

SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("base", "STRING"),
    bigquery.SchemaField("target", "STRING"),
    bigquery.SchemaField("rate", "FLOAT"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]

def ensure_table():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    try:
        client.get_table(table_id)
        logger.info("BQ table exists: %s", table_id)
    except Exception:
        logger.info("Creating BQ table: %s", table_id)
        table = bigquery.Table(table_id, schema=SCHEMA)
        client.create_table(table)
    return table_id

def insert_rows(rows):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = ensure_table()
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        logger.error("BQ insert returned errors: %s", errors)
    return errors

        logger.exception("insert_rows failed")
        raise

