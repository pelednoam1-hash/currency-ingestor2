import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

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

def _client() -> bigquery.Client:
    if not PROJECT_ID:
        raise RuntimeError("Missing PROJECT_ID env var")
    return bigquery.Client(project=PROJECT_ID)

def ensure_table() -> str:
    client = _client()
    dataset_id = f"{PROJECT_ID}.{DATASET}"
    table_id = f"{dataset_id}.{TABLE}"

    try:
        client.get_dataset(dataset_id)
    except NotFound:
        client.create_dataset(bigquery.Dataset(dataset_id), exists_ok=True)

    try:
        client.get_table(table_id)
    except NotFound:
        table = bigquery.Table(table_id, schema=SCHEMA)
        client.create_table(table)
    return table_id

def insert_rows(rows: list) -> list:
    client = _client()
    table_id = ensure_table()
    errors = client.insert_rows_json(table_id, rows)
    return errors or []
