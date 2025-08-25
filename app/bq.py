# app/bq.py
import os
import logging
from google.cloud import bigquery

logger = logging.getLogger("ingestor.bq")

# אלה חייבים להתאים ל-Variables שהגדרת ב-Cloud Run
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET    = os.getenv("BQ_DATASET", "Analytics")
TABLE      = os.getenv("BQ_TABLE", "exchange_rates")

SCHEMA = [
    bigquery.SchemaField("date",        "DATE"),
    bigquery.SchemaField("base",        "STRING"),
    bigquery.SchemaField("target",      "STRING"),
    bigquery.SchemaField("rate",        "FLOAT"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]

def ensure_table() -> str:
    """ודא שהטבלה קיימת; אם לא – צור אותה."""
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    logger.info("ensure_table project=%s dataset=%s table=%s", PROJECT_ID, DATASET, TABLE)
    client = bigquery.Client(project=PROJECT_ID)
    try:
        client.get_table(table_id)   # קיימת
        return table_id
    except Exception:
        logger.info("table not found; creating %s", table_id)
        try:
            table = bigquery.Table(table_id, schema=SCHEMA)
            client.create_table(table)
            logger.info("table created: %s", table_id)
            return table_id
        except Exception:
            # חשוב: להדפיס Traceback כדי שיופיע בענן
            logger.exception("ensure_table failed")
            raise

def insert_rows(rows: list[dict]) -> None:
    """הכנסת רשומות ל-BigQuery עם לוגים מלאים."""
    logger.info("insert_rows count=%d", len(rows))
    client = bigquery.Client(project=PROJECT_ID)
    table_id = ensure_table()
    try:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            logger.error("BQ insert errors: %s", errors)
            raise RuntimeError(errors)
    except Exception:
        logger.exception("insert_rows failed")
        raise

