# app/main.py
import logging, os
from fastapi import FastAPI, HTTPException, Query
from datetime import datetime, timezone
from typing import List
from app.exchangerate import fetch_rates
from app.bq import insert_rows, ensure_table
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestor")

app = FastAPI(title="Currency Ingestor")

class IngestResponse(BaseModel):
    inserted: int
    date: str
    base: str
    targets: List[str]

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ingest", response_model=IngestResponse)
def ingest(
    base: str = Query(default="USD"),
    targets: str = Query(default="ILS,EUR,GBP"),
):
    try:
        # נקה ירידות שורה ורווחים מיותרים
        targets = targets.strip()
        targets_list = [t.strip().upper() for t in targets.split(",") if t.strip()]
        if not targets_list:
            raise ValueError("targets is empty")

        # משיכת שערים
        data = fetch_rates(base)  # בפונקציה אנחנו קוראים ל-EXCHANGE_API_KEY מהסביבה
        ensure_table()

        now = datetime.now(timezone.utc).isoformat()
        rows = [
            {"date": data["date"], "base": data["base"], "target": t,
             "rate": float(data["rates"][t]), "ingested_at": now}
            for t in targets_list if t in data["rates"]
        ]

        errors = insert_rows(rows)
        if errors:
            logger.error("BigQuery insert errors: %s", errors)
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        return IngestResponse(
            inserted=len(rows), date=data["date"], base=data["base"], targets=targets_list
        )

    except Exception:
        logger.exception("Unhandled error in /ingest")
        raise HTTPException(status_code=500, detail="Internal error; see logs.")


