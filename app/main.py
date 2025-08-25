import os
import logging
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List
from app.exchangerate import fetch_rates, ApiError
from app.bq import ensure_table, insert_rows

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

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
    dry_run: bool = Query(default=False)
):
    try:
        data = fetch_rates(base)
        ensure_table()  # יוודא שהטבלה קיימת

        targets_list = [t.strip().upper() for t in targets.split(",") if t.strip()]
        available = data["rates"].keys()

        rows = []
        for t in targets_list:
            if t not in available:
                log.warning("Target %s not in API response", t)
                continue
            rows.append({
                "date": data["date"],
                "base": data["base"],
                "target": t,
                "rate": float(data["rates"][t]),
                "ingested_at": None,  # ימולא ע״י BQ בתור TIMESTAMP אם תרצה
            })

        if not rows:
            raise HTTPException(status_code=400, detail="No targets matched the API response")

        if dry_run:
            log.info("Dry-run: not inserting to BigQuery")
            return IngestResponse(inserted=len(rows), date=data["date"], base=data["base"], targets=targets_list)

        errors = insert_rows(rows)
        if errors:
            log.error("BigQuery insert errors: %s", errors)
            raise HTTPException(status_code=500, detail={"bq_errors": errors})

        return IngestResponse(inserted=len(rows), date=data["date"], base=data["base"], targets=targets_list)

    except ApiError as e:
        log.error("Exchange API error: %s", e)
        raise HTTPException(status_code=502, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Unhandled error in /ingest")
        raise HTTPException(status_code=500, detail=str(e))

