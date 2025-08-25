import logging, os
from fastapi import FastAPI, HTTPException, Query
from datetime import datetime, timezone
from typing import List
from pydantic import BaseModel
from app.exchangerate import fetch_rates
from app.bq import insert_rows, ensure_table

logging.basicConfig(level=logging.DEBUG)  # שים DEBUG כדי לראות הכל
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
    logger.info("INGEST start base=%s targets_raw=%r", base, targets)

    try:
        # נקה תווי שורה/רווחים
        targets = (targets or "").strip()
        targets_list = [t.strip().upper() for t in targets.split(",") if t.strip()]
        if not targets_list:
            logger.error("Empty targets after parsing")
            raise HTTPException(status_code=400, detail="targets is empty")

        data = fetch_rates(base)  # בפונקציה הזו נוסיף גם לוגים (סעיף C)
        ensure_table()

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for t in targets_list:
            if t not in data.get("rates", {}):
                logger.warning("Target %s not in rates (available=%s)", t, list(data.get("rates", {}).keys())[:10])
                continue
            rows.append({
                "date": data["date"],
                "base": data["base"],
                "target": t,
                "rate": float(data["rates"][t]),
                "ingested_at": now
            })

        logger.info("Prepared %d rows; first=%s", len(rows), rows[0] if rows else None)

        errors = insert_rows(rows)
        if errors:
            logger.error("BigQuery insert errors: %s", errors)
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        resp = IngestResponse(inserted=len(rows), date=data["date"], base=data["base"], targets=targets_list)
        logger.info("INGEST done: %s", resp)
        return resp

    except HTTPException as e:
        # גם HTTPException יתועד
        logger.error("HTTPException %s", e.detail)
        raise
    except Exception:
        logger.exception("Unhandled error in /ingest")
        raise HTTPException(status_code=500, detail="Internal error; see logs.")



