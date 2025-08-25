# app/main.py
import logging
from fastapi import FastAPI, HTTPException, Query
from typing import List

from app.exchangerate import fetch_rates
from app.bq import ensure_table, insert_rows

app = FastAPI(title="Currency Ingestor")

# לוגגר בסיסי שנכתב ל-stdout/stderr ונאסף ע"י Cloud Run
logger = logging.getLogger("ingestor")
logger.setLevel(logging.INFO)

@app.get("/health")
def health():
    return {"status": "ok"}

class IngestResponse(BaseModel):
    inserted: int
    date: str
    base: str
    targets: List[str]

@app.post("/ingest", response_model=IngestResponse)
def ingest(
    base: str = Query(default="USD"),
    targets: str = Query(default="ILS,EUR,GBP")
):
    logger.info("ingest called base=%s targets=%s", base, targets)
    try:
        targets_list = [t.strip().upper() for t in targets.split(",") if t.strip()]
        data = fetch_rates(base, targets_list)
        ensure_table()
        rows = [
            {"date": data["date"], "base": data["base"], "target": t,
             "rate": float(data["rates"][t]), "ingested_at": datetime.now(timezone.utc).isoformat()}
            for t in targets_list if t in data["rates"]
        ]
        insert_rows(rows)
        return IngestResponse(
            inserted=len(rows),
            date=data["date"],
            base=data["base"],
            targets=targets_list
        )
    except Exception:
        # חשוב: מדפיס Traceback ל-stderr כדי שיופיע ב-Logs Explorer
        logger.exception("INGEST FAILED")
        # מרים שוב את החריגה כדי ש-FastAPI יחזיר 500
        raise

    errors = insert_rows(rows)
    if errors:
        return IngestResponse(inserted=0, date=date_str, base=base_up, targets=targets_list)

    return IngestResponse(inserted=len(rows), date=date_str, base=base_up, targets=targets_list)
