from datetime import datetime, timezone
from typing import List

from fastapi import FastAPI, Query
from pydantic import BaseModel

from app.exchangerate import fetch_rates
from app.bq import ensure_table, insert_rows

app = FastAPI(title="Currency Ingestor")

@app.get("/health")
def health():
    return {"status": "ok"}

class IngestResponse(BaseModel):
    inserted: int
    date: str
    base: str
    targets: List[str]

@app.post("/ingest", response_model=IngestResponse)
async def ingest(
    base: str = Query(default="USD"),
    targets: str = Query(default="ILS,EUR,GBP")
):
    targets_list = [t.strip().upper() for t in targets.split(",") if t.strip()]
    base_up = base.upper()

    data = await fetch_rates(base_up)
    rates = data.get("conversion_rates", {})
    date_str = data.get("time_last_update_utc", "")

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for t in targets_list:
        if t not in rates:
            continue
        rows.append({
            "date": datetime.now(timezone.utc).date().isoformat(),
            "base": base_up,
            "target": t,
            "rate": float(rates[t]),
            "ingested_at": now_iso,
        })

    ensure_table()
    errors = insert_rows(rows)
    if errors:
        return IngestResponse(inserted=0, date=date_str, base=base_up, targets=targets_list)

    return IngestResponse(inserted=len(rows), date=date_str, base=base_up, targets=targets_list)
