import os, logging, requests
from fastapi import HTTPException

logger = logging.getLogger("ingestor")
API_KEY = os.getenv("EXCHANGE_API_KEY")

def fetch_rates(base: str):
    if not API_KEY:
        logger.error("Missing EXCHANGE_API_KEY")
        raise HTTPException(status_code=500, detail="Missing EXCHANGE_API_KEY")

    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{base.upper()}"
    logger.info("Calling exchangerate: %s", url)
    r = requests.get(url, timeout=15)
    logger.info("exchangerate status=%s", r.status_code)

    try:
        data = r.json()
    except Exception:
        logger.exception("JSON decode failed from exchangerate")
        raise HTTPException(status_code=502, detail="Bad response from exchangerate")

    # התאמה למבנה שהקוד מצפה לו
    if "conversion_rates" in data:
        normalized = {"date": data.get("time_last_update_utc", "")[:10],
                      "base": data.get("base_code", base.upper()),
                      "rates": data["conversion_rates"]}
    else:
        logger.error("Unexpected payload: %s", list(data.keys()))
        raise HTTPException(status_code=502, detail="Unexpected exchangerate payload")

    logger.info("exchangerate got %d rates for base=%s", len(normalized["rates"]), normalized["base"])
    return normalized

