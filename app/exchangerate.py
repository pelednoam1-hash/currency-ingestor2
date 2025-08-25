import os
import requests
from datetime import datetime

API_KEY = os.getenv("EXCHANGE_API_KEY")

class ApiError(Exception):
    pass

def fetch_rates(base: str) -> dict:
    if not API_KEY:
        raise ApiError("Missing EXCHANGE_API_KEY")

    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{base.upper()}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    j = r.json()

    # איחוד לשם 'rates' לא משנה איזה ספק
    if "rates" in j:
        rates = j["rates"]
    elif "conversion_rates" in j:
        rates = j["conversion_rates"]
    else:
        raise ApiError(f"No rates key in API response: keys={list(j.keys())}")

    # תאריך בסיסי לשורה (אפשר גם מה־API אם קיים)
    return {
        "date": datetime.utcnow().date().isoformat(),
        "base": j.get("base_code", base.upper()),
        "rates": rates,
    }

