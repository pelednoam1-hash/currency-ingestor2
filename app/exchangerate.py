import os
from typing import Dict
import httpx
from fastapi import HTTPException

API_KEY = os.getenv("EXCHANGE_API_KEY", "")  
BASE_URL = os.getenv("EXCHANGE_API_URL", "https://v6.exchangerate-api.com/v6")

async def fetch_rates(base: str) -> Dict:
    if not API_KEY:
        raise HTTPException(status_code=500, detail="Missing EXCHANGE_API_KEY")

    url = f"{BASE_URL}/{API_KEY}/latest/{base.upper()}"
    timeout = httpx.Timeout(20.0, read=20.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Rates API error: {r.text}")
        data = r.json()
        if data.get("result") != "success":
            raise HTTPException(status_code=502, detail=f"Rates API error: {data}")
        return data
