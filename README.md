# Currency Ingestor (FastAPI + BigQuery)

Minimal FastAPI service for Cloud Run that fetches currency rates from ExchangeRate-API
and writes them into BigQuery.

## Endpoints
- `GET /health` – health check
- `POST /ingest?base=USD&targets=ILS,EUR,GBP` – fetch rates and insert to BigQuery

## Required environment variables (set in Cloud Run):
- `PROJECT_ID` – your GCP project id
- `BQ_DATASET` – e.g. `Analytics`
- `BQ_TABLE` – e.g. `exchange_rates`
- `EXCHANGE_API_KEY` – API key for https://v6.exchangerate-api.com

## Deploy notes
- Region of the BigQuery dataset should be **US** (to match most public datasets / defaults).
- The Cloud Run service account needs `BigQuery Data Editor` and `BigQuery Job User` roles.

## Local quick run (optional)
```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
# open http://127.0.0.1:8000/health
```
