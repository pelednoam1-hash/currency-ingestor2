FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# התקנת תלויות
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# העתקת קוד האפליקציה
COPY app ./app

# ליתר ביטחון נחשוף 8080 (Cloud Run לא תלוי בזה, אבל זה עוזר לבדיקות)
EXPOSE 8080

# מריצים את uvicorn שיאזין על 0.0.0.0:8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

