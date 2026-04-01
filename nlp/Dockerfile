FROM python:3.13-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8001

CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port ${NLP_PORT:-8001}"]
