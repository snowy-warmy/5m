FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .

# Fly.io will mount a volume at /data for SQLite persistence
RUN mkdir -p /data
ENV DB_PATH=/data/bot.sqlite3 \
    HEALTHCHECK_PORT=8080 \
    PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["python", "bot.py"]
