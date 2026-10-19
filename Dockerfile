FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collectors/ ./collectors/
COPY config/ ./config/
COPY utils/ ./utils/
COPY tools/ ./tools/

RUN useradd -m appuser && \
    chown -R appuser:appuser /app

USER appuser

CMD ["python", "-u", "collectors/dnse_ingestion_2.py"]
