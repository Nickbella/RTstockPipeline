FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Ho_Chi_Minh

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collectors/ ./collectors/
COPY utils/ ./utils/
COPY config/ ./config/

RUN useradd -m appuser && mkdir -p data && chown appuser:appuser data
USER appuser

CMD ["python", "-u", "-m", "collectors.dnse_ingestion_2", "--until", "15:05"]