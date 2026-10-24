FROM python:3.13-slim

# TZ là BẮT BUỘC: SDK DNSE đổi epoch sang chuỗi giờ theo múi giờ của máy, còn collector giả định đó là giờ Việt Nam.
# Để mặc định (UTC) thì event_timestamp_utc sẽ lệch 7 tiếng mà không có cảnh báo nào.
ENV TZ=Asia/Ho_Chi_Minh \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Mã nguồn và config/vn30.txt (xem .dockerignore: không đưa .env, .venv, data vào image)
COPY . .

CMD ["python", "-u", "-m", "collectors.dnse_ingestion_2", "--until", "15:05"]