FROM python:3.13-slim

# Không tạo file .pyc, log hiện ngay ra terminal
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Cài thư viện trước để tận dụng cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy mã nguồn
COPY src/ ./src/

# Chạy bằng user thường thay vì root (an toàn hơn)
RUN useradd -m appuser
USER appuser

CMD ["python", "src/main.py"]