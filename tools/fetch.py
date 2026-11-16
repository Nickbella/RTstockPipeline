import json
import os
from pathlib import Path
from dotenv import load_dotenv
from dnse import DNSEClient

load_dotenv()
client = DNSEClient(
    api_key=os.environ["DNSE_API_KEY"],
    api_secret=os.environ["DNSE_API_SECRET"],
    base_url=os.environ.get("DNSE_BASE_URL", "https://openapi.dnse.com.vn"),
    api_version=os.environ.get("DNSE_API_VERSION", "2026-07-23"),
)

status_code, body = client.get_instruments(
    symbol="", market_id="", security_group_id="", index_name="VN100", limit=100, page=1
)

if not status_code or status_code >= 300:
    raise SystemExit(f"Lỗi {status_code}")

items = json.loads(body).get("data") or []

symbols = []
for item in items:
    symbol = item["symbol"].upper() 
    symbols.append(symbol)          
symbols.sort() 

Path("config/vn100.txt").write_text("\n".join(symbols) + "\n", encoding="utf-8")
print(f"Đã ghi {len(symbols)} mã vào file cấu hình")