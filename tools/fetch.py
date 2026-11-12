import json
import os
import sys
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

index_name = sys.argv[1].upper()
symbols, page = set(), 1
while True:
    status_code, body = client.get_instruments(symbol="", market_id="", security_group_id="",
                                               index_name=index_name, limit=100, page=page)
    if not status_code or status_code >= 300:
        raise SystemExit(f"DNSE trả lỗi {status_code} ở trang {page}")
    payload = json.loads(body)
    items = payload.get("data") or []
    symbols |= {item["symbol"].upper() for item in items if index_name in item.get("indexName", [])}
    if not items or page * 100 >= payload.get("total", 0):
        break
    page += 1

if not symbols:
    raise SystemExit(f"Không có mã nào thuộc {index_name}, không ghi tệp")

output_path = Path("config") / f"{index_name.lower()}.txt"
output_path.write_text("\n".join(sorted(symbols)) + "\n", encoding="utf-8")
print(f"Đã ghi {len(symbols)} mã vào {output_path}")