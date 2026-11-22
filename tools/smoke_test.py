import json
import os

from dotenv import load_dotenv
from dnse import DNSEClient

load_dotenv()
dnse_client = DNSEClient(
    api_key=os.environ["DNSE_API_KEY"],
    api_secret=os.environ["DNSE_API_SECRET"],
    base_url=os.environ.get("DNSE_BASE_URL", "https://openapi.dnse.com.vn"),
    api_version=os.environ.get("DNSE_API_VERSION", "2026-07-23"),
)

for symbol in ["HPG", "VCB", "FPT"]:
    status_code, response_body = dnse_client.get_security_definition(symbol)
    if not status_code or status_code >= 300:
        print(symbol, "lỗi", status_code)
        continue
    parsed_body = json.loads(response_body)
    security = (parsed_body if isinstance(parsed_body, list) else [parsed_body])[0]
    print(symbol, security["ceilingPrice"], security["floorPrice"], security["basicPrice"])