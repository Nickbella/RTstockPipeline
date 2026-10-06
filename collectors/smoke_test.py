import os 
import sys
from dotenv import load_dotenv
from dnse import DNSEClient

load_dotenv()
API_KEY = os.getenv("DNSE_API_KEY")
API_SECRET = os.getenv("DNSE_API_SECRET")

client = DNSEClient(
    api_key=API_KEY,
    api_secret=API_SECRET,
    base_url="https://openapi.dnse.com.vn",
    api_version="2026-07-23",
)

status, body = client.get_accounts(dry_run=False)

print(status)
print(body)
