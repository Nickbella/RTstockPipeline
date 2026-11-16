import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from dnse import DNSEClient

load_dotenv()

def main():
    client = DNSEClient(
        api_key=os.environ["DNSE_API_KEY"],
        api_secret=os.environ["DNSE_API_SECRET"],
        base_url="https://openapi.dnse.com.vn",
    )

    symbols = Path("config/vn100.txt").read_text(encoding="utf-8").split()

    total = 0
    for symbol in symbols:
        status, body = client.get_ohlc(
            bar_type="STOCK",
            query={
                "symbol": symbol,
                "resolution": "1",
                "from": 1794542339,   
                "to": 1794544199,    
            },
            dry_run=False,
        )
        total += len(json.loads(body).get("t", []))
        time.sleep(0.2)
    print(total)


if __name__ == "__main__":
    main()