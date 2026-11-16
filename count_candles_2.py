import json, os, time
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from dotenv import load_dotenv
from dnse import DNSEClient

load_dotenv()
VN = ZoneInfo("Asia/Ho_Chi_Minh")


def main():
    client = DNSEClient(api_key=os.environ["DNSE_API_KEY"], api_secret=os.environ["DNSE_API_SECRET"],
                        base_url="https://openapi.dnse.com.vn")
    symbols = Path("config/vn100.txt").read_text(encoding="utf-8").split()
    api = set()                                   # {(mã, phút giờ VN không offset)}
    for symbol in symbols:
        status, body = client.get_ohlc(bar_type="STOCK", query={
            "symbol": symbol, "resolution": "1", "from": 1794542340, "to": 1794544199}, dry_run=False)
        for t in json.loads(body).get("t", []):
            api.add((symbol, datetime.fromtimestamp(t, VN).replace(tzinfo=None)))
        time.sleep(0.2)

    with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=5) as conn:
        mine = set(conn.execute("SELECT symbol, bucket FROM candles_1m WHERE trading_date = '2026-11-13'").fetchall())

    print("api:", len(api), "| của tôi:", len(mine), "| chung:", len(api & mine))
    for name, diff in (("chỉ tôi có", mine - api), ("chỉ API có", api - mine)):
        print(f"\n{name}: {len(diff)}")
        print("  theo phút:", sorted(Counter(b.strftime('%H:%M') for _, b in diff).items())[:10])
        print("  theo mã  :", Counter(s for s, _ in diff).most_common(10))


if __name__ == "__main__":
    main()