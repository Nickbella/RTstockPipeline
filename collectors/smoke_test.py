from ast import main
import os 
import sys
import json
from dotenv import load_dotenv
from logging import log
from dnse import DNSEClient
from datetime import date, timedelta


load_dotenv()
API_KEY = os.getenv("DNSE_API_KEY")
API_SECRET = os.getenv("DNSE_API_SECRET")

import asyncio
from datetime import datetime

from dnse import TradingClient
from dnse.websocket.models import Trade


async def main():
    encoding = "msgpack"  
    client = TradingClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        base_url="wss://ws-openapi.dnse.com.vn",
        encoding=encoding,
    )

    def handle_trade(trade: Trade):
        received_at = datetime.fromtimestamp(trade.receivedAt).strftime("%H:%M:%S.%f")[:-3] if trade.receivedAt else "N/A"
        print(f"[{received_at}] TRADE: {trade}")

    # Connect to gateway
    print("Connecting to WebSocket gateway...")
    await client.connect()
    print(f"Connected! Session ID: {client._session_id}\n")

    print("Subscribing to trades for SSI and 41I1G4000...")
    await client.subscribe_trades(["SSI", "41I1G4000"], on_trade=handle_trade, encoding=encoding, board_id="G1")

    print("\nReceiving market data (will run for 1 hour)...\n")

    # Run for 8H to collect data
    # In a real application, you might run indefinitely or until a specific condition
    await asyncio.sleep(8 * 60 * 60)

    # Disconnect gracefully
    print("\n\nDisconnecting...")
    await client.disconnect()
    print("Disconnected!")


if __name__ == "__main__":
    asyncio.run(main())