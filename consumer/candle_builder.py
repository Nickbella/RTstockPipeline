import os, json, sys 
import argparse, time
import uuid, dataclasses, collections
from datetime import datetime, timedelta

class Candle():
    def __init__(self, symbol, minute_start, open_event_time, open_price, high_price, low_price, close_price, volume):
        self.symbol = symbol
        self.minute_start = minute_start
        self.open_event_time = open_event_time
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume
        self.traded_value = self.close_price * self.volume
        self.open_event_time = datetime.fromtimestamp(self.open_event_time / 1000)
        self.closed_event_time = self.open_event_time + timedelta(milliseconds=self.minute_start)

    def to_dict(self):
        return {
            "Mã CK": self.symbol,
            "Phút tính nến": self.minute_start,
            "Thời gian gốc": self.open_event_time,
            "Giá mở cửa": self.open_price,
            "Giá cao nhất": self.high_price,
            "Giá thấp nhất": self.low_price,
            "Giá đóng cửa": self.close_price,
            "Khối lượng GD": self.volume
        }

    def add_trade(self, price, volume):
        self.volume += volume
        self.traded_value += price * volume
        self.close_price = price
        if price > self.high_price:
            self.high_price = price
        if price < self.low_price:
            self.low_price = price

    def vwap(self):
        if self.volume == 0:
            return 0
        return self.traded_value / self.volume

class SeenTradeKeys(set):
    """Track trade keys and provide a convenient duplicate check."""

    def __init__(self, keys=()):
        super().__init__(keys or ())

    def add_if_new(self, key):
        """Add a key and return whether it was not already present."""
        if key in self:
            return False
        self.add(key)
        return True

    def filter_new(self, keys):
        """Return keys not seen before, preserving their original order."""
        return [key for key in keys if self.add_if_new(key)]


class CandleBuilder():
    def __init__(self, seen_trade_keys, open_candles, duplicated_count, max_event_time):
        self.seen_trade_keys = (
            seen_trade_keys
            if isinstance(seen_trade_keys, SeenTradeKeys)
            else SeenTradeKeys(seen_trade_keys or ())
        )
        self.open_candles = self.Candle.to_dict()
        self.duplicated_count = duplicated_count
        self.max_event_time = max_event_time        
