import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


class RawWriter:
    """Ghi mỗi message thành một dòng JSON, đổi tệp theo giờ, thư mục theo ngày."""

    def __init__(self, source, exchange_tz="Asia/Ho_Chi_Minh",
                 root="data/raw", flush_every=50, flush_seconds=5):
        self.source = source
        self.tz = ZoneInfo(exchange_tz)
        self.root = Path(root)
        self.flush_every = flush_every
        self.flush_seconds = flush_seconds
        self._lock = threading.Lock()
        self._fh = None
        self._current_key = None
        self._count = 0
        self._last_flush = time.monotonic()

    def write(self, topic, payload, symbol=None, event_ts=None):
        now_utc = datetime.now(timezone.utc)
        record = {
            "source": self.source,
            "topic": topic,
            "symbol": symbol,
            "event_ts": event_ts,
            "recv_ts": now_utc.isoformat(),
            "payload": payload,
        }
        line = json.dumps(record, ensure_ascii=False, default=str) + "\n"

        local = now_utc.astimezone(self.tz)
        key = (local.strftime("%Y-%m-%d"), local.strftime("%H"))

        with self._lock:
            if key != self._current_key:
                self._rotate(key)
            self._fh.write(line)
            self._count += 1
            if (self._count % self.flush_every == 0
                    or time.monotonic() - self._last_flush >= self.flush_seconds):
                self._fh.flush()
                self._last_flush = time.monotonic()

    def _rotate(self, key):
        if self._fh:
            self._fh.close()
        folder = self.root / self.source / key[0]
        folder.mkdir(parents=True, exist_ok=True)
        self._fh = open(folder / f"{key[1]}.jsonl", "a", encoding="utf-8")
        self._current_key = key

    def close(self):
        with self._lock:
            if self._fh:
                self._fh.close()
                self._fh = None