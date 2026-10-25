import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


class RawWriter:
    def __init__(self, source, exchange_timezone="Asia/Ho_Chi_Minh", root_directory="data/raw"):
        self.source = source
        self.exchange_timezone = ZoneInfo(exchange_timezone)
        self.root_directory = Path(root_directory)
        self._write_lock = threading.Lock()     # lớp bảo hiểm; mô hình mới chỉ có một luồng ghi
        self._file_handle = None
        self._current_file_key = None

    def build_record(self, topic, payload, symbol=None, event_timestamp_utc=None,
                     sdk_received_timestamp_utc=None, sequence_number=None):
        """Tạo bản ghi (chạy ở producer, không đụng tới tệp). Trả về (file_key, record)."""
        current_utc_time = datetime.now(timezone.utc)
        record = {
            "Nguồn dữ liệu": self.source,
            "Chủ đề": topic,
            "Mã CK": symbol,
            "Thời gian gốc (UTC)": event_timestamp_utc,
            "Thời gian collector ghi nhận (UTC)": current_utc_time.isoformat(),
        }
        if sdk_received_timestamp_utc is not None:
            record["Thời gian SDK ghi nhận (UTC)"] = sdk_received_timestamp_utc
        if sequence_number is not None:
            record["Số thứ tự"] = sequence_number
        record["Nội dung gốc"] = payload

        local_time = current_utc_time.astimezone(self.exchange_timezone)
        file_key = (local_time.strftime("%Y-%m-%d"), local_time.strftime("%H"))
        return file_key, record

    def write_batch(self, keyed_records):
        """Ghi cả lô (danh sách (file_key, record)) rồi flush một lần."""
        if not keyed_records:
            return
        with self._write_lock:
            for file_key, record in keyed_records:
                if file_key != self._current_file_key:
                    self._rotate_file(file_key)
                try:
                    line = json.dumps(record, ensure_ascii=False, default=str)
                except (TypeError, ValueError):
                    # payload không mã hóa được: vẫn ghi dòng thay thế để không mất dấu vết
                    fallback_record = dict(record, **{"Nội dung gốc": repr(record.get("Nội dung gốc")), "payload_encoding_error": True})
                    line = json.dumps(fallback_record, ensure_ascii=False, default=str)
                self._file_handle.write(line + "\n")
            self._file_handle.flush()

    def close(self):
        with self._write_lock:
            if self._file_handle:
                self._file_handle.close()
                self._file_handle = None
            self._current_file_key = None

    def _rotate_file(self, file_key):
        """Đóng tệp cũ (nếu có) và mở tệp của giờ mới ở chế độ ghi nối tiếp."""
        if self._file_handle:
            self._file_handle.close()
        folder_path = self.root_directory / self.source / file_key[0]
        folder_path.mkdir(parents=True, exist_ok=True)
        self._file_handle = open(folder_path / f"{file_key[1]}.jsonl", "a", encoding="utf-8")
        self._current_file_key = file_key