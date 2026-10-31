"""Đọc data/raw/dnse/*.jsonl -> normalize() -> PostgreSQL (không qua Kafka).

Chạy từ thư mục gốc:
    python -u -m tools.replay_raw                    # nạp toàn bộ
    python -u -m tools.replay_raw --date 2026-10-30  # chỉ nạp một ngày
"""
import argparse
import json
import logging
from collections import Counter
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()
from consumer.normalizer import normalize
from rdbms import RAW_TOPICS, connect, create_tables, save

log = logging.getLogger("dnse.replay")
BATCH = 500
RAW_DIRECTORY = Path("data/raw/dnse")
LEGACY_TOPICS = {"ohlc_1": "ohlc_1_minute", "foreign": "foreign_trading"}   # tên topic của dữ liệu thu cũ


def flush(conn, rows, errors):
    """Ghi một lô. Trả về (số dòng sạch, số dòng lỗi) thực tế đã ghi."""
    if not (rows or errors):
        return 0, 0
    try:
        save(conn, rows, errors)
        return len(rows), len(errors)
    except psycopg.Error as error:           # lô có dòng xấu: ghi từng dòng để cô lập
        log.warning("Lô lỗi (%s), ghi từng dòng", error)
        ok = 0
        for row in rows:
            try:
                save(conn, [row], [])
                ok += 1
            except psycopg.Error as row_error:
                errors.append((row["kafka_topic"], row["kafka_partition"], row["kafka_offset"],
                               str(row_error), ""))
        save(conn, [], errors)
        return ok, len(errors)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Chỉ nạp một ngày, định dạng YYYY-MM-DD, ví dụ 2026-10-30")
    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    if arguments.date:
        files = sorted((RAW_DIRECTORY / arguments.date).glob("*.jsonl"))
        if not files:
            raise SystemExit(f"Không có file nào trong {RAW_DIRECTORY / arguments.date}")
    else:
        files = sorted(RAW_DIRECTORY.rglob("*.jsonl"))

    conn = connect()
    create_tables(conn)
    total_ok = total_err = 0
    reasons = Counter()
    for path in files:
        partition = int(path.parent.name.replace("-", "") + path.stem)    # ví dụ 2026103009
        rows, errors = [], []
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    record["topic"] = LEGACY_TOPICS.get(record.get("topic"), record.get("topic"))
                    row = normalize(record)
                    if row is None:
                        raise ValueError("normalize trả về None (thiếu mã/thời gian/trường bắt buộc hoặc giá-khối lượng <= 0)")
                    if row["topic"] not in RAW_TOPICS:
                        raise ValueError(f"topic không hỗ trợ: {row['topic']}")
                    row.update(kafka_topic=f"file-{row['topic']}", kafka_partition=partition,
                               kafka_offset=line_number)
                    rows.append(row)
                except Exception as error:
                    reasons[str(error)[:80]] += 1
                    errors.append((f"file-{path.parent.name}", partition, line_number,
                                   str(error), line[:2000]))
                if len(rows) + len(errors) >= BATCH:
                    ok, err = flush(conn, rows, errors)
                    total_ok += ok
                    total_err += err
                    rows, errors = [], []
        ok, err = flush(conn, rows, errors)
        total_ok += ok
        total_err += err
        log.info("%s xong | tổng sạch %d, lỗi %d", path, total_ok, total_err)
    conn.close()
    log.info("Hoàn tất | sạch %d, lỗi %d | lý do lỗi: %s", total_ok, total_err, dict(reasons))


if __name__ == "__main__":
    main()