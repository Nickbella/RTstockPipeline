from psycopg.types.json import Jsonb

from rdbms.schema import schema


def save(conn, rows, errors):
    """Một transaction cho cả lô; ON CONFLICT giúp ghi lại sau sự cố không sinh dòng trùng."""
    by_topic = {}
    for row in rows:
        if "data" in row:
            row["data"] = Jsonb(row["data"])
        by_topic.setdefault(row["topic"], []).append(row)
    with conn.transaction(), conn.cursor() as cur:
        for topic, group in by_topic.items():
            cols = list(schema(topic))
            cur.executemany(f"INSERT INTO {topic} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))}) "
                            f"ON CONFLICT DO NOTHING", [[r.get(c) for c in cols] for r in group])
        cur.executemany("INSERT INTO error_records (kafka_topic, kafka_partition, kafka_offset, error, content) "
                        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", errors)