from psycopg.types.json import Jsonb

from rdbms.schema import schema


def save(conn, rows, errors):
    """Một transaction cho cả lô; ON CONFLICT giúp ghi lại sau sự cố không sinh dòng trùng."""
    by_topic = {}
    for row in rows:
        if "du_lieu" in row:
            row["du_lieu"] = Jsonb(row["du_lieu"])
        by_topic.setdefault(row["topic"], []).append(row)
    with conn.transaction(), conn.cursor() as cur:
        for topic, group in by_topic.items():
            cols = list(schema(topic))
            cur.executemany(f"INSERT INTO {topic} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))}) "
                            f"ON CONFLICT DO NOTHING", [[r.get(c) for c in cols] for r in group])
        cur.executemany("INSERT INTO ban_ghi_loi VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", errors)