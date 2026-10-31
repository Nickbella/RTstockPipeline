import logging
import os
import time

import psycopg

log = logging.getLogger("dnse.rdbms")


def connect(dsn=None, retry_seconds=5):
    """Kết nối PostgreSQL, thử lại đến khi được."""
    dsn = dsn or os.environ["DATABASE_URL"]
    while True:
        try:
            return psycopg.connect(dsn)
        except psycopg.OperationalError as error:
            log.error("Chưa kết nối được DB: %s. Thử lại sau %ss", error, retry_seconds)
            time.sleep(retry_seconds)