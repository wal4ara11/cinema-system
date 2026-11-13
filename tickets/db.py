"""SQLite-хранилище Tickets. Соединение открывается на каждую операцию —
так проще и безопасно при работе из нескольких потоков."""
import os
import sqlite3
from contextlib import contextmanager

import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS tickets (
    id          TEXT PRIMARY KEY,
    session_id  TEXT NOT NULL,
    seat        TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
-- проверки занятости места на шаге 1 нет намеренно: иначе дубль не воспроизвести

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT PRIMARY KEY,
    status        TEXT NOT NULL CHECK (status IN ('processing', 'done')),
    response_code INTEGER,
    response_body TEXT,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


@contextmanager
def connect():
    conn = sqlite3.connect(settings.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        with conn:          # commit при успехе, rollback при исключении
            yield conn
    finally:
        conn.close()


def init() -> None:
    os.makedirs(os.path.dirname(os.path.abspath(settings.DB_PATH)), exist_ok=True)
    with connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
