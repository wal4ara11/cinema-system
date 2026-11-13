"""SQLite-хранилище Loyalty (на volume — переживает перезапуск контейнера)."""
import os
import sqlite3
from contextlib import contextmanager

import settings

SCHEMA = """
-- ACCRUAL_MODE=naive: «прибавить баллы» к счётчику; повторная доставка удваивает баллы
CREATE TABLE IF NOT EXISTS balances (
    customer_id TEXT PRIMARY KEY,
    points      INTEGER NOT NULL DEFAULT 0
);

-- ACCRUAL_MODE=idempotent: одна запись начисления на билет, баланс = SUM(points)
CREATE TABLE IF NOT EXISTS accruals (
    ticket_id   TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    points      INTEGER NOT NULL,
    message_id  TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_accruals_customer ON accruals (customer_id);
"""


@contextmanager
def connect():
    conn = sqlite3.connect(settings.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init() -> None:
    os.makedirs(os.path.dirname(os.path.abspath(settings.DB_PATH)), exist_ok=True)
    with connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
