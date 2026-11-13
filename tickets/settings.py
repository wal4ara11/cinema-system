import os


def _bool(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


MODE = os.getenv("MODE", "sync").strip().lower()                     # sync | async
WORKER_THREADS = int(os.getenv("WORKER_THREADS", "4"))               # одновременно обрабатываемых запросов
LOYALTY_URL = os.getenv("LOYALTY_URL", "http://localhost:8002")
LOYALTY_TIMEOUT_SEC = float(os.getenv("LOYALTY_TIMEOUT_SEC", "0"))   # 0 — без таймаута
IDEMPOTENCY_ENABLED = _bool("IDEMPOTENCY_ENABLED", "true")
BREAKER_ENABLED = _bool("BREAKER_ENABLED", "false")
BREAKER_FAIL_THRESHOLD = int(os.getenv("BREAKER_FAIL_THRESHOLD", "5"))
BREAKER_OPEN_SEC = float(os.getenv("BREAKER_OPEN_SEC", "10"))
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
DB_PATH = os.getenv("DB_PATH", "./tickets.db")


def summary() -> str:
    return (f"MODE={MODE} WORKER_THREADS={WORKER_THREADS} LOYALTY_TIMEOUT_SEC={LOYALTY_TIMEOUT_SEC or 'none'} "
            f"IDEMPOTENCY_ENABLED={IDEMPOTENCY_ENABLED} BREAKER_ENABLED={BREAKER_ENABLED} "
            f"(threshold={BREAKER_FAIL_THRESHOLD}, open={BREAKER_OPEN_SEC}s)")
