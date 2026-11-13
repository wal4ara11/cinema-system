import os

MODE = os.getenv("MODE", "sync").strip().lower()                      # sync | async
LOYALTY_DELAY_MS = int(os.getenv("LOYALTY_DELAY_MS", "0"))           # искусственная задержка ответа
ACCRUAL_MODE = os.getenv("ACCRUAL_MODE", "naive").strip().lower()    # naive | idempotent
POINTS_PER_TICKET = int(os.getenv("POINTS_PER_TICKET", "50"))
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
DB_PATH = os.getenv("DB_PATH", "./loyalty.db")


def summary() -> str:
    return (f"MODE={MODE} LOYALTY_DELAY_MS={LOYALTY_DELAY_MS} ACCRUAL_MODE={ACCRUAL_MODE} "
            f"POINTS_PER_TICKET={POINTS_PER_TICKET}")
