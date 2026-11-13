"""Начисление баллов. Две реализации — для сравнения на шаге 3."""
import db
import settings


def accrue(ticket_id: str, customer_id: str, points: int, message_id: str | None = None) -> str:
    """Возвращает 'accrued' или 'duplicate'."""
    with db.connect() as conn:
        if settings.ACCRUAL_MODE == "idempotent":
            # одна запись на билет; баланс = SUM(points) по таблице accruals
            cur = conn.execute(
                "INSERT INTO accruals (ticket_id, customer_id, points, message_id) VALUES (?, ?, ?, ?) "
                "ON CONFLICT (ticket_id) DO NOTHING",
                (ticket_id, customer_id, points, message_id))
            return "accrued" if cur.rowcount == 1 else "duplicate"
        # naive: «прибавить баллы» — каждое повторное обращение прибавляет ещё раз
        conn.execute(
            "INSERT INTO balances (customer_id, points) VALUES (?, ?) "
            "ON CONFLICT (customer_id) DO UPDATE SET points = points + excluded.points",
            (customer_id, points))
        return "accrued"
