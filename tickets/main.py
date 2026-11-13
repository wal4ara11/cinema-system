"""Сервис Tickets: продажа билетов на сеанс.

Все обработчики объявлены обычными (def), поэтому выполняются в пуле потоков.
Размер пула ограничен WORKER_THREADS (по заданию — 4): это и есть «рабочие места»
сервиса. /health обслуживается тем же пулом, поэтому при их исчерпании он тоже
перестаёт отвечать — так каскадный отказ виден снаружи.
"""
import json
import logging
import uuid
from contextlib import asynccontextmanager

import anyio.to_thread
import httpx
from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import db
import settings
import publisher
from breaker import CircuitBreaker
from common import broker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [tickets] %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)   # не засорять лог строкой на каждый вызов
logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger("tickets")


def _declare_topology() -> None:
    conn = broker.connect(settings.RABBITMQ_HOST)
    try:
        broker.declare_topology(conn.channel())
    finally:
        conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    anyio.to_thread.current_default_thread_limiter().total_tokens = settings.WORKER_THREADS
    db.init()
    try:
        # очередь и привязка должны существовать до первой публикации TicketSold
        await anyio.to_thread.run_sync(_declare_topology)
    except Exception:
        if settings.MODE == "async":
            raise
        log.exception("RabbitMQ topology was not declared (not critical in sync mode)")
    log.info("Tickets started: %s", settings.summary())
    yield


app = FastAPI(title="Tickets", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "tickets", "mode": settings.MODE,
            "breaker": breaker_.state if breaker_ else "disabled"}


class TicketRequest(BaseModel):
    session_id: str
    seat: str
    customer_id: str


breaker_ = CircuitBreaker(settings.BREAKER_FAIL_THRESHOLD, settings.BREAKER_OPEN_SEC) if settings.BREAKER_ENABLED else None


def _call_loyalty(ticket_id: str, customer_id: str) -> str:
    """Синхронный вызов Loyalty. Токен отмены входящего запроса сюда не передаётся:
    вызов выполняется до конца, даже если клиент уже оборвал соединение.
    Любая ошибка не отменяет продажу — билет уже сохранён, в лог пишется предупреждение."""
    permit = "call"
    if breaker_:
        permit = breaker_.allow()
        if permit is None:
            log.warning("Circuit is %s: Loyalty call skipped for ticket_id=%s", breaker_.state, ticket_id)
            return "skipped"
    timeout = settings.LOYALTY_TIMEOUT_SEC or None
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.post(f"{settings.LOYALTY_URL}/accrue",
                            json={"ticket_id": ticket_id, "customer_id": customer_id})
        if r.status_code >= 500:
            raise RuntimeError(f"Loyalty answered {r.status_code}")
    except Exception as exc:
        log.warning("Failed to accrue points for ticket_id=%s: %s", ticket_id, repr(exc))
        if breaker_:
            breaker_.failure(permit)
        return "failed"
    if breaker_:
        breaker_.success(permit)
    return "accrued"


def _create_ticket(req: TicketRequest) -> tuple[int, dict]:
    ticket_id = str(uuid.uuid4())
    with db.connect() as conn:
        conn.execute("INSERT INTO tickets (id, session_id, seat, customer_id) VALUES (?, ?, ?, ?)",
                     (ticket_id, req.session_id, req.seat, req.customer_id))
    log.info("Ticket saved ticket_id=%s session=%s seat=%s", ticket_id, req.session_id, req.seat)
    if settings.MODE == "async":
        # синхронного вызова нет: публикуем событие и сразу отвечаем клиенту
        try:
            message_id = publisher.publish_ticket_sold(ticket_id, req.customer_id, req.session_id, req.seat)
            loyalty = "queued"
        except Exception:
            # проблема двойной записи: билет сохранён, а событие не опубликовано (лечится Transactional Outbox)
            log.exception("Ticket %s saved but TicketSold was NOT published", ticket_id)
            loyalty = "event_not_published"
    else:
        loyalty = _call_loyalty(ticket_id, req.customer_id)
    return 201, {"ticket_id": ticket_id, "session_id": req.session_id, "seat": req.seat,
                 "customer_id": req.customer_id, "loyalty": loyalty}


@app.post("/tickets")
def create_ticket(req: TicketRequest, idempotency_key: str | None = Header(default=None)):
    if not (settings.IDEMPOTENCY_ENABLED and idempotency_key):
        code, body = _create_ticket(req)
        return JSONResponse(body, status_code=code)

    # Ключ резервируется ДО обработки, поэтому повтор, пришедший пока первый запрос
    # ещё выполняется, видит статус 'processing' и получает 409 Conflict.
    with db.connect() as conn:
        cur = conn.execute("INSERT INTO idempotency_keys (key, status) VALUES (?, 'processing') "
                           "ON CONFLICT (key) DO NOTHING", (idempotency_key,))
        first = cur.rowcount == 1
    if not first:
        with db.connect() as conn:
            row = conn.execute("SELECT * FROM idempotency_keys WHERE key = ?", (idempotency_key,)).fetchone()
        if row["status"] == "done":
            log.info("Idempotency-Key=%s replayed, returning stored response", idempotency_key)
            return JSONResponse(json.loads(row["response_body"]), status_code=row["response_code"],
                                headers={"Idempotent-Replay": "true"})
        log.info("Idempotency-Key=%s is still processing, answering 409", idempotency_key)
        return JSONResponse({"error": "request with this Idempotency-Key is still being processed"},
                            status_code=409, headers={"Retry-After": "2"})
    try:
        code, body = _create_ticket(req)
    except Exception:
        with db.connect() as conn:          # освободить ключ, чтобы клиент мог повторить
            conn.execute("DELETE FROM idempotency_keys WHERE key = ?", (idempotency_key,))
        raise
    with db.connect() as conn:
        conn.execute("UPDATE idempotency_keys SET status = 'done', response_code = ?, response_body = ? "
                     "WHERE key = ?", (code, json.dumps(body), idempotency_key))
    return JSONResponse(body, status_code=code)


@app.get("/tickets")
def list_tickets():
    with db.connect() as conn:
        rows = conn.execute("SELECT * FROM tickets ORDER BY created_at").fetchall()
    return [dict(r) for r in rows]
