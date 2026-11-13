"""Сервис Loyalty: начисление бонусных баллов за покупку билета.

HTTP-обработчики асинхронные, задержка — asyncio.sleep: сам Loyalty держит
сколько угодно медленных запросов одновременно и узким местом не становится.
"""
import asyncio
import logging
from contextlib import asynccontextmanager

import anyio.to_thread
from fastapi import FastAPI
from pydantic import BaseModel

import accrual
import consumer
import db
import settings
from common import broker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [loyalty] %(levelname)s %(name)s: %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger("loyalty")


def _declare_topology() -> None:
    conn = broker.connect(settings.RABBITMQ_HOST)
    try:
        broker.declare_topology(conn.channel())
    finally:
        conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init()
    if settings.MODE == "async":
        await anyio.to_thread.run_sync(_declare_topology)
        consumer.start()
    log.info("Loyalty started: %s", settings.summary())
    yield


app = FastAPI(title="Loyalty", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "loyalty", "mode": settings.MODE,
            "delay_ms": settings.LOYALTY_DELAY_MS, "accrual_mode": settings.ACCRUAL_MODE}


class AccrueRequest(BaseModel):
    ticket_id: str
    customer_id: str
    points: int | None = None


@app.post("/accrue")
async def accrue_points(req: AccrueRequest):
    """Синхронное начисление (MODE=sync). Задержка имитирует медленную зависимость."""
    if settings.LOYALTY_DELAY_MS:
        await asyncio.sleep(settings.LOYALTY_DELAY_MS / 1000)
    points = req.points or settings.POINTS_PER_TICKET
    result = await anyio.to_thread.run_sync(accrual.accrue, req.ticket_id, req.customer_id, points)
    log.info("Accrued %d points to customer=%s for ticket=%s (%s)", points, req.customer_id, req.ticket_id, result)
    return {"status": result, "ticket_id": req.ticket_id, "points": points}


@app.get("/balance/{customer_id}")
def balance(customer_id: str):
    with db.connect() as conn:
        counter = conn.execute("SELECT points FROM balances WHERE customer_id = ?", (customer_id,)).fetchone()
        accrued = conn.execute(
            "SELECT COALESCE(SUM(points), 0) AS points, COUNT(*) AS n FROM accruals WHERE customer_id = ?",
            (customer_id,)).fetchone()
    return {
        "customer_id": customer_id,
        "naive_counter_points": counter["points"] if counter else 0,
        "accruals_sum_points": accrued["points"],
        "accruals_count": accrued["n"],
    }
