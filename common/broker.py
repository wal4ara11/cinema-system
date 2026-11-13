"""Общий для Tickets и Loyalty код работы с RabbitMQ.

Топология объявляется идемпотентно (повторный declare с теми же параметрами
ничего не меняет), поэтому её безопасно вызывать при старте обоих сервисов:
очередь и привязка существуют до первой публикации события.
"""
import logging
import os
import time

import pika

log = logging.getLogger("broker")

EXCHANGE = "cinema"                 # topic-обменник
EXCHANGE_TYPE = "topic"
ROUTING_KEY = "ticket.sold"
QUEUE = "loyalty.ticket_sold"       # именованная, durable, не exclusive, не auto-delete


def connect(host: str, attempts: int | None = None, delay_sec: float = 2.0) -> pika.BlockingConnection:
    """Подключение к брокеру с повторами: сервис не падает, если брокер ещё стартует."""
    attempts = attempts or int(os.getenv("BROKER_CONNECT_ATTEMPTS", "30"))
    params = pika.ConnectionParameters(host=host, heartbeat=30, blocked_connection_timeout=10)
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError as exc:
            last_exc = exc
            log.warning("RabbitMQ is not ready (attempt %d/%d), retrying in %.0fs", attempt, attempts, delay_sec)
            time.sleep(delay_sec)
    raise RuntimeError(f"cannot connect to RabbitMQ at {host}") from last_exc


def declare_topology(channel) -> None:
    channel.exchange_declare(exchange=EXCHANGE, exchange_type=EXCHANGE_TYPE, durable=True)
    channel.queue_declare(queue=QUEUE, durable=True, exclusive=False, auto_delete=False)
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)
    log.info("Topology declared: exchange=%s (%s, durable) -> queue=%s (durable) key=%s",
             EXCHANGE, EXCHANGE_TYPE, QUEUE, ROUTING_KEY)
