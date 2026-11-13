"""Consumer очереди loyalty.ticket_sold.

ack отправляется ТОЛЬКО после того, как начисление записано в БД. Если сервис упал или
остановлен, неподтверждённое сообщение вернётся в очередь и будет доставлено снова
(at-least-once), поэтому дубли — штатная ситуация, а не исключение.
"""
import json
import logging
import threading
import time

import pika

import accrual
import settings
from common import broker

log = logging.getLogger("consumer")


def _on_message(ch, method, props, body):
    message_id = props.message_id
    try:
        event = json.loads(body)
        ticket_id, customer_id = event["ticket_id"], event["customer_id"]
    except (ValueError, KeyError, TypeError):
        log.error("Bad message message_id=%s, rejecting without requeue: %r", message_id, body[:200])
        ch.basic_reject(method.delivery_tag, requeue=False)
        return
    try:
        result = accrual.accrue(ticket_id, customer_id, settings.POINTS_PER_TICKET, message_id)
    except Exception:
        log.exception("Accrual failed for message_id=%s, will be redelivered", message_id)
        time.sleep(1)
        ch.basic_nack(method.delivery_tag, requeue=True)
        return
    if result == "duplicate":
        log.info("Duplicate event ignored: ticket_id=%s message_id=%s (already accrued)", ticket_id, message_id)
    else:
        log.info("Accrued %d points to customer=%s for ticket=%s message_id=%s",
                 settings.POINTS_PER_TICKET, customer_id, ticket_id, message_id)
    ch.basic_ack(method.delivery_tag)          # подтверждаем после обработки


def _run() -> None:
    while True:
        try:
            conn = broker.connect(settings.RABBITMQ_HOST)
            ch = conn.channel()
            broker.declare_topology(ch)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=broker.QUEUE, on_message_callback=_on_message, auto_ack=False)
            log.info("Consuming queue %s", broker.QUEUE)
            ch.start_consuming()
        except Exception:
            log.exception("Consumer connection lost, reconnecting in 3s")
            time.sleep(3)


def start() -> threading.Thread:
    t = threading.Thread(target=_run, name="ticket-sold-consumer", daemon=True)
    t.start()
    return t
