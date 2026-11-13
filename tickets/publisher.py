"""Публикация события TicketSold.

Свой connection на поток (pika.BlockingConnection не потокобезопасен), publisher confirms
включены: basic_publish возвращается только после того, как брокер принял сообщение.
Сообщение постоянное (delivery_mode=2), обменник durable, message_id задаётся при создании.
"""
import json
import logging
import threading
import uuid

import pika

import settings
from common import broker

log = logging.getLogger("publisher")
_local = threading.local()


def _channel():
    conn = getattr(_local, "conn", None)
    ch = getattr(_local, "ch", None)
    if conn is None or conn.is_closed or ch is None or ch.is_closed:
        conn = broker.connect(settings.RABBITMQ_HOST, attempts=3)
        ch = conn.channel()
        ch.confirm_delivery()
        _local.conn, _local.ch = conn, ch
    return ch


def publish_ticket_sold(ticket_id: str, customer_id: str, session_id: str, seat: str) -> str:
    message_id = str(uuid.uuid4())
    body = json.dumps({"ticket_id": ticket_id, "customer_id": customer_id,
                       "session_id": session_id, "seat": seat})
    props = pika.BasicProperties(message_id=message_id, delivery_mode=2, content_type="application/json",
                                 type="TicketSold")
    try:
        _channel().basic_publish(exchange=broker.EXCHANGE, routing_key=broker.ROUTING_KEY,
                                 body=body, properties=props, mandatory=True)
    except Exception:
        _local.conn = _local.ch = None        # пересоздать соединение при следующей публикации
        raise
    log.info("Published TicketSold ticket_id=%s message_id=%s", ticket_id, message_id)
    return message_id
