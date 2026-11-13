"""Вспомогательная утилита: опубликовать событие TicketSold с заданным message_id (в т.ч. несколько раз).
Имитирует повторную доставку. Запуск внутри контейнера:
  docker compose exec -T tickets python publish_event.py --message-id m-1 --ticket-id t-1 --customer c1 --times 2
"""
import argparse
import json
import os

import pika

from common import broker

p = argparse.ArgumentParser()
p.add_argument("--message-id", required=True)
p.add_argument("--ticket-id", required=True)
p.add_argument("--customer", required=True)
p.add_argument("--times", type=int, default=1)
a = p.parse_args()

conn = broker.connect(os.getenv("RABBITMQ_HOST", "rabbitmq"), attempts=3)
ch = conn.channel()
ch.confirm_delivery()
body = json.dumps({"ticket_id": a.ticket_id, "customer_id": a.customer, "session_id": "s1", "seat": "manual"})
for _ in range(a.times):
    ch.basic_publish(broker.EXCHANGE, broker.ROUTING_KEY, body,
                     pika.BasicProperties(message_id=a.message_id, delivery_mode=2, type="TicketSold"),
                     mandatory=True)
print(f"published TicketSold message_id={a.message_id} ticket_id={a.ticket_id} x{a.times}")
conn.close()
