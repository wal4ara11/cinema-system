# Кинотеатр: Tickets + Loyalty (вариант 2)

Практическое задание по курсу «Архитектура распределённых систем. Способы межсервисного взаимодействия».

| Сервис | Порт | Назначение |
|---|---|---|
| tickets | 8001 | продажа билета `POST /tickets` (FastAPI, SQLite) |
| loyalty | 8002 | начисление бонусных баллов (FastAPI, SQLite) |
| rabbitmq | 5672, 15672 | брокер; панель — http://localhost:15672 (guest/guest) |

## Запуск

Нужен Docker Desktop. Команды для PowerShell из корня репозитория:

```powershell
docker compose up -d --build          # поднять стек
docker compose ps                     # все сервисы должны быть running, rabbitmq/tickets — healthy
Invoke-RestMethod http://localhost:8001/health
Invoke-RestMethod http://localhost:8002/health
docker compose logs -f tickets loyalty
docker compose down -v                # остановить и удалить данные (чистый старт)
```

## Режимы

Значения по умолчанию лежат в `.env`. Наборы для каждого шага лежат в `envs/`, запуск: `docker compose --env-file envs/<набор>.env up -d --build`.

| Переменная | Значения | Смысл |
|---|---|---|
| `MODE` | `sync` / `async` | HTTP-вызов Loyalty или событие `TicketSold` через RabbitMQ |
| `WORKER_THREADS` | `4` | сколько запросов Tickets обрабатывает одновременно |
| `LOYALTY_DELAY_MS` | `1500` / `20000` | искусственная задержка ответа Loyalty |
| `LOYALTY_TIMEOUT_SEC` | `0` (нет) / `1.5` | таймаут вызова Loyalty |
| `IDEMPOTENCY_ENABLED` | `true` / `false` | поддержка заголовка `Idempotency-Key` |
| `BREAKER_ENABLED` и др. | `true` / `false` | circuit breaker на вызов Loyalty |
| `ACCRUAL_MODE` | `naive` / `idempotent` | «прибавить баллы» или запись начисления с уникальным `ticket_id` |

Отчёт: [REPORT.md](REPORT.md).
