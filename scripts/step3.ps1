# Шаг 3: события. Запуск из корня репозитория:  powershell -ExecutionPolicy Bypass -File scripts\step3.ps1
$T = "http://localhost:8001"; $L = "http://localhost:8002"
$naive = "envs/step3-naive.env"; $idem = "envs/step3-idempotent.env"

function Up($envf, $svc) {
    $o = docker compose --env-file $envf up -d --build --wait $svc 2>&1
    if ($LASTEXITCODE -ne 0) { $o | Out-Host; Write-Host "!! docker compose up не удался (см. выше). Чаще всего порт занят другим стеком: docker ps"; exit 1 }
}
function Buy($seat, $customer) {
    $body = @{ session_id = "s1"; seat = $seat; customer_id = $customer } | ConvertTo-Json
    $h = @{ "Idempotency-Key" = [guid]::NewGuid().ToString() }
    $r = $null
    $ms = (Measure-Command { $r = Invoke-WebRequest -Uri "$T/tickets" -Method Post -Body $body -ContentType "application/json" -Headers $h -UseBasicParsing }).TotalMilliseconds
    Write-Host ("  билет {0} ({1}): HTTP {2} за {3:N0} мс  {4}" -f $seat, $customer, $r.StatusCode, $ms, $r.Content)
}
function Queue { docker compose --env-file $naive exec -T rabbitmq rabbitmqctl -q list_queues name messages_ready messages_unacknowledged durable auto_delete exclusive 2>&1 | Where-Object { $_ -match "loyalty|name" } }
function Bal($c) { $b = Invoke-RestMethod "$L/balance/$c"; Write-Host ("  клиент {0}: счётчик «прибавить» = {1}; сумма по записям начислений = {2} (записей: {3})" -f $c, $b.naive_counter_points, $b.accruals_sum_points, $b.accruals_count) }
function Publish($mid, $tid, $cust, $n) { docker compose --env-file $naive exec -T tickets python publish_event.py --message-id $mid --ticket-id $tid --customer $cust --times $n }

Write-Host "`n=== Чистый старт, режим событий (MODE=async), начисление «прибавить баллы» ==="
docker compose --env-file $naive down -v 2>&1 | Out-Null
Up $naive ""

Write-Host "`n=== 1. Останавливаю Loyalty полностью и продаю три билета ==="
docker compose --env-file $naive stop loyalty 2>&1 | Out-Null
Buy "A1" "c-a1"; Buy "A2" "c-a2"; Buy "A3" "c-a3"
Write-Host "  Очередь в RabbitMQ (события накопились, очередь durable, не exclusive, не auto_delete):"
Queue

Write-Host "`n=== 2. Поднимаю Loyalty - баллы начисляются задним числом ==="
docker compose --env-file $naive start loyalty 2>&1 | Out-Null
for ($i = 0; $i -lt 30; $i++) { try { if ((Invoke-RestMethod "$L/balance/c-a3").naive_counter_points -gt 0) { break } } catch {}; Start-Sleep -Seconds 1 }
Bal "c-a1"; Bal "c-a2"; Bal "c-a3"
Write-Host "  Очередь после обработки:"; Queue
docker compose --env-file $naive logs loyalty --no-log-prefix | Select-String "Accrued" | Select-Object -Last 3

Write-Host "`n=== 3. Повторная доставка при начислении «прибавить баллы» ==="
Publish "m-dup-1" "ticket-dup-1" "c-dup" 2
Start-Sleep -Seconds 3
Bal "c-dup"
Write-Host "  (одно событие, дважды один и тот же message_id -> баллы удвоились)"

Write-Host "`n=== 4. Начисление с уникальным ticket_id: INSERT ... ON CONFLICT (ticket_id) DO NOTHING ==="
Up $idem "loyalty"
Publish "m-dup-2" "ticket-dup-2" "c-dup2" 2
Start-Sleep -Seconds 3
Bal "c-dup2"
docker compose --env-file $idem logs loyalty --no-log-prefix | Select-String "dup" | Select-Object -Last 3
Write-Host "  (то же самое событие дважды -> одна запись, баланс не изменился при повторе)"
