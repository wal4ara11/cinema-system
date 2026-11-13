# Шаг 1: потеря ответа -> дубли -> Idempotency-Key.
# Запуск из корня репозитория:  powershell -ExecutionPolicy Bypass -File scripts\step1.ps1
$ErrorActionPreference = "Continue"
$T = "http://localhost:8001"; $L = "http://localhost:8002"

function Post-Ticket($seat, $customer, $key, $timeout) {
    $h = @{}; if ($key) { $h["Idempotency-Key"] = $key }
    $body = @{ session_id = "s1"; seat = $seat; customer_id = $customer } | ConvertTo-Json
    try {
        $r = Invoke-WebRequest -Uri "$T/tickets" -Method Post -Body $body -ContentType "application/json" `
             -Headers $h -TimeoutSec $timeout -UseBasicParsing
        Write-Host ("  -> HTTP {0} {1}" -f $r.StatusCode, $r.Content)
    } catch {
        $resp = $_.Exception.Response
        if ($resp) { Write-Host ("  -> HTTP {0}" -f [int]$resp.StatusCode) }
        else       { Write-Host "  -> клиент не получил ответ (таймаут $timeout с)" }
    }
}
function Show($seat, $customer) {
    $raw = (Invoke-WebRequest -Uri "$T/tickets" -UseBasicParsing).Content   # считаем по тексту: PS 5.1 отдаёт JSON-массив одним объектом
    $n = ([regex]::Matches($raw, '"seat":"' + $seat + '"')).Count
    $b = Invoke-RestMethod "$L/balance/$customer"
    Write-Host ("  билетов на место {0}: {1}; баллы клиента {2}: {3}" -f $seat, $n, $customer, $b.naive_counter_points)
}

Write-Host "`n=== Чистый старт, Loyalty отвечает 1.5 с ===" 
docker compose --env-file envs/step1.env down -v | Out-Null
$upOut = docker compose --env-file envs/step1.env up -d --build --wait 2>&1
if ($LASTEXITCODE -ne 0) { $upOut | Out-Host; Write-Host "!! docker compose up не удался (см. выше). Чаще всего порт занят другим стеком: docker ps"; exit 1 }

Write-Host "`n=== A. Без ключа: запрос, обрыв клиента через 1 с, повтор ==="
Post-Ticket "A1" "c-nokey" $null 1
Post-Ticket "A1" "c-nokey" $null 1
Start-Sleep -Seconds 3      # даём серверу дойти до конца
Show "A1" "c-nokey"

Write-Host "`n=== B. С Idempotency-Key: тот же сценарий ==="
Post-Ticket "B2" "c-key" "key-1" 1
Write-Host "  повтор, пока первый запрос ещё выполняется:"
Post-Ticket "B2" "c-key" "key-1" 5
Start-Sleep -Seconds 3
Write-Host "  повтор после завершения первого:"
Post-Ticket "B2" "c-key" "key-1" 5
Show "B2" "c-key"

Write-Host "`n=== Логи Tickets (последние строки) ==="
docker compose logs tickets --no-log-prefix --tail 12
