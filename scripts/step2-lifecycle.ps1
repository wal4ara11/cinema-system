# Шаг 2 (продолжение): жизненный цикл предохранителя.
# Loyalty исправен -> останавливаем его при идущей нагрузке -> breaker размыкается (OPEN),
# пробные вызовы падают -> поднимаем Loyalty -> пробный вызов успешен -> breaker замыкается (CLOSED).
# Запуск из корня репозитория:  powershell -ExecutionPolicy Bypass -File scripts\step2-lifecycle.ps1
$envf = "envs/step2-lifecycle.env"
function Breaker-Log { docker compose --env-file $envf logs tickets --no-log-prefix | Select-String "Circuit breaker" }
function Wait-Log($pattern, $sec) {
    for ($i = 0; $i -lt $sec; $i++) {
        if ((Breaker-Log | Out-String) -match $pattern) { return $true }
        Start-Sleep -Seconds 1
    }
    return $false
}
docker compose --env-file $envf down -v 2>&1 | Out-Null
$upOut = docker compose --env-file $envf up -d --build --wait 2>&1
if ($LASTEXITCODE -ne 0) { $upOut | Out-Host; Write-Host "!! docker compose up не удался (см. выше). Чаще всего порт занят другим стеком: docker ps"; exit 1 }
docker compose --env-file $envf --profile tools build loadgen 2>&1 | Out-Null
Write-Host "Стек поднят, Loyalty исправен. Запускаю нагрузку 6 запросов/с на 80 с..."

$dir = (Get-Location).Path
$job = Start-Job -ScriptBlock {
    param($d, $e)
    Set-Location $d
    docker compose --env-file $e run --rm -e RATE=6 -e DURATION=80 -e LABEL=lifecycle loadgen 2>&1
} -ArgumentList $dir, $envf

Start-Sleep -Seconds 10
Write-Host "`n[t=10 с] Останавливаю Loyalty (docker compose stop loyalty)"
docker compose --env-file $envf stop loyalty 2>&1 | Out-Null
if (Wait-Log "CLOSED -> OPEN" 30) { Write-Host "  -> предохранитель разомкнулся" } else { Write-Host "  !! OPEN не наступил" }

Write-Host "Жду 25 с: пробные вызовы при остановленном Loyalty должны возвращать его в OPEN..."
Start-Sleep -Seconds 25
Write-Host "`n[Loyalty поднимается обратно, задержка 0]"
docker compose --env-file $envf start loyalty 2>&1 | Out-Null
if (Wait-Log "probe call succeeded" 40) { Write-Host "  -> пробный вызов успешен, предохранитель замкнулся" } else { Write-Host "  !! CLOSED не наступил" }

Write-Host "`nЖду окончания нагрузки..."
Receive-Job $job -Wait | Out-Host
Write-Host "`n=== Переходы предохранителя в логе Tickets ==="
Breaker-Log
