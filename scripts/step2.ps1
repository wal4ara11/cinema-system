# Шаг 2: три прогона одной и той же нагрузки (8 запросов/с, 30 с, свой Idempotency-Key у каждого).
# Запуск из корня репозитория:  powershell -ExecutionPolicy Bypass -File scripts\step2.ps1
# Занимает около 5 минут. CSV с результатами каждого прогона остаются в каталоге results\.
$runs = @(
    @{ label = "1-none";    env = "envs/step2-none.env";    title = "Без защиты (Loyalty 20 с, таймаута нет)" },
    @{ label = "2-timeout"; env = "envs/step2-timeout.env"; title = "С таймаутом 1.5 с" },
    @{ label = "3-breaker"; env = "envs/step2-breaker.env"; title = "С таймаутом 1.5 с + circuit breaker" }
)
foreach ($r in $runs) {
    Write-Host "`n################ $($r.title) ################"
    docker compose --env-file $r.env down -v 2>&1 | Out-Null
    $upOut = docker compose --env-file $r.env up -d --build --wait 2>&1
    if ($LASTEXITCODE -ne 0) { $upOut | Out-Host; Write-Host "!! docker compose up не удался (см. выше). Чаще всего порт занят другим стеком: docker ps"; exit 1 }
    Write-Host "Стек поднят: $($r.env)"
    docker compose --env-file $r.env --profile tools build loadgen 2>&1 | Out-Null
    docker compose --env-file $r.env run --rm -e LABEL=$($r.label) loadgen
    if ($r.label -eq "3-breaker") {
        Write-Host "`n--- Переходы circuit breaker в логе Tickets ---"
        docker compose logs tickets --no-log-prefix | Select-String "Circuit breaker"
        Write-Host "--- Примеры пропущенных вызовов ---"
        docker compose logs tickets --no-log-prefix | Select-String "skipped" | Select-Object -First 3
    }
    if ($r.label -eq "2-timeout") {
        Write-Host "`n--- Примеры ошибок вызова Loyalty в логе Tickets ---"
        docker compose logs tickets --no-log-prefix | Select-String "Failed to accrue" | Select-Object -First 3
    }
}
Write-Host "`nГотово. Файлы: results\1-none.csv, 2-timeout.csv, 3-breaker.csv"
