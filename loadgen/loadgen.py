"""Генератор нагрузки для шага 2.

Подаёт поток POST /tickets (RATE запросов в секунду в течение DURATION секунд), у каждого
запроса свой Idempotency-Key. Параллельно раз в секунду опрашивает GET /health.
Успех = ответ 201 не позже чем за MAX_TIME секунд (аналог curl --max-time 5).
"""
import csv
import os
import statistics
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

TARGET = os.getenv("TARGET", "http://tickets:8001")
RATE = float(os.getenv("RATE", "8"))
DURATION = float(os.getenv("DURATION", "30"))
MAX_TIME = float(os.getenv("MAX_TIME", "5"))
LABEL = os.getenv("LABEL", "run")
OUT_DIR = os.getenv("OUT_DIR", "/results")

results: list[dict] = []
health: list[tuple[float, str]] = []
lock = threading.Lock()
t0 = time.monotonic()


def send(i: int) -> None:
    started = time.monotonic()
    body = {"session_id": "s1", "seat": f"R{i // 20 + 1}-{i % 20 + 1}", "customer_id": f"c{i % 50}"}
    status = "ERR"
    try:
        r = httpx.post(f"{TARGET}/tickets", json=body, headers={"Idempotency-Key": str(uuid.uuid4())},
                       timeout=MAX_TIME)
        status = str(r.status_code)
    except httpx.TimeoutException:
        status = "TIMEOUT"
    except httpx.HTTPError:
        status = "ERR"
    elapsed = time.monotonic() - started
    with lock:
        results.append({"n": i, "t_start": round(started - t0, 2), "status": status,
                        "elapsed_ms": round(elapsed * 1000),
                        "ok": status == "201" and elapsed <= MAX_TIME})


def poll_health(stop: threading.Event) -> None:
    while not stop.is_set():
        started = time.monotonic()
        try:
            r = httpx.get(f"{TARGET}/health", timeout=2)
            state = "ok" if r.status_code == 200 else f"HTTP {r.status_code}"
        except httpx.HTTPError:
            state = "NO RESPONSE"
        with lock:
            health.append((round(started - t0, 1), state))
        stop.wait(max(0.0, 1.0 - (time.monotonic() - started)))


def pct(values: list[int], p: float) -> int:
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * p))] if values else 0


def main() -> None:
    total = int(RATE * DURATION)
    print(f"[{LABEL}] {total} запросов, {RATE:g}/с в течение {DURATION:g} с, успех = 201 за ≤ {MAX_TIME:g} с", flush=True)
    stop = threading.Event()
    hp = threading.Thread(target=poll_health, args=(stop,), daemon=True)
    hp.start()
    with ThreadPoolExecutor(max_workers=int(RATE * (MAX_TIME + 2)) + 10) as pool:
        for i in range(total):
            delay = t0 + i / RATE - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            pool.submit(send, i)
        # дождаться всех ответов (клиентский предел MAX_TIME)
    stop.set()
    hp.join(timeout=3)

    ok = [r for r in results if r["ok"]]
    answered = [r["elapsed_ms"] for r in results if r["status"] == "201"]
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    h_ok = sum(1 for _, s in health if s == "ok")
    print(f"\n=== Итог [{LABEL}] ===")
    print(f"успешных покупок (201 за ≤ {MAX_TIME:g} с): {len(ok)}/{len(results)} = {100 * len(ok) / max(1, len(results)):.0f}%")
    print(f"статусы: {dict(sorted(by_status.items()))}")
    if answered:
        print(f"время ответа среди 201: p50={pct(answered, .5)} мс, p95={pct(answered, .95)} мс, среднее={statistics.mean(answered):.0f} мс")
    print(f"/health: ok {h_ok} из {len(health)} опросов")
    line = " ".join(f"{t:.0f}с:{'ok' if s == 'ok' else 'X'}" for t, s in health)
    print(f"/health по секундам: {line}")

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, f"{LABEL}.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["n", "t_start", "status", "elapsed_ms", "ok"])
        w.writeheader()
        w.writerows(sorted(results, key=lambda r: r["n"]))


if __name__ == "__main__":
    main()
