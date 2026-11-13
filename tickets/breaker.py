"""Упрощённый circuit breaker.

CLOSED    — вызовы идут как обычно; считаем ошибки подряд.
OPEN      — после `threshold` ошибок подряд вызовы не делаются вообще (мгновенный отказ
            без сетевого запроса) в течение `open_sec` секунд.
HALF_OPEN — по истечении `open_sec` пропускается ровно ОДИН пробный вызов, остальные
            по-прежнему отклоняются. Успех пробного → CLOSED, неудача → снова OPEN
            на те же `open_sec` секунд.

Без трафика предохранитель сам по себе не переходит из OPEN: состояние пересматривается
только в момент очередного запроса (в allow()).
Ошибкой считаются таймаут, ошибка соединения и ответ 5xx (решает вызывающий код).
"""
import logging
import threading
import time

log = logging.getLogger("breaker")

CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, threshold: int, open_sec: float, clock=time.monotonic):
        self.threshold = threshold
        self.open_sec = open_sec
        self._clock = clock
        self._lock = threading.Lock()
        self.state = CLOSED
        self._fails = 0
        self._opened_at = 0.0

    def _to(self, state: str, reason: str) -> None:
        log.warning("Circuit breaker: %s -> %s (%s)", self.state, state, reason)
        self.state = state

    def allow(self) -> str | None:
        """None — вызов запрещён; 'call' — обычный вызов; 'probe' — пробный вызов."""
        with self._lock:
            if self.state == CLOSED:
                return "call"
            if self.state == OPEN and self._clock() - self._opened_at >= self.open_sec:
                self._to(HALF_OPEN, f"{self.open_sec:g}s passed, letting one probe call through")
                return "probe"
            return None          # OPEN, пока не вышло время, или HALF_OPEN с уже идущей пробой

    def success(self, permit: str) -> None:
        with self._lock:
            if permit == "probe":
                self._fails = 0
                self._to(CLOSED, "probe call succeeded")
            elif self.state == CLOSED:
                self._fails = 0

    def failure(self, permit: str) -> None:
        with self._lock:
            if permit == "probe":
                self._opened_at = self._clock()
                self._to(OPEN, f"probe call failed, open for {self.open_sec:g}s more")
            elif self.state == CLOSED:
                self._fails += 1
                if self._fails >= self.threshold:
                    self._opened_at = self._clock()
                    self._to(OPEN, f"{self._fails} consecutive failures, open for {self.open_sec:g}s")
            # запоздалый результат вызова, начатого до размыкания, состояние не меняет
