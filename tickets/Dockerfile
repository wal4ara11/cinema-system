FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY tickets/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ ./common/
COPY tickets/ ./
EXPOSE 8001
# один процесс: состояние circuit breaker общее для всех потоков;
# параллелизм ограничивается пулом потоков (WORKER_THREADS) внутри приложения
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1"]
