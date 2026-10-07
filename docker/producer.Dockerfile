FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app
RUN pip install --no-cache-dir requests==2.32.5 confluent-kafka==2.11.1 \
    && useradd --create-home --uid 1000 app
COPY src/ /app/src/
USER app
CMD ["python", "-m", "producer.main"]
