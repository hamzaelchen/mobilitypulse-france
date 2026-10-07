# Light image (no JVM): reads Gold Parquet with pyarrow and loads it into PostgreSQL.
# pytest is included so the integration tests can run next to the database.
FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
RUN pip install --no-cache-dir "psycopg[binary]==3.2.3" pyarrow==17.0.0 pytest==8.4.2 \
    && useradd --create-home --uid 1000 app
WORKDIR /app
USER app
CMD ["python", "-m", "loader.main"]
