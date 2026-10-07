# Light image (no JVM): read-only API + dashboard over PostgreSQL.
FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
RUN pip install --no-cache-dir fastapi==0.142.2 uvicorn==0.54.0 "psycopg[binary]==3.2.3" \
    && useradd --create-home --uid 1000 app
WORKDIR /app
USER app
CMD ["uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"]
