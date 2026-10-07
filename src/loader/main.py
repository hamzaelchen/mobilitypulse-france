"""CLI: apply SQL migrations, then load Gold into PostgreSQL.

Run: ``python -m loader.main [--migrate-only] [--days N]`` (compose service ``loader``).
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from common.config import ConfigError, PostgresSettings
from common.logging_utils import configure_logging, log_fields
from loader.gold_loader import SPECS, load_gold, specs_without_current
from loader.migrations import apply_migrations, validate_identifier

logger = logging.getLogger("loader")


def connect(settings: PostgresSettings):
    import psycopg  # lazy: unit tests of pure helpers do not need the driver

    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.database,
        user=settings.user,
        password=settings.password,
        connect_timeout=10,
    )


def ensure_reader_role(conn, settings: PostgresSettings) -> bool:
    """Read-only role for Grafana. Returns False when no password is configured (role skipped)."""
    from psycopg import sql

    if not settings.reader_password:
        return False
    role = sql.Identifier(settings.reader_user)
    schema = sql.Identifier(validate_identifier(settings.schema))
    exists = conn.execute(
        "SELECT 1 FROM pg_roles WHERE rolname = %s", (settings.reader_user,)
    ).fetchone()
    verb = "ALTER" if exists else "CREATE"
    password = sql.Literal(settings.reader_password)
    with conn.transaction():
        conn.execute(sql.SQL(f"{verb} ROLE {{}} LOGIN PASSWORD {{}}").format(role, password))
        conn.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(settings.database), role
            )
        )
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, role))
        conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(schema, role))
        conn.execute(
            sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}").format(
                schema, role
            )
        )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--migrate-only", action="store_true", help="skip the Gold load")
    parser.add_argument(
        "--days", type=int, help="only load partitions of the last N days (default: all)"
    )
    parser.add_argument(
        "--skip-current",
        action="store_true",
        help="do not touch station_current (owned by the live updater)",
    )
    args = parser.parse_args()

    configure_logging()
    try:
        settings = PostgresSettings.from_env()
    except ConfigError as exc:
        logger.error("Invalid configuration: %s", exc)
        return 2
    migrations_dir = Path(os.environ.get("MIGRATIONS_DIR", "/app/sql/migrations"))
    gold_dir = Path(os.environ.get("DATA_DIR", "/data")) / "gold"

    with connect(settings) as conn:
        applied = apply_migrations(conn, migrations_dir, settings.schema)
        logger.info("Migrations applied", extra=log_fields(applied=applied))
        reader = ensure_reader_role(conn, settings)
        logger.info("Reader role", extra=log_fields(role=settings.reader_user, configured=reader))
        if args.migrate_only:
            return 0
        since = (datetime.now(UTC) - timedelta(days=args.days)).date() if args.days else None
        specs = specs_without_current() if args.skip_current else SPECS
        results = load_gold(conn, settings.schema, gold_dir, since=since, specs=specs)
    for r in results:
        logger.info(
            "Loaded",
            extra=log_fields(
                table=r.table, rows=r.rows_read, inserted=r.inserted,
                updated=r.updated, unchanged=r.unchanged, deleted=r.deleted,
            ),
        )  # fmt: skip
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
