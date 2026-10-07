"""Versioned SQL migrations: ordered files, checksum-verified, applied once.

Files live in ``sql/migrations`` as ``NNN_description.sql`` and may use the ``{{schema}}``
placeholder. An applied migration must never change: its checksum is stored and compared.
The database connection is passed in (psycopg 3 style), so this module imports no driver.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_FILE_NAME = re.compile(r"^(\d{3,})_[a-z0-9_]+\.sql$")


class MigrationError(RuntimeError):
    """A migration set is inconsistent with what the database recorded."""


@dataclass(frozen=True)
class Migration:
    version: str  # file stem, e.g. "001_gold_tables"
    path: Path

    @property
    def raw(self) -> str:
        return self.path.read_text(encoding="utf-8")

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.raw.encode("utf-8")).hexdigest()

    def render(self, schema: str) -> str:
        return self.raw.replace("{{schema}}", validate_identifier(schema))


def validate_identifier(name: str) -> str:
    """Schema names are interpolated into SQL text: accept plain lowercase identifiers only."""
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def discover(directory: Path) -> list[Migration]:
    migrations: list[Migration] = []
    for path in sorted(directory.glob("*.sql")):
        if not _FILE_NAME.match(path.name):
            raise MigrationError(f"Migration file name must look like 001_name.sql: {path.name}")
        migrations.append(Migration(path.stem, path))
    numbers = [int(m.version.split("_", 1)[0]) for m in migrations]
    if len(set(numbers)) != len(numbers):
        raise MigrationError("Two migration files share the same number")
    return migrations


def apply_migrations(conn, directory: Path, schema: str) -> list[str]:
    """Apply pending migrations in order. Returns the versions applied by this call."""
    schema = validate_identifier(schema)
    migrations = discover(directory)
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"migrate:{schema}",))
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.schema_migrations ("
            "version text PRIMARY KEY, checksum text NOT NULL, "
            "applied_at timestamptz NOT NULL DEFAULT now())"
        )
        applied = dict(conn.execute(f"SELECT version, checksum FROM {schema}.schema_migrations"))
        known = {m.version for m in migrations}
        missing = sorted(set(applied) - known)
        if missing:
            raise MigrationError(f"Applied migrations missing from disk: {missing}")

        done: list[str] = []
        for migration in migrations:
            recorded = applied.get(migration.version)
            if recorded is not None:
                if recorded != migration.checksum:
                    raise MigrationError(
                        f"{migration.version} was modified after being applied "
                        "(checksum mismatch): add a new migration instead"
                    )
                continue
            conn.execute(migration.render(schema))
            conn.execute(
                f"INSERT INTO {schema}.schema_migrations (version, checksum) VALUES (%s, %s)",
                (migration.version, migration.checksum),
            )
            done.append(migration.version)
    return done
