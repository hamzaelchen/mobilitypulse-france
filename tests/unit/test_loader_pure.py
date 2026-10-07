"""Pure-Python checks of the loader: migration files, generated SQL, settings (no database)."""

import re
from pathlib import Path

import pytest

from common.config import ConfigError, PostgresSettings
from loader.gold_loader import SPECS, build_mirror_delete_sql, build_upsert_sql
from loader.migrations import MigrationError, discover, validate_identifier

MIGRATIONS = Path(__file__).resolve().parents[2] / "sql" / "migrations"


def table_columns(sql_text: str, table: str) -> list[str]:
    """Columns of the LAST ``CREATE TABLE`` of ``table`` (a later migration may recreate it)."""
    bodies = re.findall(rf"CREATE TABLE \{{\{{schema\}}\}}\.{table} \((.*?)\n\);", sql_text, re.S)
    assert bodies, f"table {table} not found in migrations"
    columns = []
    for line in bodies[-1].splitlines():
        line = line.strip()
        if line and not line.startswith(("PRIMARY KEY", "--")):
            columns.append(line.split()[0])
    return columns


needs_migrations = pytest.mark.skipif(
    not MIGRATIONS.exists(), reason="sql/migrations is not mounted in this environment"
)


@needs_migrations
def test_migration_files_are_ordered_and_well_named():
    versions = [m.version for m in discover(MIGRATIONS)]
    assert versions == sorted(versions)
    assert versions[0].startswith("001_")


@needs_migrations
def test_loader_columns_match_the_sql_tables():
    sql_text = "\n".join(m.raw for m in discover(MIGRATIONS))
    for spec in SPECS:
        table = table_columns(sql_text, spec.name)
        assert set(spec.columns) <= set(table), spec.name
        # Every column of the table is either loaded or maintained by the database itself.
        assert set(table) - set(spec.columns) == {"loaded_at", "updated_at"}, spec.name
        assert set(spec.key) <= set(spec.columns)


def test_loader_columns_match_the_gold_spark_schemas():
    """Catches a Gold schema change that the loader/SQL did not follow (needs PySpark)."""
    pytest.importorskip("pyspark")
    from streaming.schemas import (
        GOLD_HOURLY_RATES_SCHEMA,
        GOLD_SERIES_15MIN_SCHEMA,
        GOLD_STATION_CURRENT_SCHEMA,
    )

    expected = {
        "station_current": GOLD_STATION_CURRENT_SCHEMA,
        "station_series_15min": GOLD_SERIES_15MIN_SCHEMA,
        "station_hourly_rates": GOLD_HOURLY_RATES_SCHEMA,
    }
    for spec in SPECS:
        gold = [f.name for f in expected[spec.name].fields if f.name != spec.partition_column]
        assert list(spec.columns) == gold, spec.name


def test_mirror_delete_sql_is_keyed_and_optionally_time_bounded():
    series = next(s for s in SPECS if s.name == "station_series_15min")
    full = build_mirror_delete_sql("mobility", series, bounded=False)
    assert full.startswith("DELETE FROM mobility.station_series_15min t WHERE NOT EXISTS")
    assert (
        "s.source = t.source AND s.station_id = t.station_id AND s.slot_start = t.slot_start"
        in full
    )
    assert "%s" not in full
    assert build_mirror_delete_sql("mobility", series, bounded=True).endswith("t.slot_start >= %s")


def test_upsert_sql_is_a_guarded_merge():
    spec = next(s for s in SPECS if s.name == "station_hourly_rates")
    create, copy, merge = build_upsert_sql("mobility", spec)
    assert "ON COMMIT DROP" in create
    assert copy.startswith("COPY _stage_station_hourly_rates (source, hour_start,")
    assert "ON CONFLICT (source, hour_start) DO UPDATE SET" in merge
    assert "IS DISTINCT FROM" in merge  # unchanged rows are not rewritten
    assert "updated_at = now()" in merge
    assert "source = EXCLUDED.source" not in merge  # keys are never updated
    assert merge.endswith("RETURNING (xmax = 0) AS inserted")


def test_identifiers_are_validated_before_being_interpolated():
    assert validate_identifier("mobility") == "mobility"
    for bad in ["Mobility", "a;drop table x", "1abc", "", "a-b", "x" * 64]:
        with pytest.raises(ValueError):
            validate_identifier(bad)
    with pytest.raises(ValueError):
        build_upsert_sql("mobility; DROP TABLE x", SPECS[0])


def test_bad_migration_file_names_are_rejected(tmp_path):
    (tmp_path / "init.sql").write_text("SELECT 1;")
    with pytest.raises(MigrationError):
        discover(tmp_path)
    (tmp_path / "init.sql").unlink()
    (tmp_path / "001_a.sql").write_text("SELECT 1;")
    (tmp_path / "001_b.sql").write_text("SELECT 2;")
    with pytest.raises(MigrationError):
        discover(tmp_path)


def test_settings_require_credentials_and_never_print_them(monkeypatch):
    for name in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ConfigError):
        PostgresSettings.from_env()
    monkeypatch.setenv("POSTGRES_DB", "db")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "s3cret-value")
    monkeypatch.setenv("GRAFANA_DB_PASSWORD", "ro-secret")
    settings = PostgresSettings.from_env()
    assert "s3cret-value" not in repr(settings)
    assert "ro-secret" not in repr(settings)
    assert settings.password == "s3cret-value"


def test_committed_files_hold_no_secret_values():
    root = Path(__file__).resolve().parents[2]
    if not (root / ".env.example").exists():
        pytest.skip("repository root files are not mounted in this environment")
    for path in [
        root / ".env.example",
        root / "docker-compose.yml",
        *(root / "grafana").rglob("*.yml"),
    ]:
        for line in path.read_text(encoding="utf-8").splitlines():
            match = re.match(r"\s*[A-Z_]*PASSWORD\s*[:=]\s*(\S+)", line)
            if match:
                value = match.group(1)
                assert value == "change-me" or value.startswith(("$", '"$', "${")), (
                    path.name,
                    line,
                )
