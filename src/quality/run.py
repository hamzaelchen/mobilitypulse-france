"""Run the serving checks, merge the latest lake report, store the results, print a summary.

Run: ``python -m quality.run [--no-lake] [--keep-days 30]`` (compose service ``quality``).
Exit code: 0 if no check FAILs, 1 otherwise, 2 for a configuration error.
"""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from common.config import ConfigError, PostgresSettings
from common.logging_utils import configure_logging
from loader.main import connect
from loader.migrations import validate_identifier
from quality.core import FAIL, CheckResult, format_results, from_json, measure, overall, summarize
from quality.serving_checks import run_serving_checks

logger = logging.getLogger("quality")


def lake_results(path: Path, now: datetime) -> list[CheckResult]:
    """Results of the Spark lake checks, plus how old that report is (never silently stale)."""
    if not path.exists():
        return [
            measure(
                "lake",
                "lake_report_age_minutes",
                None,
                f"no report at {path}",
                warn_above=60,
                fail_above=1440,
            )
        ]
    checked_at, results = from_json(path.read_text(encoding="utf-8"))
    age = (now - datetime.fromisoformat(checked_at)).total_seconds() / 60
    age_check = measure(
        "lake",
        "lake_report_age_minutes",
        round(age, 1),
        "age of the Spark lake report merged below",
        warn_above=60,
        fail_above=1440,
    )
    return [age_check, *results]


def store(conn, schema: str, run_id: uuid.UUID, checked_at: datetime, results, keep_days: int):
    schema = validate_identifier(schema)
    with conn.transaction():
        for r in results:
            conn.execute(
                f"INSERT INTO {schema}.data_quality_results "
                "(run_id, checked_at, layer, name, status, value, threshold, detail) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (run_id, checked_at, r.layer, r.name, r.status, r.value, r.threshold, r.detail),
            )
        conn.execute(
            f"DELETE FROM {schema}.data_quality_results WHERE checked_at < %s",
            (checked_at - timedelta(days=keep_days),),
        )


def run(
    conn,
    schema: str,
    *,
    now: datetime,
    lake_report: Path | None,
    keep_days: int = 30,
) -> tuple[uuid.UUID, list[CheckResult]]:
    results = run_serving_checks(conn, schema, now)
    if lake_report is not None:
        results += lake_results(lake_report, now)
    run_id = uuid.uuid4()
    store(conn, schema, run_id, now, results, keep_days)
    return run_id, results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-lake", action="store_true", help="do not merge the lake report")
    parser.add_argument("--keep-days", type=int, default=30)
    args = parser.parse_args()

    configure_logging()
    try:
        settings = PostgresSettings.from_env()
    except ConfigError as exc:
        logger.error("Invalid configuration: %s", exc)
        return 2
    report = (
        None
        if args.no_lake
        else Path(os.environ.get("DATA_DIR", "/data")) / "quality" / "lake_latest.json"
    )
    now = datetime.now(UTC)
    with connect(settings) as conn:
        run_id, results = run(
            conn, settings.schema, now=now, lake_report=report, keep_days=args.keep_days
        )
    print(format_results(results))
    counts = summarize(results)
    print(f"run {run_id}: overall {overall(results)} - {counts}")
    return 1 if any(r.status == FAIL for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
