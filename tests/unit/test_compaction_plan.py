import os
from datetime import date
from pathlib import Path

from streaming.compaction_plan import COMPACTED_PREFIX, PartitionPlan, plan_compaction

NOW = 1_000_000.0
TODAY = date(2026, 10, 6)


def make_partition(bronze: Path, day: str, names: list[str], mtime: float = NOW - 3600) -> Path:
    directory = bronze / f"ingestion_date={day}"
    directory.mkdir(parents=True)
    for name in names:
        file = directory / name
        file.write_bytes(b"x")
        os.utime(file, (mtime, mtime))
    return directory


def plan(bronze: Path, **kwargs) -> list[PartitionPlan]:
    return plan_compaction(bronze, today=TODAY, now=NOW, **kwargs)


def test_closed_partition_with_several_small_files_is_planned(tmp_path):
    make_partition(tmp_path, "2026-10-05", ["part-0-a.parquet", "part-0-b.parquet"])
    [result] = plan(tmp_path)
    assert result.partition_date == date(2026, 10, 5)
    assert [f.name for f in result.small_files] == ["part-0-a.parquet", "part-0-b.parquet"]


def test_today_and_future_partitions_are_never_planned(tmp_path):
    make_partition(tmp_path, "2026-10-06", ["part-0-a.parquet", "part-0-b.parquet"])
    make_partition(tmp_path, "2026-10-07", ["part-0-a.parquet", "part-0-b.parquet"])
    assert plan(tmp_path) == []


def test_single_file_and_already_compacted_partitions_are_skipped(tmp_path):
    make_partition(tmp_path, "2026-10-04", ["part-0-a.parquet"])
    make_partition(tmp_path, "2026-10-03", [f"{COMPACTED_PREFIX}abc.snappy.parquet"])
    assert plan(tmp_path) == []


def test_recently_written_file_blocks_the_partition(tmp_path):
    directory = make_partition(tmp_path, "2026-10-05", ["part-0-a.parquet", "part-0-b.parquet"])
    os.utime(directory / "part-0-b.parquet", (NOW - 10, NOW - 10))
    assert plan(tmp_path, min_file_age_seconds=600) == []
    assert len(plan(tmp_path, min_file_age_seconds=5)) == 1


def test_only_small_parquet_files_are_sources(tmp_path):
    make_partition(
        tmp_path,
        "2026-10-05",
        [
            "part-0-a.parquet",
            "part-0-b.parquet",
            ".part-0-a.parquet.crc",
            "_SUCCESS",
            f"{COMPACTED_PREFIX}old.snappy.parquet",
        ],
    )
    [result] = plan(tmp_path)
    assert [f.name for f in result.small_files] == ["part-0-a.parquet", "part-0-b.parquet"]


def test_unrelated_directories_are_ignored(tmp_path):
    (tmp_path / "ingestion_date=not-a-date").mkdir()
    (tmp_path / "other").mkdir()
    assert plan(tmp_path) == []


def test_target_name_is_deterministic_and_depends_on_sources(tmp_path):
    make_partition(tmp_path, "2026-10-05", ["part-0-a.parquet", "part-0-b.parquet"])
    make_partition(tmp_path, "2026-10-04", ["part-0-a.parquet", "part-0-c.parquet"])
    first, second = plan(tmp_path)  # sorted by partition name: 10-04 then 10-05
    assert first.target_name != second.target_name
    assert second.target_name == plan(tmp_path)[1].target_name
    assert second.target_name.startswith(COMPACTED_PREFIX)
