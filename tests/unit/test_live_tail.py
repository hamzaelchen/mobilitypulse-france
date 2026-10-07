"""Tailing the Silver dataset: which files are new, and how rows become readings."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from live.silver_tail import list_new_files, read_readings  # noqa: E402

T0 = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


def silver_row(station="1", minute=0, **overrides):
    row = {
        "source": "s", "station_id": station, "station_name": f"S{station}",
        "latitude": 47.2, "longitude": -1.55, "capacity": 10,
        "num_bikes_available": 3, "num_docks_available": 7,
        "num_bikes_disabled": 0, "num_docks_disabled": 0,
        "is_installed": True, "is_renting": True, "is_returning": True,
        "event_timestamp": T0 - timedelta(hours=5),
        "observed_at": T0 + timedelta(minutes=minute), "kafka_offset": 10 + minute,
    }  # fmt: skip
    row.update(overrides)
    return row


def write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def touch(path, when):
    stamp = when.timestamp()
    os.utime(path, (stamp, stamp))
    return path


def test_rows_become_readings_with_renamed_fields(tmp_path):
    path = write(tmp_path / "a.parquet", [silver_row("1", 2)])
    (reading,) = read_readings([path])
    assert reading.key == ("s", "1")
    assert (reading.bikes, reading.docks, reading.bikes_disabled) == (3, 7, 0)
    assert reading.observed_at == T0 + timedelta(minutes=2)
    assert reading.last_reported_at == T0 - timedelta(hours=5)
    assert reading.kafka_offset == 12


def test_naive_timestamps_are_read_as_utc(tmp_path):
    naive = silver_row("1", 0, observed_at=T0.replace(tzinfo=None))
    path = write(tmp_path / "a.parquet", [naive])
    assert read_readings([path])[0].observed_at == T0


def test_rows_missing_a_required_value_are_skipped(tmp_path):
    rows = [
        silver_row("1"),
        silver_row("2", num_bikes_available=None),
        silver_row("3", station_name=None),
        silver_row("4", is_renting=None),
        silver_row("5", latitude=None),
    ]
    path = write(tmp_path / "a.parquet", rows)
    assert [r.station_id for r in read_readings([path])] == ["1"]


def test_an_unknown_capacity_is_kept(tmp_path):
    path = write(tmp_path / "a.parquet", [silver_row("1", capacity=None)])
    (reading,) = read_readings([path])
    assert reading.capacity is None


def test_an_unreadable_file_is_skipped_not_fatal(tmp_path):
    good = write(tmp_path / "good.parquet", [silver_row("1")])
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not parquet")
    assert [r.station_id for r in read_readings([bad, good])] == ["1"]


def test_only_settled_recent_parquet_files_are_listed(tmp_path):
    now = T0 + timedelta(hours=1)
    old = touch(
        write(tmp_path / "ingestion_date=2026-10-07" / "part-old.parquet", [silver_row()]), T0
    )
    new = touch(
        write(tmp_path / "ingestion_date=2026-10-07" / "part-new.parquet", [silver_row()]),
        now - timedelta(seconds=30),
    )
    writing = touch(
        write(tmp_path / "ingestion_date=2026-10-07" / "part-writing.parquet", [silver_row()]),
        now - timedelta(seconds=1),
    )
    temp = touch(
        write(tmp_path / "_temporary" / "0" / "part-tmp.parquet", [silver_row()]),
        now - timedelta(seconds=30),
    )
    meta = touch(
        write(tmp_path / "_spark_metadata" / "1.parquet", [silver_row()]),
        now - timedelta(seconds=30),
    )
    other = tmp_path / "ingestion_date=2026-10-07" / "notes.txt"
    other.write_text("x")
    touch(other, now - timedelta(seconds=30))

    files = list_new_files(tmp_path, since=now - timedelta(minutes=5), now=now, settle_seconds=2)

    assert files == [new]
    assert old not in files and writing not in files and temp not in files and meta not in files


def test_files_come_back_oldest_first(tmp_path):
    now = T0 + timedelta(hours=1)
    second = touch(
        write(tmp_path / "ingestion_date=d" / "part-b.parquet", [silver_row()]),
        now - timedelta(seconds=20),
    )
    first = touch(
        write(tmp_path / "ingestion_date=d" / "part-a.parquet", [silver_row()]),
        now - timedelta(seconds=40),
    )
    assert list_new_files(tmp_path, since=T0, now=now, settle_seconds=2) == [first, second]


def test_a_missing_root_is_an_empty_listing(tmp_path):
    assert list_new_files(tmp_path / "nope", since=T0, now=T0, settle_seconds=2) == []


def count_stats(monkeypatch):
    """Count Path.stat() calls per file name (stat is the slow part on a bind mount)."""
    calls = []
    real = type(Path()).stat

    def counting(self, *args, **kwargs):
        calls.append(self.name)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(type(Path()), "stat", counting)
    return calls


def test_files_older_than_the_window_are_stat_ed_only_once(tmp_path, monkeypatch):
    now = T0 + timedelta(hours=1)
    touch(write(tmp_path / "ingestion_date=2026-10-07" / "part-old.parquet", [silver_row()]), T0)
    new = touch(
        write(tmp_path / "ingestion_date=2026-10-07" / "part-new.parquet", [silver_row()]),
        now - timedelta(seconds=30),
    )
    skip: set = set()
    calls = count_stats(monkeypatch)

    first = list_new_files(
        tmp_path, since=now - timedelta(minutes=5), now=now, settle_seconds=2, skip=skip
    )
    second = list_new_files(
        tmp_path, since=now - timedelta(minutes=5), now=now, settle_seconds=2, skip=skip
    )

    assert first == second == [new]
    assert calls.count("part-old.parquet") == 1  # remembered as too old after the first listing
    assert calls.count("part-new.parquet") == 2  # a candidate is re-checked every time


def test_files_already_in_the_skip_set_are_never_stat_ed(tmp_path, monkeypatch):
    now = T0 + timedelta(hours=1)
    done = touch(
        write(tmp_path / "ingestion_date=2026-10-07" / "part-done.parquet", [silver_row()]),
        now - timedelta(seconds=30),
    )
    calls = count_stats(monkeypatch)
    files = list_new_files(tmp_path, since=T0, now=now, settle_seconds=2, skip={done})
    assert files == [] and "part-done.parquet" not in calls


def test_partitions_older_than_the_window_are_not_even_looked_at(tmp_path, monkeypatch):
    now = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
    touch(
        write(tmp_path / "ingestion_date=2026-10-05" / "part-ancient.parquet", [silver_row()]),
        now - timedelta(seconds=30),
    )
    calls = count_stats(monkeypatch)
    files = list_new_files(tmp_path, since=now - timedelta(minutes=5), now=now, settle_seconds=2)
    assert files == [] and "part-ancient.parquet" not in calls


def test_a_file_written_after_midnight_for_a_poll_of_the_day_before_is_still_listed(tmp_path):
    now = datetime(2026, 10, 8, 0, 3, tzinfo=UTC)
    late = touch(
        write(tmp_path / "ingestion_date=2026-10-07" / "part-late.parquet", [silver_row()]),
        now - timedelta(seconds=30),
    )
    files = list_new_files(tmp_path, since=now - timedelta(minutes=2), now=now, settle_seconds=2)
    assert files == [late]
