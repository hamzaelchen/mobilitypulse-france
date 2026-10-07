"""Spark-side validation. Needs Java: runs inside the Spark image, skipped on a bare host."""

from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from producer.inject_invalid import _VALID_BASE, broken_messages  # noqa: E402
from producer.publisher import encode_event  # noqa: E402
from streaming.validation import parse_kafka_messages, to_bronze, to_rejected  # noqa: E402

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def kafka_df(spark, messages):
    ts = datetime(2026, 10, 6, 12, 0, 0)
    rows = [(k.encode(), v, "t", 0, i, ts) for i, (k, v) in enumerate(messages)]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def test_valid_event_lands_in_bronze_with_kafka_metadata(spark):
    classified = parse_kafka_messages(kafka_df(spark, [("999", encode_event(_VALID_BASE))]))
    bronze = to_bronze(classified).collect()
    assert len(bronze) == 1
    row = bronze[0]
    assert row.station_id == "999"
    assert row.num_bikes_available == 3
    assert row.kafka_offset == 0
    assert row.kafka_topic == "t"
    assert row.event_timestamp == datetime(2026, 1, 1, 0, 0, 0)
    assert str(row.ingestion_date) == "2026-01-01"
    assert to_rejected(classified).count() == 0


def test_every_broken_message_is_rejected_and_none_reaches_bronze(spark):
    messages = broken_messages()
    classified = parse_kafka_messages(kafka_df(spark, messages))
    assert to_bronze(classified).count() == 0
    reasons = {r.kafka_key: r.reject_reason for r in to_rejected(classified).collect()}
    assert set(reasons) == {key for key, _ in messages}
    assert reasons["bad-json"] == "invalid_json"
    assert reasons["negative-bikes"] == "negative_availability"
    assert reasons["bad-latitude"] == "invalid_coordinates"
    assert reasons["bad-timestamp"] == "invalid_event_timestamp"
    assert all(reasons.values())


def test_valid_and_invalid_are_split_in_the_same_batch(spark):
    messages = [("ok", encode_event(_VALID_BASE)), ("bad", b"not json")]
    classified = parse_kafka_messages(kafka_df(spark, messages))
    assert to_bronze(classified).count() == 1
    rejected = to_rejected(classified).collect()
    assert [r.raw_value for r in rejected] == ["not json"]
