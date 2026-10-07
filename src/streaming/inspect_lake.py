"""Read-only inspection of the Bronze and rejected Parquet datasets.

Run inside the Spark image: ``python -m streaming.inspect_lake`` (see README).
"""

from __future__ import annotations

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.config import StreamingSettings
from streaming.silver import BUSINESS_KEY, KAFKA_COORDINATES


def main() -> None:
    settings = StreamingSettings.from_env()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("inspect-lake")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    bronze = spark.read.parquet(settings.bronze_path)
    print("== BRONZE schema")
    bronze.printSchema()
    total = bronze.count()
    distinct = bronze.select("kafka_topic", "kafka_partition", "kafka_offset").distinct().count()
    print(f"== BRONZE rows={total} distinct(topic,partition,offset)={distinct}")
    print(f"== BRONZE duplicated rows (same Kafka coordinates): {total - distinct}")
    print("== BRONZE per Kafka partition (rows, min/max offset)")
    (
        bronze.groupBy("kafka_partition")
        .agg(
            F.count("*").alias("rows"),
            F.min("kafka_offset").alias("min_offset"),
            F.max("kafka_offset").alias("max_offset"),
        )
        .orderBy("kafka_partition")
        .show()
    )
    print("== BRONZE files per ingestion_date")
    (
        bronze.withColumn("source_file", F.input_file_name())
        .groupBy("ingestion_date")
        .agg(F.countDistinct("source_file").alias("files"), F.count("*").alias("rows"))
        .orderBy("ingestion_date")
        .show()
    )

    rejected = spark.read.parquet(settings.rejected_path)
    print("== REJECTED schema")
    rejected.printSchema()
    print(f"== REJECTED rows={rejected.count()}")
    (
        rejected.select(
            "kafka_key", "reject_reason", "kafka_partition", "kafka_offset", "raw_value"
        )
        .orderBy("kafka_key")
        .show(truncate=40)
    )
    inspect_silver(spark, settings, bronze)
    spark.stop()


def inspect_silver(spark: SparkSession, settings: StreamingSettings, bronze) -> None:
    try:
        silver = spark.read.parquet(settings.silver_path)
    except AnalysisException:
        print("== SILVER: no data yet")
        return
    print("== SILVER schema")
    silver.printSchema()
    key = BUSINESS_KEY
    silver_rows = silver.count()
    # Silver can lag Bronze by about one trigger (and by unprocessed files): a small gap is normal.
    comparable = bronze
    print(f"== SILVER rows={silver_rows} (Bronze rows: {comparable.count()})")
    silver_keys = silver.select(*key).distinct().count()
    bronze_keys = comparable.select(*key).distinct().count()
    distinct_kafka = silver.select(*KAFKA_COORDINATES).distinct().count()
    print(f"== SILVER distinct business keys={silver_keys}")
    print(f"== BRONZE distinct business keys ={bronze_keys}")
    print(f"== SILVER duplicated Kafka coordinates: {silver_rows - distinct_kafka}")
    print("== SILVER derived columns")
    silver.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("fill_rate").isNull().cast("int")).alias("null_fill_rate"),
        F.sum(F.col("is_empty").cast("int")).alias("empty"),
        F.sum(F.col("is_full").cast("int")).alias("full"),
        F.sum((~F.col("is_operational")).cast("int")).alias("not_operational"),
        F.min("fill_rate").alias("min_fill"),
        F.max("fill_rate").alias("max_fill"),
    ).show()
    print("== SILVER per ingestion_date")
    (
        silver.withColumn("source_file", F.input_file_name())
        .groupBy("ingestion_date")
        .agg(F.countDistinct("source_file").alias("files"), F.count("*").alias("rows"))
        .orderBy("ingestion_date")
        .show()
    )


if __name__ == "__main__":
    main()
