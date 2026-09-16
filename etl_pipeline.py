"""
PySpark ETL Pipeline: Raw Sales Data -> Cleaned, Partitioned Parquet

Simulates a batch ETL job as it would run against a data lake:
  raw_zone/    -> equivalent to an S3 raw/ prefix (landing zone, untouched source data)
  processed_zone/ -> equivalent to an S3 processed/ prefix (cleaned, query-ready data)

This script runs against the local filesystem in this environment. To point it at
real AWS S3 instead of local paths, see the "Running against real S3" section in
the README — only the RAW_PATH / OUTPUT_PATH values and the Spark S3A config need
to change; the transformation logic is identical either way.

Pipeline stages:
  1. EXTRACT - read raw CSV with an explicit schema (no relying on inferSchema,
     since untyped ingestion from real sources is a common source of silent bugs)
  2. TRANSFORM - clean, deduplicate, validate, and enrich the data
  3. LOAD - write partitioned Parquet, plus a small aggregated summary table
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

RAW_PATH = os.path.join(os.path.dirname(__file__), "..", "raw_zone", "sales_raw.csv")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "processed_zone")


def get_spark():
    return (
        SparkSession.builder
        .appName("SalesETLPipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")  # allow to_date() to return null on
        .getOrCreate()                              # unparseable strings instead of throwing
    )


def extract(spark):
    """Read raw CSV with an explicit schema rather than inferSchema=True."""
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("region", StringType(), True),
        StructField("order_date", StringType(), True),
    ])
    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(RAW_PATH)
    )
    print(f"[EXTRACT] Read {df.count()} raw rows from {RAW_PATH}")
    return df


def transform(df):
    """Clean, validate, deduplicate, and enrich the raw data."""

    before = df.count()

    # 1. Drop exact duplicate rows (simulates upstream retry/resend duplicates)
    df = df.dropDuplicates()

    # 2. Standardize region casing (raw data has "south", "SOUTH", "South" etc.)
    df = df.withColumn("region", F.initcap(F.trim(F.col("region"))))

    # 3. Drop rows with missing region or missing/invalid quantity —
    #    logged separately rather than silently discarded, for data-quality tracking
    invalid_rows = df.filter(
        (F.col("region").isNull()) | (F.col("region") == "") |
        (F.col("quantity").isNull()) | (F.col("quantity") <= 0)
    )
    invalid_count = invalid_rows.count()

    df = df.filter(
        (F.col("region").isNotNull()) & (F.col("region") != "") &
        (F.col("quantity").isNotNull()) & (F.col("quantity") > 0)
    )

    # 4. Parse the three inconsistent date formats into one canonical date type
    df = df.withColumn(
        "order_date_parsed",
        F.coalesce(
            F.to_date("order_date", "yyyy-MM-dd"),
            F.to_date("order_date", "dd/MM/yyyy"),
            F.to_date("order_date", "dd-MM-yyyy"),
        )
    ).drop("order_date").withColumnRenamed("order_date_parsed", "order_date")

    # 5. Enrich: derived revenue column + partition keys (year, month)
    df = (
        df.withColumn("revenue", F.round(F.col("unit_price") * F.col("quantity"), 2))
          .withColumn("order_year", F.year("order_date"))
          .withColumn("order_month", F.month("order_date"))
    )

    after = df.count()
    print(f"[TRANSFORM] {before} raw rows -> {after} clean rows "
          f"({before - after} removed: duplicates + {invalid_count} invalid)")

    return df


def load(df, spark):
    """Write partitioned Parquet (query-ready) plus a small aggregated summary."""

    # Partitioned fact table — mirrors how a real data lake table would be laid out
    # for efficient downstream querying (e.g. by Athena/BigQuery/Spark SQL on year/month)
    (
        df.write
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .parquet(os.path.join(OUTPUT_PATH, "sales_fact"))
    )
    print(f"[LOAD] Wrote partitioned fact table -> {OUTPUT_PATH}/sales_fact")

    # A small aggregated summary table, the kind of output a BI tool would query directly
    summary = (
        df.groupBy("region", "order_year", "order_month")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("order_id").alias("total_orders"),
        )
        .orderBy("order_year", "order_month", "region")
    )
    (
        summary.write
        .mode("overwrite")
        .parquet(os.path.join(OUTPUT_PATH, "region_monthly_summary"))
    )
    print(f"[LOAD] Wrote aggregated summary table -> {OUTPUT_PATH}/region_monthly_summary")

    return summary


def run():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = extract(spark)
    clean_df = transform(raw_df)
    summary_df = load(clean_df, spark)

    print("\n[SUMMARY PREVIEW]")
    summary_df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    run()
