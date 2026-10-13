"""
Finds the most recent raw market data file in S3, downloads it locally,
cleans and transforms it with PySpark, and writes the result locally
as Parquet (S3 upload of the processed output comes in a later story).
"""

import os

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

S3_BUCKET = "crypto-pipeline-raw-data-neeraj"
S3_RAW_PREFIX = "raw/"

LOCAL_DOWNLOAD_DIR = "data/raw_download"
LOCAL_OUTPUT_DIR = "data/processed"


def get_latest_raw_key(bucket: str = S3_BUCKET, prefix: str = S3_RAW_PREFIX) -> str:
    """
    Lists all objects under the given S3 prefix and returns the key
    of the most recently modified one.
    """
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")

    latest_key = None
    latest_modified = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest_modified is None or obj["LastModified"] > latest_modified:
                latest_modified = obj["LastModified"]
                latest_key = obj["Key"]

    if latest_key is None:
        raise RuntimeError(f"No objects found under s3://{bucket}/{prefix}")

    return latest_key


def download_file(key: str, bucket: str = S3_BUCKET, local_dir: str = LOCAL_DOWNLOAD_DIR) -> str:
    """
    Downloads the given S3 object to local_dir, preserving just the filename.
    Returns the local file path.
    """
    os.makedirs(local_dir, exist_ok=True)
    filename = os.path.basename(key)
    local_path = os.path.join(local_dir, filename)

    s3_client = boto3.client("s3")
    s3_client.download_file(bucket, key, local_path)

    return local_path


def transform(spark: SparkSession, local_json_path: str):
    """
    Reads the raw JSON, cleans it, and produces two DataFrames:
      - cleaned: one row per coin, relevant columns, correct types, no duplicates/nulls in key fields
      - summary: a small aggregated view across all coins
    """
    raw_df = spark.read.option("multiLine", True).json(local_json_path)

    cleaned = (
        raw_df.select(
            F.col("id").alias("coin_id"),
            F.col("symbol"),
            F.col("name"),
            F.col("current_price").cast(DoubleType()),
            F.col("market_cap").cast(DoubleType()),
            F.col("market_cap_rank").cast(IntegerType()),
            F.col("total_volume").cast(DoubleType()),
            F.col("price_change_percentage_24h").cast(DoubleType()),
        )
        # Drop rows missing essential fields - can't meaningfully analyze a coin with no id or price
        .dropna(subset=["coin_id", "current_price", "market_cap"])
        # Guard against any duplicate coin entries in a single pull
        .dropDuplicates(["coin_id"])
    )

    summary = cleaned.agg(
        F.count("coin_id").alias("num_coins"),
        F.sum("market_cap").alias("total_market_cap"),
        F.avg("price_change_percentage_24h").alias("avg_24h_price_change_pct"),
        F.max("current_price").alias("highest_price"),
        F.min("current_price").alias("lowest_price"),
    )

    return cleaned, summary


def write_outputs(cleaned, summary, output_dir: str = LOCAL_OUTPUT_DIR):
    """
    Writes both DataFrames to local Parquet files.
    The results are small, so we convert to pandas and write with pyarrow.
    This avoids Hadoop's native Windows file layer, which needs a
    hadoop.dll matching the exact Hadoop version PySpark bundles.
    """
    os.makedirs(output_dir, exist_ok=True)

    cleaned.toPandas().to_parquet(
        os.path.join(output_dir, "cleaned_coins.parquet"), index=False
    )
    summary.toPandas().to_parquet(
        os.path.join(output_dir, "daily_summary.parquet"), index=False
    )

def main():
    print("Finding latest raw file in S3...")
    latest_key = get_latest_raw_key()
    print(f"Latest file: s3://{S3_BUCKET}/{latest_key}")

    local_path = download_file(latest_key)
    print(f"Downloaded to {local_path}")

    spark = SparkSession.builder.appName("CryptoMarketDataTransform").getOrCreate()

    cleaned, summary = transform(spark, local_path)

    print("Cleaned data sample:")
    cleaned.show(10)

    print("Summary:")
    summary.show()

    write_outputs(cleaned, summary)
    print(f"Wrote outputs to {LOCAL_OUTPUT_DIR}/")

    spark.stop()


if __name__ == "__main__":
    main()