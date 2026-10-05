"""
Pulls current crypto market data from the CoinGecko public API,
saves it locally, and uploads it to the S3 raw zone.
"""

import json
import os
from datetime import datetime, timezone

import boto3
import requests

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

DEFAULT_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1,
    "sparkline": "false",
}

S3_BUCKET = "crypto-pipeline-raw-data-neeraj"


def fetch_market_data(params: dict = None) -> list[dict]:
    """
    Calls the CoinGecko /coins/markets endpoint and returns the parsed
    response as a list of dicts (one dict per coin).
    Raises an exception if the request fails or the response is invalid.
    """
    query_params = params or DEFAULT_PARAMS

    try:
        response = requests.get(COINGECKO_URL, params=query_params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch data from CoinGecko: {e}") from e

    data = response.json()

    if not isinstance(data, list):
        raise ValueError(f"Unexpected response format from CoinGecko: {data}")

    return data


def save_to_local_file(data: list[dict], output_dir: str = "data/raw") -> str:
    """
    Saves the given data as a timestamped JSON file inside output_dir.
    Returns the path of the file that was written.
    """
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"market_data_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return filepath


def upload_to_s3(local_filepath: str, bucket: str = S3_BUCKET) -> str:
    """
    Uploads the given local file to S3 under a date-partitioned prefix:
    raw/YYYY/MM/DD/<filename>.
    Returns the S3 key the file was uploaded to.
    """
    now = datetime.now(timezone.utc)
    filename = os.path.basename(local_filepath)
    s3_key = f"raw/{now:%Y}/{now:%m}/{now:%d}/{filename}"

    s3_client = boto3.client("s3")

    try:
        s3_client.upload_file(local_filepath, bucket, s3_key)
    except Exception as e:
        raise RuntimeError(f"Failed to upload {local_filepath} to s3://{bucket}/{s3_key}: {e}") from e

    return s3_key


def main():
    print("Fetching market data from CoinGecko...")
    data = fetch_market_data()
    print(f"Fetched {len(data)} coins.")

    filepath = save_to_local_file(data)
    print(f"Saved locally to {filepath}")

    s3_key = upload_to_s3(filepath)
    print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")


if __name__ == "__main__":
    main()