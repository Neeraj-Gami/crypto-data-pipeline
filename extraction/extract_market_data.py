"""
Pulls current crypto market data from the CoinGecko public API
and saves it locally as a JSON file (S3 upload comes in E3).
"""

import json
import os
from datetime import datetime, timezone

import requests

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

DEFAULT_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1,
    "sparkline": "false",
}


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


def main():
    print("Fetching market data from CoinGecko...")
    data = fetch_market_data()
    print(f"Fetched {len(data)} coins.")

    filepath = save_to_local_file(data)
    print(f"Saved to {filepath}")


if __name__ == "__main__":
    main()