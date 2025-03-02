#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Polygon.io S3 Flat Files Connector
---------------------------------
This module provides utilities to access and download historical market data
from Polygon.io's Flat Files service via their S3 interface.
"""

import os
import logging
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from typing import List, Dict, Optional, Union, Tuple
from datetime import datetime, date
import gzip
import pandas as pd
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)


class PolygonS3Connector:
    """
    Connector for Polygon.io's Flat Files S3 service.

    This class provides methods to list available data and download specific
    files from Polygon.io's S3 bucket containing historical market data.
    """

    # Data categories available in Polygon Flat Files
    DATA_CATEGORIES = {
        "stocks": {
            "trades": "us_stocks_sip/trades_v1",
            "quotes": "us_stocks_sip/quotes_v1",
            "day_aggs": "us_stocks_sip/day_aggs_v1",
            "minute_aggs": "us_stocks_sip/minute_aggs_v1",
        },
        "options": {
            "trades": "us_options_opra/trades_v1",
            "quotes": "us_options_opra/quotes_v1",
            "day_aggs": "us_options_opra/day_aggs_v1",
            "minute_aggs": "us_options_opra/minute_aggs_v1",
        },
        "forex": {
            "quotes": "global_forex/quotes_v1",
            "day_aggs": "global_forex/day_aggs_v1",
            "minute_aggs": "global_forex/minute_aggs_v1",
        },
        "crypto": {
            "trades": "global_crypto/trades_v1",
            "quotes": "global_crypto/quotes_v1",
            "day_aggs": "global_crypto/day_aggs_v1",
            "minute_aggs": "global_crypto/minute_aggs_v1",
        },
        "indices": {
            "values": "us_indices/values_v1",
            "day_aggs": "us_indices/day_aggs_v1",
            "minute_aggs": "us_indices/minute_aggs_v1",
        },
    }

    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize the Polygon S3 connector.

        Args:
            env_file: Optional path to .env file with credentials. If None,
                     credentials are loaded from environment variables.
        """
        # Load environment variables if env_file is provided
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()

        # Get credentials from environment
        self.access_key = os.getenv("POLYGON_ACCESS_KEY_ID")
        self.secret_key = os.getenv("POLYGON_SECRET_ACCESS_KEY")
        self.endpoint_url = os.getenv("POLYGON_S3_ENDPOINT")
        self.bucket_name = os.getenv("BUCKET", "flatfiles")

        # Validate credentials
        if not self.access_key or not self.secret_key:
            raise ValueError(
                "Polygon credentials not found. Set POLYGON_ACCESS_KEY_ID and "
                "POLYGON_SECRET_ACCESS_KEY environment variables or provide an env_file."
            )

        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint_url,
        )

        logger.info(
            f"Polygon S3 Connector initialized with endpoint: {self.endpoint_url}"
        )

    def list_available_data(self) -> Dict:
        """
        List all available top-level directories in the S3 bucket.

        Returns:
            Dictionary of available data categories and their prefixes.
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Delimiter="/"
            )

            prefixes = [
                prefix.get("Prefix") for prefix in response.get("CommonPrefixes", [])
            ]
            return {"available_prefixes": prefixes}

        except ClientError as e:
            logger.error(f"Error listing available data: {e}")
            return {"error": str(e)}

    def list_files(self, prefix: str) -> List[Dict]:
        """
        List files in a specific prefix/directory.

        Args:
            prefix: S3 prefix/directory to list

        Returns:
            List of dictionaries containing file metadata
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )

            files = []
            for item in response.get("Contents", []):
                files.append(
                    {
                        "key": item.get("Key"),
                        "size": item.get("Size"),
                        "last_modified": item.get("LastModified"),
                    }
                )

            return files

        except ClientError as e:
            logger.error(f"Error listing files for prefix {prefix}: {e}")
            return []

    def list_data_by_category(
        self,
        asset_class: str,
        data_type: str,
        year: Optional[int] = None,
        month: Optional[int] = None,
    ) -> List[Dict]:
        """
        List available data files for a specific asset class and data type.

        Args:
            asset_class: Asset class (stocks, options, forex, crypto, indices)
            data_type: Type of data (trades, quotes, day_aggs, minute_aggs, values)
            year: Optional filter by year (YYYY)
            month: Optional filter by month (MM) - requires year to be specified

        Returns:
            List of available data files with metadata
        """
        if asset_class not in self.DATA_CATEGORIES:
            raise ValueError(
                f"Invalid asset class: {asset_class}. Valid options: {list(self.DATA_CATEGORIES.keys())}"
            )

        if data_type not in self.DATA_CATEGORIES[asset_class]:
            valid_types = list(self.DATA_CATEGORIES[asset_class].keys())
            raise ValueError(
                f"Invalid data type for {asset_class}: {data_type}. Valid options: {valid_types}"
            )

        # Construct the prefix
        prefix = f"{self.DATA_CATEGORIES[asset_class][data_type]}/"

        if year:
            prefix += f"{year}/"
            if month:
                # Ensure month is formatted with leading zero if needed
                prefix += f"{month:02d}/"

        return self.list_files(prefix)

    def download_file(
        self, s3_key: str, local_path: Union[str, Path], decompress: bool = True
    ) -> Path:
        """
        Download a specific file from S3.

        Args:
            s3_key: S3 object key (path to file in S3)
            local_path: Local path to save the file
            decompress: Whether to decompress .gz files automatically

        Returns:
            Path to the downloaded file
        """
        local_path = Path(local_path)

        # Create directory if it doesn't exist
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            logger.info(f"Downloading {s3_key} to {local_path}")
            self.s3_client.download_file(
                Bucket=self.bucket_name, Key=s3_key, Filename=str(local_path)
            )

            # Decompress if file is gzipped and decompress is True
            if decompress and s3_key.endswith(".gz"):
                decompressed_path = local_path.with_suffix("")
                with gzip.open(local_path, "rb") as f_in:
                    with open(decompressed_path, "wb") as f_out:
                        f_out.write(f_in.read())

                # Remove the compressed file
                local_path.unlink()
                return decompressed_path

            return local_path

        except ClientError as e:
            logger.error(f"Error downloading file {s3_key}: {e}")
            raise

    def download_data(
        self,
        asset_class: str,
        data_type: str,
        date_str: str,
        local_dir: Union[str, Path],
        decompress: bool = True,
    ) -> List[Path]:
        """
        Download data for a specific date.

        Args:
            asset_class: Asset class (stocks, options, forex, crypto, indices)
            data_type: Type of data (trades, quotes, day_aggs, minute_aggs, values)
            date_str: Date string in YYYY-MM-DD format
            local_dir: Local directory to save files
            decompress: Whether to decompress .gz files automatically

        Returns:
            List of paths to downloaded files
        """
        if asset_class not in self.DATA_CATEGORIES:
            raise ValueError(
                f"Invalid asset class: {asset_class}. Valid options: {list(self.DATA_CATEGORIES.keys())}"
            )

        if data_type not in self.DATA_CATEGORIES[asset_class]:
            valid_types = list(self.DATA_CATEGORIES[asset_class].keys())
            raise ValueError(
                f"Invalid data type for {asset_class}: {data_type}. Valid options: {valid_types}"
            )

        # Parse date
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Date should be in YYYY-MM-DD format")

        # Construct the prefix
        base_prefix = self.DATA_CATEGORIES[asset_class][data_type]
        prefix = f"{base_prefix}/{dt.year}/{dt.month:02d}/{date_str.replace('-', '')}"

        # List files matching the prefix
        files = self.list_files(prefix)

        if not files:
            logger.warning(f"No files found for {prefix}")
            return []

        # Download each file
        local_dir = Path(local_dir)
        downloaded_files = []

        for file_info in files:
            s3_key = file_info["key"]
            filename = Path(s3_key).name
            local_path = local_dir / filename

            try:
                downloaded_path = self.download_file(s3_key, local_path, decompress)
                downloaded_files.append(downloaded_path)
            except ClientError as e:
                logger.error(f"Failed to download {s3_key}: {e}")

        return downloaded_files

    def load_to_dataframe(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Load a CSV file into a Pandas DataFrame.

        Args:
            file_path: Path to the CSV file

        Returns:
            Pandas DataFrame containing the data
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Check if file is gzipped
        if file_path.suffix == ".gz":
            return pd.read_csv(file_path, compression="gzip")
        else:
            return pd.read_csv(file_path)

    def download_date_range(
        self,
        asset_class: str,
        data_type: str,
        start_date: Union[str, date],
        end_date: Union[str, date],
        local_dir: Union[str, Path],
        decompress: bool = True,
    ) -> Dict[str, List[Path]]:
        """
        Download data for a range of dates.

        Args:
            asset_class: Asset class (stocks, options, forex, crypto, indices)
            data_type: Type of data (trades, quotes, day_aggs, minute_aggs, values)
            start_date: Start date (YYYY-MM-DD string or date object)
            end_date: End date (YYYY-MM-DD string or date object)
            local_dir: Local directory to save files
            decompress: Whether to decompress .gz files automatically

        Returns:
            Dictionary mapping dates to lists of downloaded files
        """
        # Convert string dates to date objects if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        if start_date > end_date:
            raise ValueError("start_date must be before or equal to end_date")

        # Generate date range
        current_date = start_date
        all_files = {}

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            files = self.download_data(
                asset_class=asset_class,
                data_type=data_type,
                date_str=date_str,
                local_dir=local_dir,
                decompress=decompress,
            )

            all_files[date_str] = files

            # Move to next day
            current_date = date.fromordinal(current_date.toordinal() + 1)

        return all_files


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    # Initialize connector
    connector = PolygonS3Connector()

    # List available top-level directories
    available_data = connector.list_available_data()
    print("Available data categories:")
    print(available_data)

    # List stock trades for March 2024
    stock_trades = connector.list_data_by_category(
        asset_class="stocks", data_type="trades", year=2024, month=3
    )
    print(f"Found {len(stock_trades)} stock trade files for March 2024")

    # Download a specific file (example)
    if stock_trades:
        example_file = stock_trades[0]["key"]
        download_path = connector.download_file(
            s3_key=example_file, local_path=f"./data/storage/{Path(example_file).name}"
        )
        print(f"Downloaded file to {download_path}")
