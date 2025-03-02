#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Download Historical Market Data
------------------------------
Example script for downloading historical market data from Polygon.io Flat Files
using the S3 integration.
"""

import argparse
import logging
from pathlib import Path
from datetime import datetime

# Import the PolygonS3Connector - adjust the import path as needed for your project structure
import sys

sys.path.append(str(Path(__file__).parent.parent))
from data.connectors.polygon_s3_connector import PolygonS3Connector


def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("download_history.log")],
    )
    return logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download historical market data from Polygon.io Flat Files."
    )

    parser.add_argument(
        "--asset-class",
        type=str,
        required=True,
        choices=["stocks", "options", "forex", "crypto", "indices"],
        help="Asset class to download data for",
    )

    parser.add_argument(
        "--data-type",
        type=str,
        required=True,
        help="Type of data to download (e.g., trades, quotes, day_aggs, minute_aggs)",
    )

    parser.add_argument(
        "--start-date", type=str, required=True, help="Start date in YYYY-MM-DD format"
    )

    parser.add_argument(
        "--end-date", type=str, required=True, help="End date in YYYY-MM-DD format"
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data/storage",
        help="Directory to save downloaded files",
    )

    parser.add_argument(
        "--keep-compressed",
        action="store_true",
        help="Keep compressed .gz files instead of decompressing",
    )

    parser.add_argument(
        "--env-file", type=str, default=None, help="Path to .env file with credentials"
    )

    return parser.parse_args()


def validate_dates(start_date, end_date):
    """Validate that the date strings are in the correct format and logically valid."""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        if start > end:
            raise ValueError("Start date must be before or equal to end date")

        return True
    except ValueError as e:
        raise ValueError(f"Date validation error: {e}")


def main():
    """Main function to handle downloading historical market data."""
    # Set up logging
    logger = setup_logging()
    logger.info("Starting historical data download")

    # Parse arguments
    args = parse_arguments()

    try:
        # Validate dates
        validate_dates(args.start_date, args.end_date)

        # Create output directory if it doesn't exist
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create asset class and data type specific subdirectory
        specific_dir = output_dir / args.asset_class / args.data_type
        specific_dir.mkdir(parents=True, exist_ok=True)

        # Initialize the connector
        connector = PolygonS3Connector(env_file=args.env_file)

        # Download data for the date range
        logger.info(
            f"Downloading {args.asset_class} {args.data_type} data from {args.start_date} to {args.end_date}"
        )

        downloaded_files = connector.download_date_range(
            asset_class=args.asset_class,
            data_type=args.data_type,
            start_date=args.start_date,
            end_date=args.end_date,
            local_dir=specific_dir,
            decompress=not args.keep_compressed,
        )

        # Log summary
        total_files = sum(len(files) for files in downloaded_files.values())
        logger.info(
            f"Download complete. Downloaded {total_files} files across {len(downloaded_files)} dates."
        )

        # Print summary for each date
        for date_str, files in downloaded_files.items():
            if files:
                logger.info(f"  {date_str}: Downloaded {len(files)} files")
            else:
                logger.warning(f"  {date_str}: No files found")

    except Exception as e:
        logger.error(f"Error downloading data: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    main()
