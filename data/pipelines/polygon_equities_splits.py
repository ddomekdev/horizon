from data.connectors.polygon_connector import PolygonConnector
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime, timedelta
import logging
import time


def update_split_history():
    connector = PolygonConnector()
    output_path = os.path.join(os.getenv("EQUITY_META_DATA"), "splits.gzip")

    # Load all tickers
    all_tickers = pd.read_csv(
        os.path.join(os.getenv("EQUITY_META_DATA"), "all_ticker_meta.csv")
    )
    all_tickers_list = all_tickers["ticker"].drop_duplicates().tolist()

    # Check if we have existing split data
    existing_splits = None
    if os.path.exists(output_path):
        try:
            existing_splits = pd.read_parquet(output_path)

        except Exception as e:
            print(e)
            existing_splits = None

    # Determine what to update
    if existing_splits is not None and not existing_splits.empty:
        # Find the latest execution_date we have in our data
        latest_split_date = pd.to_datetime(existing_splits["execution_date"]).max()

        # Use a buffer of 7 days to ensure we don't miss any splits due to data delays
        from_date = (latest_split_date - timedelta(days=7)).strftime("%Y-%m-%d")

        # Process all tickers but only fetch newer split records
        new_split_records = []

        # Split tickers into batches to avoid rate limiting
        batch_size = 100
        ticker_batches = [
            all_tickers_list[i : i + batch_size]
            for i in range(0, len(all_tickers_list), batch_size)
        ]

        for batch in tqdm(ticker_batches, desc="Processing ticker batches"):
            for ticker in tqdm(batch, desc="Updating split history", leave=False):
                try:
                    # Use the execution_date_gte parameter to get splits after our latest date
                    # Sort by execution_date in ascending order to get oldest first
                    for split in connector.client.list_splits(
                        ticker=ticker,
                        execution_date_gte=from_date,
                        sort="execution_date",
                        order="asc",
                        limit=1000,  # Maximum allowed by API
                    ):
                        if split:
                            new_split_records.append(split.__dict__)
                except Exception as e:
                    print(e)

            # Add a small delay between batches to avoid rate limiting
            time.sleep(1)

        # Convert new records to DataFrame
        if new_split_records:
            new_records_df = pd.DataFrame(new_split_records)

            # Calculate the ratio for new records
            new_records_df["ratio"] = (
                new_records_df["split_to"] / new_records_df["split_from"]
            )

            # Drop any duplicate records before merging
            # First ensure the execution_date is in datetime format for comparison
            if "execution_date" in new_records_df.columns:
                new_records_df["execution_date"] = pd.to_datetime(
                    new_records_df["execution_date"]
                )

            # Do the same for the existing data
            existing_splits["execution_date"] = pd.to_datetime(
                existing_splits["execution_date"]
            )

            # Merge with existing data
            combined_df = pd.concat([existing_splits, new_records_df])

            # Remove duplicates based on ticker and execution_date, and split ratio values
            combined_df = combined_df.drop_duplicates(
                subset=["ticker", "execution_date", "split_from", "split_to"]
            )

            # Save the updated dataset
            combined_df.to_parquet(path=output_path, index=False)

            return combined_df
        else:
            return existing_splits
    else:
        # Initial import - we'll fetch all historical splits
        split_history = []

        # Split tickers into batches
        batch_size = 100
        ticker_batches = [
            all_tickers_list[i : i + batch_size]
            for i in range(0, len(all_tickers_list), batch_size)
        ]

        for batch in tqdm(ticker_batches, desc="Processing ticker batches"):
            for ticker in tqdm(batch, desc="Getting all split history", leave=False):
                try:
                    # We need to fetch all split history for initial import
                    for split in connector.client.list_splits(
                        ticker=ticker,
                        limit=1000,  # Maximum allowed by API
                        sort="execution_date",
                        order="asc",
                    ):
                        if split:
                            split_history.append(split.__dict__)
                except Exception as e:
                    print(e)

            # Add a small delay between batches to avoid rate limiting
            time.sleep(1)

        # Create DataFrame and save
        split_history_df = pd.DataFrame(split_history)

        if not split_history_df.empty:
            # Calculate the split ratio
            split_history_df["ratio"] = (
                split_history_df["split_to"] / split_history_df["split_from"]
            )

            split_history_df.to_parquet(path=output_path, index=False)

        return split_history_df


if __name__ == "__main__":
    update_split_history()
