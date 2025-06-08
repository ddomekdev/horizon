from data.connectors.polygon_connector import PolygonConnector
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime, timedelta
import logging


def update_dividend_history():
    connector = PolygonConnector()
    output_path = os.path.join(os.getenv("EQUITY_META_DATA"), "dividends.gzip")

    # Load all tickers
    all_tickers = pd.read_csv(
        os.path.join(os.getenv("EQUITY_META_DATA"), "all_ticker_meta.csv")
    )
    all_tickers_list = all_tickers["ticker"].drop_duplicates().tolist()

    # Check if we have existing dividend data
    existing_dividends = None
    if os.path.exists(output_path):
        try:
            existing_dividends = pd.read_parquet(output_path)

        except Exception as e:
            existing_dividends = None

    # Determine what to update
    if existing_dividends is not None and not existing_dividends.empty:
        # Find the latest ex_dividend_date we have in our data
        latest_ex_div_date = pd.to_datetime(
            existing_dividends["ex_dividend_date"]
        ).max()

        # Use a buffer of 7 days to ensure we don't miss any dividends due to data delays
        from_date = (latest_ex_div_date - timedelta(days=7)).strftime("%Y-%m-%d")

        # Process all tickers but only fetch newer dividend records
        new_dividend_records = []

        # Split tickers into batches to avoid rate limiting
        batch_size = 100
        ticker_batches = [
            all_tickers_list[i : i + batch_size]
            for i in range(0, len(all_tickers_list), batch_size)
        ]

        for batch in tqdm(ticker_batches, desc="Processing ticker batches"):
            for ticker in tqdm(batch, desc="Updating dividend history", leave=False):
                try:
                    # Use the ex_dividend_date_gte parameter to get dividends after our latest date
                    # Sort by ex_dividend_date in ascending order to get oldest first
                    for div in connector.client.list_dividends(
                        ticker=ticker,
                        ex_dividend_date_gte=from_date,
                        sort="ex_dividend_date",
                        order="asc",
                        limit=1000,  # Maximum allowed by API
                    ):
                        if div:
                            new_dividend_records.append(div.__dict__)
                except Exception as e:
                    print(e)

            # Add a small delay between batches to avoid rate limiting
            time.sleep(1)

        # Convert new records to DataFrame
        if new_dividend_records:
            new_records_df = pd.DataFrame(new_dividend_records)

            # Drop any duplicate records before merging
            # First ensure the ex_dividend_date is in datetime format for comparison
            if "ex_dividend_date" in new_records_df.columns:
                new_records_df["ex_dividend_date"] = pd.to_datetime(
                    new_records_df["ex_dividend_date"]
                )

            # Do the same for the existing data
            existing_dividends["ex_dividend_date"] = pd.to_datetime(
                existing_dividends["ex_dividend_date"]
            )

            # Merge with existing data
            combined_df = pd.concat([existing_dividends, new_records_df])

            # Remove duplicates based on ticker and ex_dividend_date
            combined_df = combined_df.drop_duplicates(
                subset=["ticker", "ex_dividend_date", "cash_amount"]
            )

            # Save the updated dataset
            combined_df.to_parquet(path=output_path, index=False)

            return combined_df
        else:
            return existing_dividends
    else:
        # Initial import - we'll fetch all historical dividends
        dividend_history = []

        # Split tickers into batches
        batch_size = 100
        ticker_batches = [
            all_tickers_list[i : i + batch_size]
            for i in range(0, len(all_tickers_list), batch_size)
        ]

        for batch in tqdm(ticker_batches, desc="Processing ticker batches"):
            for ticker in tqdm(batch, desc="Getting all dividend history", leave=False):
                try:
                    # We need to fetch all dividend history for initial import
                    for div in connector.client.list_dividends(
                        ticker=ticker,
                        limit=1000,  # Maximum allowed by API
                        sort="ex_dividend_date",
                        order="asc",
                    ):
                        if div:
                            dividend_history.append(div.__dict__)
                except Exception as e:
                    print(e)

            # Add a small delay between batches to avoid rate limiting
            time.sleep(1)

        # Create DataFrame and save
        dividend_history_df = pd.DataFrame(dividend_history)

        if not dividend_history_df.empty:
            dividend_history_df.to_parquet(path=output_path, index=False)

        return dividend_history_df


if __name__ == "__main__":
    import time

    update_dividend_history()
