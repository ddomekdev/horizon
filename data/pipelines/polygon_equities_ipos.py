from data.connectors.polygon_connector import PolygonConnector
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime, timedelta
import logging
import time
import requests


def update_ipo_history():
    connector = PolygonConnector()
    output_path = os.path.join(os.getenv("EQUITY_META_DATA"), "ipos.gzip")

    # Check if we have existing IPO data
    existing_ipos = None
    if os.path.exists(output_path):
        try:
            existing_ipos = pd.read_parquet(output_path)
        except Exception as e:
            print(e)
            existing_ipos = None

    # Create a function to make requests to the IPO endpoint
    def get_ipos(params=None):
        """
        Make a request to the IPOs endpoint with the given parameters.
        Returns the JSON response.
        """
        base_url = "https://api.polygon.io/vX/reference/ipos"

        # Get the API key from the connector
        api_key = connector.client.API_KEY

        # Initialize parameters if None
        if params is None:
            params = {}

        # Add API key to parameters
        params["apiKey"] = api_key

        # Make the request
        response = requests.get(base_url, params=params)

        # Check if request was successful
        if response.status_code == 200:
            return response.json()
        else:
            return None

    # Determine what to update
    if existing_ipos is not None and not existing_ipos.empty:
        # Find the latest modified date we have in our data
        if "modified_date" in existing_ipos.columns:
            latest_modified_date = pd.to_datetime(existing_ipos["modified_date"]).max()

            # Use a buffer of 7 days to ensure we don't miss any updates to IPO entries
            from_date = (latest_modified_date - timedelta(days=7)).strftime("%Y-%m-%d")

            # Process IPO data in batches to handle rate limits
            new_ipo_records = []

            try:
                # Get all IPOs modified since our last update
                params = {
                    "modified_date.gte": from_date,
                    "sort": "modified_date",
                    "order": "asc",
                    "limit": 1000,
                }

                pagination_cursor = None
                page_count = 0
                max_pages = 100  # Safety limit

                while page_count < max_pages:
                    page_count += 1

                    # If we have a pagination cursor, use it
                    if pagination_cursor:
                        params["cursor"] = pagination_cursor

                    # Make the request
                    response = get_ipos(params)

                    # Process the IPO records
                    if response and "results" in response and response["results"]:
                        for ipo in response["results"]:
                            new_ipo_records.append(ipo)

                    else:
                        break

                    # Check if we have a next_url for pagination
                    if "next_url" in response and response["next_url"]:
                        # Extract the cursor from the next_url
                        if "cursor=" in response["next_url"]:
                            pagination_cursor = (
                                response["next_url"].split("cursor=")[1].split("&")[0]
                                if "&" in response["next_url"].split("cursor=")[1]
                                else response["next_url"].split("cursor=")[1]
                            )
                            # Add a small delay to avoid hitting rate limits
                            time.sleep(0.5)
                        else:
                            break
                    else:
                        # No more pages to fetch
                        break

            except Exception as e:
                print(e)

            # Convert new records to DataFrame
            if new_ipo_records:
                new_records_df = pd.DataFrame(new_ipo_records)

                # Ensure date fields are in datetime format for comparison
                date_columns = ["modified_date", "listing_date", "announcement_date"]
                for col in date_columns:
                    if col in new_records_df.columns:
                        new_records_df[col] = pd.to_datetime(new_records_df[col])
                    if col in existing_ipos.columns:
                        existing_ipos[col] = pd.to_datetime(existing_ipos[col])

                # Remove existing entries that are being updated
                if (
                    "ticker" in new_records_df.columns
                    and "ticker" in existing_ipos.columns
                ):
                    updated_tickers = new_records_df["ticker"].unique()
                    existing_filtered = existing_ipos[
                        ~existing_ipos["ticker"].isin(updated_tickers)
                    ]

                    # Merge with new/updated data
                    combined_df = pd.concat([existing_filtered, new_records_df])

                    # Save the updated dataset
                    combined_df.to_parquet(path=output_path, index=False)

                    return combined_df
                else:
                    return existing_ipos
            else:
                return existing_ipos
        else:
            existing_ipos = None

    # No existing data or invalid existing data, collect all IPO data
    if existing_ipos is None:
        # Initial import - fetch all historical IPOs
        ipo_history = []

        try:
            # Get all pages of IPO results
            params = {"sort": "listing_date", "order": "asc", "limit": 1000}

            pagination_cursor = None
            page_count = 0
            max_pages = 100  # Safety limit

            while page_count < max_pages:
                page_count += 1

                # If we have a pagination cursor, use it
                if pagination_cursor:
                    params["cursor"] = pagination_cursor

                # Make the request
                response = get_ipos(params)

                # Process the IPO records
                if response and "results" in response and response["results"]:
                    for ipo in response["results"]:
                        ipo_history.append(ipo)

                else:
                    break

                # Check if we have a next_url for pagination
                if "next_url" in response and response["next_url"]:
                    # Extract the cursor from the next_url
                    if "cursor=" in response["next_url"]:
                        pagination_cursor = (
                            response["next_url"].split("cursor=")[1].split("&")[0]
                            if "&" in response["next_url"].split("cursor=")[1]
                            else response["next_url"].split("cursor=")[1]
                        )

                        # Add a small delay to avoid hitting rate limits
                        time.sleep(0.5)
                    else:
                        break
                else:
                    # No more pages to fetch
                    break

        except Exception as e:
            raise e

        # Create DataFrame and save
        ipo_history_df = pd.DataFrame(ipo_history)

        if not ipo_history_df.empty:
            # Convert date columns to datetime format
            date_columns = ["modified_date", "listing_date", "announcement_date"]
            for col in date_columns:
                if col in ipo_history_df.columns:
                    ipo_history_df[col] = pd.to_datetime(ipo_history_df[col])

            # Save the DataFrame
            ipo_history_df.to_parquet(path=output_path, index=False)

        return ipo_history_df


if __name__ == "__main__":
    update_ipo_history()
