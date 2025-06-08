from data.connectors.polygon_connector import PolygonConnector
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime, timedelta
import logging
import time


def update_ticker_events():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)

    connector = PolygonConnector()
    output_path = os.path.join(os.getenv("EQUITY_META_DATA"), "ticker_events.gzip")

    # Load all tickers with their FIGIs
    all_tickers = pd.read_csv(
        os.path.join(os.getenv("EQUITY_META_DATA"), "all_ticker_meta.csv")
    )

    # Get valid FIGIs
    figis = all_tickers["composite_figi"].dropna().drop_duplicates().tolist()
    logger.info(f"Found {len(figis)} unique FIGIs to process")

    # Check if we have existing ticker events data
    existing_events = None
    if os.path.exists(output_path):
        try:
            existing_events = pd.read_parquet(output_path)
            logger.info(
                f"Loaded existing ticker events data with {len(existing_events)} records"
            )
        except Exception as e:
            logger.error(f"Error loading existing ticker events data: {e}")
            existing_events = None

    # Determine what to update
    if existing_events is not None and not existing_events.empty:
        # Find the latest date we have in our data
        latest_event_date = pd.to_datetime(existing_events["date"]).max()

        # Use a buffer of 7 days to ensure we don't miss any events due to data delays
        from_date = (latest_event_date - timedelta(days=7)).strftime("%Y-%m-%d")

        logger.info(f"Updating ticker events data from {from_date} onwards")

        # Get FIGIs we already have data for
        processed_figis = set(existing_events["composite_figi"].dropna().unique())

        # Process all FIGIs
        event_history = []

        # Split FIGIs into batches to avoid rate limiting
        batch_size = 100
        figi_batches = [
            figis[i : i + batch_size] for i in range(0, len(figis), batch_size)
        ]

        for batch in tqdm(figi_batches, desc="Processing FIGI batches"):
            for figi in tqdm(batch, desc="Updating ticker events", leave=False):
                try:
                    # Get ticker events for this FIGI
                    events = connector.client.get_ticker_events(ticker=figi)

                    if events and hasattr(events, "events") and events.events:
                        # Process each event
                        collection = []
                        for row in events.events:
                            # Check if this is a new event
                            event_date = pd.to_datetime(row["date"])
                            if (
                                event_date >= pd.to_datetime(from_date)
                                or figi not in processed_figis
                            ):
                                collection_dict = dict()
                                collection_dict["name"] = events.name
                                collection_dict["composite_figi"] = (
                                    events.composite_figi
                                )
                                collection_dict["cik"] = getattr(events, "cik", None)
                                collection_dict["new_ticker"] = row["ticker_change"][
                                    "ticker"
                                ]
                                collection_dict["type"] = row["type"]
                                collection_dict["date"] = row["date"]
                                collection.append(collection_dict)

                        # Add to our event history
                        event_history.extend(collection)
                except Exception as e:
                    logger.error(f"Error fetching events for FIGI {figi}: {e}")

            # Add a small delay between batches to avoid rate limiting
            time.sleep(1)

        # Convert new records to DataFrame
        if event_history:
            new_records_df = pd.DataFrame(event_history)
            logger.info(f"Found {len(new_records_df)} new ticker event records")

            # Convert date to datetime for comparison
            new_records_df["date"] = pd.to_datetime(new_records_df["date"])
            existing_events["date"] = pd.to_datetime(existing_events["date"])

            # Remove any events in our new data that already exist in our old data
            combined_df = pd.concat([existing_events, new_records_df])

            # Remove duplicates
            combined_df = combined_df.drop_duplicates(
                subset=["composite_figi", "type", "date", "new_ticker"]
            )

            # Save the updated dataset
            combined_df.to_parquet(path=output_path, index=False)
            logger.info(
                f"Saved updated ticker events data with {len(combined_df)} total records"
            )

            return combined_df
        else:
            logger.info("No new ticker event records found")
            return existing_events
    else:
        # No existing data, collect all ticker events
        logger.info(
            "No existing ticker events data found. Collecting all ticker events history."
        )

        # Process all FIGIs
        event_history = []

        # Split FIGIs into batches to avoid rate limiting
        batch_size = 100
        figi_batches = [
            figis[i : i + batch_size] for i in range(0, len(figis), batch_size)
        ]

        for batch in tqdm(figi_batches, desc="Processing FIGI batches"):
            for figi in tqdm(batch, desc="Getting all ticker events", leave=False):
                try:
                    # Get ticker events for this FIGI
                    events = connector.client.get_ticker_events(ticker=figi)

                    if events and hasattr(events, "events") and events.events:
                        # Process each event
                        collection = []
                        for row in events.events:
                            collection_dict = dict()
                            collection_dict["name"] = events.name
                            collection_dict["composite_figi"] = events.composite_figi
                            collection_dict["cik"] = getattr(events, "cik", None)
                            collection_dict["new_ticker"] = row["ticker_change"][
                                "ticker"
                            ]
                            collection_dict["type"] = row["type"]
                            collection_dict["date"] = row["date"]
                            collection.append(collection_dict)

                        # Add to our event history
                        event_history.extend(collection)
                except Exception as e:
                    logger.error(f"Error fetching events for FIGI {figi}: {e}")

            # Add a small delay between batches to avoid rate limiting
            time.sleep(1)

        # Create DataFrame from collected events
        event_history_df = pd.DataFrame(event_history)

        # Save the data if we have any events
        if not event_history_df.empty:
            # Convert date to datetime
            event_history_df["date"] = pd.to_datetime(event_history_df["date"])

            # Remove duplicates
            event_history_df = event_history_df.drop_duplicates(
                subset=["composite_figi", "type", "date", "new_ticker"]
            )

            # Save the DataFrame
            event_history_df.to_parquet(path=output_path, index=False)
            logger.info(
                f"Saved initial ticker events data with {len(event_history_df)} records"
            )
        else:
            logger.warning("No ticker events data collected")

        return event_history_df


if __name__ == "__main__":
    update_ticker_events()
