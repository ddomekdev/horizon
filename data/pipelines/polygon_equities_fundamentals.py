from data.connectors.polygon_connector import PolygonConnector
import pandas as pd
from tqdm import tqdm
import os
import time
from dataclasses import asdict


def update_fundamental_history():
    connector = PolygonConnector()
    output_path = os.path.join(os.getenv("EQUITY_META_DATA"), "fundamentals.gzip")

    # Load tickers
    tickers = pd.read_csv(
        os.path.join(os.getenv("EQUITY_META_DATA"), "all_ticker_meta.csv")
    )
    ticker_list = tickers["ticker"].drop_duplicates().dropna().tolist()

    # Load existing fundamentals if present
    existing_fundamentals = None
    if os.path.exists(output_path):
        try:
            existing_fundamentals = pd.read_parquet(output_path)
        except Exception:
            existing_fundamentals = None

    processed = set()
    if (
        existing_fundamentals is not None
        and not existing_fundamentals.empty
        and "ticker" in existing_fundamentals.columns
    ):
        processed = set(existing_fundamentals["ticker"].dropna().unique())

    fundamental_records = []
    for ticker in tqdm(ticker_list, desc="Fetching fundamentals"):
        if ticker in processed:
            continue
        try:
            for fin in connector.client.vx.list_stock_financials(ticker=ticker, limit=1000):
                fundamental_records.append({"ticker": ticker, **asdict(fin)})
        except Exception as e:
            print(e)
        time.sleep(0.2)

    if fundamental_records:
        new_df = pd.DataFrame(fundamental_records)
        if existing_fundamentals is not None:
            combined_df = pd.concat([existing_fundamentals, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=["ticker", "filing_date", "fiscal_year", "fiscal_period"]
            )
        else:
            combined_df = new_df
        combined_df.to_parquet(path=output_path, index=False)
        return combined_df
    else:
        return existing_fundamentals


if __name__ == "__main__":
    update_fundamental_history()
