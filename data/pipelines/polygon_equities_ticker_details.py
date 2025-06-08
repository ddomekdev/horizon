from data.connectors.polygon_connector import PolygonConnector
import pandas as pd
from tqdm import tqdm
import os

connector = PolygonConnector()

if __name__ == "__main__":
    # fetch all tickers
    all_tickers = list()
    for tick in tqdm(connector.client.list_tickers(market="stocks")):
        all_tickers.append(tick.__dict__)
    all_tickers = pd.DataFrame.from_records(all_tickers)
    all_tickers = all_tickers.sort_values("ticker")

    # fully filled in ticker records
    ins = all_tickers.dropna()
    ins = ins.sort_values("last_updated_utc").drop_duplicates(
        subset=all_tickers.columns[:-1], keep="last"
    )

    # reamining records (with null)
    remaining = all_tickers[all_tickers.isna().any(axis=1)]
    full_label = ins["ticker"] + "|" + ins["name"]
    remaining_label = remaining["ticker"] + "|" + remaining["name"]
    # get rid of combinations we are already familiar with
    remaining = remaining[~remaining_label.isin(full_label)]
    finale = pd.concat([ins, remaining])

    save_path = os.path.join(
        os.getenv("EQUITY_META_DATA"), "all_ticker_meta.csv"
    ).replace("\\", "/")
    finale.to_csv(save_path, index=False)
