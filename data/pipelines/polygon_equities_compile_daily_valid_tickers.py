import pandas as pd
from glob import glob
from os import getenv
import os
from tqdm import tqdm

if __name__ == "__main__":
    all_tickers_path = os.path.join(
        getenv("EQUITY_META_DATA"), "all_daily_tickers.gzip"
    )

    if all_tickers_path in glob(os.path.join(getenv("EQUITY_META_DATA"), "*")):
        all_daily_tickers = pd.read_parquet(all_tickers_path)
    else:
        all_daily_tickers = pd.DataFrame(columns=["tickers"])

    all_daily_files = glob(os.path.join(getenv("EQUITY_DAILY_DATA_PATH"), "*"))

    for file_name in tqdm(all_daily_files):
        date = os.path.split(file_name)[-1].split(".")[0]
        if date not in all_daily_tickers.index:
            contents = pd.read_parquet(file_name)
            uticks = (
                contents["ticker"].drop_duplicates().sort_values().dropna().tolist()
            )
            uticks_str = ",".join(uticks)
            all_daily_tickers.loc[date] = [uticks_str]
    all_daily_tickers.to_parquet(all_tickers_path)
