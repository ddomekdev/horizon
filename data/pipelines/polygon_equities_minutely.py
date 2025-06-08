#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Polygon.io Daily Aggregates ETL Pipeline
--------------------------------------
This script implements an ETL pipeline to download all available daily aggregates
data for stocks from Polygon.io's Flat Files service.
"""

import os
from data.connectors.polygon_s3_connector import PolygonS3Connector
import datetime
from pathlib import Path
from tqdm import tqdm
import pandas as pd

if __name__ == "__main__":
    conn = PolygonS3Connector()
    today_date = datetime.date.today()
    max_allowed_date = today_date - datetime.timedelta(days=365 * 10)

    print("Getting all available files")
    # get all files available
    all_files = list()
    for year in range(max_allowed_date.year, today_date.year + 1):
        for month in range(1, 13):
            daily_files = conn.list_data_by_category(
                asset_class="stocks", data_type="minute_aggs", year=year, month=month
            )
            all_files += daily_files

    print("Filter out dates that cannot be accessed with current subscription")
    # filter out dates I'm not allowed to download
    for x in range(len(all_files) - 1, -1, -1):
        s3_key = all_files[x]["key"]
        file_name = Path(s3_key).name
        file_date = file_name[: file_name.find(".")]
        file_date = datetime.date.fromisoformat(file_date)
        if file_date < max_allowed_date:
            all_files.pop(x)

    print("Filtering out dates that are already ingested")
    # filter out dates that are already ingested
    current_files = os.listdir(os.getenv("EQUITY_MINUTE_DATA_PATH"))
    current_files = [cf[: cf.find(".gzip")] for cf in current_files]
    for x in range(len(all_files) - 1, -1, -1):
        curr_name = Path(all_files[x]["key"]).name
        curr_name_date = curr_name[: curr_name.find(".csv.gz")]
        if curr_name_date in current_files:
            all_files.pop(x)

    print("Downloading files")
    for file in tqdm(all_files, desc="downloading files"):
        s3_key = file["key"]
        base_save_path = os.getenv("EQUITY_MINUTE_DATA_PATH")
        name_path = Path(s3_key).name
        output_path = conn.download_file(
            s3_key=s3_key,
            local_path=base_save_path + "/" + name_path,
            decompress=True,
        )
        data = pd.read_csv(output_path)
        data.to_parquet(str(output_path).replace(".csv", ".gzip"))
        os.remove(output_path)

    print("Successfully downloaded all data")
