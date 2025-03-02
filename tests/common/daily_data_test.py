import pandas as pd
import os
import glob
from matplotlib import pyplot as plt
from tqdm import tqdm

all_daily_files = glob.glob(os.getenv("EQUITY_DAILY_DATA_PATH") + "/*.gzip")

all_data = list()
for file in tqdm(all_daily_files):
    data = pd.read_parquet(file, filters=[("ticker", "=", "AAPL")])
    all_data.append(data)
all_data = pd.concat(all_data, axis=0)
all_data["window_start_dt"] = pd.to_datetime(all_data["window_start"])

all_data.set_index("window_start_dt")["close"].plot()
plt.show()
