from io import BytesIO
import functions_framework
from google.cloud import storage
import requests
import pandas as pd
import pandas_ta as pta

@functions_framework.http
def start(request):
    buys = []
    storage_client = storage.Client()
    ticker_blobs = storage_client.list_blobs("stockhagen-stocks")
    for ticker in ticker_blobs:
        print(f"Processing: {ticker.name}")
        df = pd.read_csv(f'gs://stockhagen-stocks/{ticker.name}')
        df["rsi"] = pta.rsi(df['Close'], length = 14)
        lastRows = df.tail(1)
        print(lastRows.iloc[0])
        if lastRows.iloc[0]["rsi"] < 30:
            ticker_name = ticker.name.split(".")[0]
            buys.append([ticker_name, lastRows.iloc[0]["Date"], lastRows.iloc[0]["Close"]])

    buys_df = pd.DataFrame(buys, columns=["ticker", "purch_date", "purch_price"])
    buys_df.to_csv(f'gs://stockhagen-buys/rsi-trader.csv')
    return f"{buys}"