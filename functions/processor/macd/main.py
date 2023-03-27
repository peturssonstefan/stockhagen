from io import BytesIO
import functions_framework
from google.cloud import storage
import requests
import pandas as pd
import pandas_ta as pta

@functions_framework.http
def start(request):
    storage_client = storage.Client()
    ticker_blobs = storage_client.list_blobs("stockhagen-stocks")
    buys = []
    for ticker in ticker_blobs:
        print(f"Processing: {ticker}")
        df = pd.read_csv(f'gs://stockhagen-stocks/{ticker.name}')
        df.ta.macd(append=True)
        lastRows = df.tail(2)
        print(df)
        if lastRows.iloc[0]["MACD_12_26_9"] < lastRows.iloc[0]["MACDs_12_26_9"] and lastRows.iloc[1]["MACD_12_26_9"] > lastRows.iloc[1]["MACDs_12_26_9"]:
            ticker_name = ticker.name.split(".")[0]
            buys.append([ticker_name, lastRows.iloc[1]["Date"], lastRows.iloc[1]["Close"]])

    buys_df = pd.DataFrame(buys, columns=["ticker", "purch_date", "purch_price"])
    buys_df.to_csv(f'gs://stockhagen-buys/macd-trader.csv')
    return f"{buys}"