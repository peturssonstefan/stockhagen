import functions_framework
from google.cloud import storage
import pandas as pd
from datetime import datetime, timedelta

def getLatestPrice(ticker, cache):
    if ticker in cache:
        print("found in cache")
        return cache[ticker]
    else: 
        price_file = f'{ticker}.csv'
        prices = pd.read_csv(f'gs://stockhagen-stocks/{price_file}')
        latest_price = prices.iloc[-1]
        cache[ticker] = latest_price
        return latest_price

@functions_framework.http
def start(request):
    date = datetime.today() - timedelta(days=5)
    current_price = {}
    storage_client = storage.Client()
    bought_blobs = storage_client.list_blobs("stockhagen-buys")
    current_ledger_blobs = storage_client.list_blobs("stockhagen-strategies")
    bucket_name = "timebound_5_days"
    ledger = pd.DataFrame([], columns=["strategy","ticker", "purch_date", "purch_price","sold_date", "sold_price"])
    for blob in current_ledger_blobs:
        if blob.name == f'{bucket_name}.csv':
            ledger = pd.read_csv(f'gs://stockhagen-strategies/{blob.name}')

    #Close trades 
    for idx, row in ledger.iterrows():
        purch_date = datetime.strptime(row["purch_date"], "%Y-%m-%d")
        if purch_date < date and row["sold_price"] == 0:
            latest_price = getLatestPrice(row["ticker"], current_price)
            ledger.loc[idx, ["sold_date", "sold_price"]] = [latest_price["Date"],latest_price["Close"]]

    # Note: The call returns a response only when the iterator is consumed.
    default_time_stamp = datetime.utcfromtimestamp(0).strftime('%Y-%m-%d')
    for blob in bought_blobs:
        df = pd.read_csv(f'gs://stockhagen-buys/{blob.name}')
        strategy_name = blob.name.split(".")[0]
        for idx, row in df.iterrows(): 
            new_row = {"strategy":strategy_name,"ticker": row["ticker"], "purch_date": row["purch_date"], "purch_price": row["purch_price"], "sold_date": default_time_stamp, "sold_price": 0}
            ledger = ledger.append(new_row, ignore_index=True)
    
    ledger.to_csv(f'gs://stockhagen-strategies/{bucket_name}.csv', index=False)
    return "Read complete"