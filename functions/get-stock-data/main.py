from io import BytesIO
import functions_framework
from google.cloud import storage
import requests
import pandas as pd
import calendar
import datetime
import google.auth.transport.requests
import google.oauth2.id_token
import urllib

@functions_framework.http
def start(request):

    # own_endpoint = "https://get-stock-data-df4dqjd3pa-lz.a.run.app/"
    # req = urllib.request.Request(own_endpoint)
    # auth_req = google.auth.transport.requests.Request()
    # id_token = google.oauth2.id_token.fetch_id_token(auth_req, own_endpoint)
    # req.add_header("Authorization", f"Bearer {id_token}")
    # response = urllib.request.urlopen(req)

    date = datetime.datetime.utcnow()
    utc_time = calendar.timegm(date.utctimetuple())
    one_year_ago = utc_time - 31536000
    tickers = ["AAPL","F","V","META", "SONO", "SRNE", "MD"]
    fakeUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    params = {"period1":one_year_ago, "period2":utc_time, "interval":"1d", "events":"history", "includeAdjustedClose":"true"}
    headers = {"User-Agent": fakeUserAgent}

    for ticker in tickers:
        print(f"Uploading: {ticker}")
        url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}" 
        res = requests.get(url=url, headers=headers, params=params)
        content = res.content
        df = pd.read_csv(BytesIO(content))
        df.to_csv(f'gs://stockhagen-stocks/{ticker}.csv')
    
    return "Upload complete"