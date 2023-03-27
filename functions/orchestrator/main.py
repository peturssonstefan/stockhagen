
import os
import google.auth.transport.requests
import google.oauth2.id_token
import functions_framework
import requests
import json

def make_authorized_get_request():
    print("Starting")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './service-acc.json'
    aud = "https://europe-north1-stockhagen.cloudfunctions.net/get-stock-data"
    endpoint = aud
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, aud)

    r = requests.post(
        endpoint, 
        headers={'Authorization': f"bearer {id_token}", "Content-Type": "application/json"},
        data=json.dumps({"key": "value"})  
    )

    print(r.status_code, r.reason)
    return "All good"

@functions_framework.http
def start(request):
    return make_authorized_get_request()
