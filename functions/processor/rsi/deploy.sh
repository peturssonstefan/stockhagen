#!/bin/bash
gcloud functions deploy rsi-trader --gen2 --region=europe-north1 --runtime=python311 --entry-point=start --trigger-http