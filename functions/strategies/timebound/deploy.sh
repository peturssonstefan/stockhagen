#!/bin/bash
gcloud functions deploy timebound-strategy --gen2 --region=europe-north1 --runtime=python311 --entry-point=start --trigger-http