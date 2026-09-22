#!/bin/bash
# One-time GCP infrastructure setup for carrier-safety-vetting-tool
# Run once after gcloud auth login

set -e

PROJECT_ID="carrier-vetting-tool"
DATASET="fmcsa_raw"
BUCKET="fmcsa-ingestion-raw"
TOPIC="fmcsa-file-loaded"
REGION="us-central1"

echo "Setting project to $PROJECT_ID..."
gcloud config set project $PROJECT_ID

echo "Creating GCS bucket..."
gsutil mb -l $REGION gs://$BUCKET

echo "Creating Pub/Sub topic..."
gcloud pubsub topics create $TOPIC

echo "Creating GCS → Pub/Sub notification..."
gsutil notification create \
  -t $TOPIC \
  -f json \
  -e OBJECT_FINALIZE \
  gs://$BUCKET

echo "Creating BigQuery dataset..."
bq mk --dataset --location=US $PROJECT_ID:$DATASET

echo "Deploying Cloud Functions..."

for fn in load_crashes load_inspections load_census; do
  echo "Deploying $fn..."
  gcloud functions deploy $fn \
    --runtime python311 \
    --trigger-topic $TOPIC \
    --entry-point main \
    --source cloud_functions/$fn \
    --region $REGION \
    --set-env-vars PROJECT_ID=$PROJECT_ID,DATASET=$DATASET \
    --timeout 300s \
    --memory 512MB
done

echo "Creating BigQuery views..."
bq query --use_legacy_sql=false < bigquery/views/v_census.sql
bq query --use_legacy_sql=false < bigquery/views/v_crashes.sql
bq query --use_legacy_sql=false < bigquery/views/v_inspections.sql
bq query --use_legacy_sql=false < bigquery/views/v_carrier_summary.sql

echo "Setup complete."
