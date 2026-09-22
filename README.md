# Carrier Safety Vetting Platform

An event-driven data pipeline that automates FMCSA carrier safety compliance checks — replacing manual spreadsheet vetting with a structured BigQuery warehouse and live Power BI dashboard.

## Architecture

```
FMCSA Public Data (pipe-delimited .txt or .csv)
        │
        ▼
Google Cloud Storage (fmcsa-ingestion-raw)
        │  [OBJECT_FINALIZE event]
        ▼
Cloud Pub/Sub (fmcsa-file-loaded)
        │  [triggers]
        ▼
Cloud Function — ingest_fmcsa (Python)
   │
   ├── route_table()     filename → crashes_raw / inspections_raw / census_raw
   ├── detect_format()   .csv (comma) vs .txt (pipe-delimited)
   ├── ensure_table_exists()
   └── submit_load_job() → async BQ Load Job (free, no row limits)
        │
        ▼
BigQuery Dataset: fmcsa_raw (carrier-vetting-tool)
   ├── crashes_raw        (57 columns, WRITE_APPEND)
   ├── inspections_raw    (3 columns, WRITE_APPEND)
   └── census_raw         (140+ columns, WRITE_APPEND)
        │
        ▼
BigQuery Views
   ├── v_census           cleaned carrier identity + safety rating fields
   ├── v_crashes          typed/cast crash records with severity fields
   ├── v_inspections      inspection records
   └── v_carrier_summary  pre-aggregated risk profile (joins all three)
        │
        ▼
Power BI Report (Import mode)
   └── Carrier risk dashboard — search by DOT#/name, safety flags,
       conditional formatting by risk tier (HIGH / MEDIUM / LOW / UNRATED)
```

## Why Load Jobs (not Streaming Inserts)

The function uses BigQuery Load Jobs instead of `insert_rows_json`:

- **Free** — load jobs have no per-row cost; streaming inserts do
- **No size limits** — streaming inserts cap at 10MB/request and 50K rows/request, which breaks on large FMCSA files
- **Async** — the function kicks off the job and returns immediately; BigQuery handles the rest
- **Idempotent** — `WRITE_APPEND` + explicit schemas prevents partial load issues
- **Handles messy data** — `max_bad_records=1000` and `ignore_unknown_values=True` absorb FMCSA format quirks

## File Routing Logic

The single Cloud Function routes files by filename pattern:

| Filename contains | Target table |
|---|---|
| `crash` | `crashes_raw` |
| `inspection` | `inspections_raw` |
| anything else | `census_raw` |

Files ending in `.csv` are treated as comma-delimited; all others (`.txt`) as pipe-delimited (`|`).

## GCP Resources

| Resource | Name |
|---|---|
| GCP Project | `carrier-vetting-tool` |
| BigQuery Dataset | `fmcsa_raw` |
| GCS Bucket | `fmcsa-ingestion-raw` |
| Pub/Sub Topic | `fmcsa-file-loaded` |
| Cloud Function | `ingest_fmcsa` |

## Project Structure

```
carrier-safety-vetting/
├── cloud_function/
│   ├── main.py              ← single function, handles all three file types
│   └── requirements.txt
├── bigquery/
│   └── views/
│       ├── v_census.sql
│       ├── v_crashes.sql
│       ├── v_inspections.sql
│       └── v_carrier_summary.sql
├── infra/
│   └── setup.sh             ← one-shot GCP deploy script
└── README.md
```

## Setup

### Prerequisites
- GCP project with billing enabled
- APIs enabled: Cloud Functions, Pub/Sub, BigQuery, Cloud Storage
- `gcloud` CLI authenticated

### Deploy

```bash
bash infra/setup.sh
```

Or manually:

```bash
# 1. Create GCS bucket
gsutil mb gs://fmcsa-ingestion-raw

# 2. Create Pub/Sub topic and GCS notification
gcloud pubsub topics create fmcsa-file-loaded
gsutil notification create \
  -t fmcsa-file-loaded \
  -f json \
  -e OBJECT_FINALIZE \
  gs://fmcsa-ingestion-raw

# 3. Create BigQuery dataset
bq mk --dataset --location=US carrier-vetting-tool:fmcsa_raw

# 4. Deploy Cloud Function
gcloud functions deploy ingest_fmcsa \
  --runtime python311 \
  --trigger-topic fmcsa-file-loaded \
  --entry-point ingest_fmcsa \
  --source cloud_function/ \
  --region us-central1 \
  --timeout 300s \
  --memory 512MB

# 5. Create BigQuery views (run in order)
bq query --use_legacy_sql=false < bigquery/views/v_census.sql
bq query --use_legacy_sql=false < bigquery/views/v_crashes.sql
bq query --use_legacy_sql=false < bigquery/views/v_inspections.sql
bq query --use_legacy_sql=false < bigquery/views/v_carrier_summary.sql
```

### Loading Data

Upload FMCSA files directly to the GCS bucket — the pipeline triggers automatically:

```bash
gsutil cp fmcsa_crash_2024.txt gs://fmcsa-ingestion-raw/
gsutil cp fmcsa_inspection_2024.txt gs://fmcsa-ingestion-raw/
gsutil cp fmcsa_census_2024.txt gs://fmcsa-ingestion-raw/
```

## Data Source

FMCSA public datasets: [ai.fmcsa.dot.gov/SMS](https://ai.fmcsa.dot.gov/SMS)
