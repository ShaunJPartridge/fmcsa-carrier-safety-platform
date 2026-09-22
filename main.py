"""
FMCSA ingestion Cloud Function — GCS-triggered, BigQuery Load Job pattern.

Why Load Jobs instead of insert_rows_json (Streaming Inserts)?
  - No row/payload size limits per batch
  - No per-row cost (load jobs are free)
  - Handles files of any size without memory pressure
  - Idempotent via WRITE_TRUNCATE or WRITE_APPEND + dedup view
  - Runs async — Cloud Function returns fast, BQ does the heavy lifting
  - Streaming Inserts have a 10MB/request and 50K row/request cap
    which causes the errors you're seeing on large files

Architecture:
  GCS upload → Pub/Sub → Cloud Function → BQ Load Job (async)
  The function just *kicks off* the load job and returns. BQ handles the rest.
"""

import json
import re
from google.cloud import bigquery
from google.cloud import storage

bq_client = bigquery.Client()
storage_client = storage.Client()

# ── Schema definitions ────────────────────────────────────────────────────────

SCHEMAS = {
    "crashes_raw": [
        bigquery.SchemaField("CHANGE_DATE",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_ID",                     "STRING",  "NULLABLE"),
        bigquery.SchemaField("REPORT_STATE",                 "STRING",  "NULLABLE"),
        bigquery.SchemaField("REPORT_NUMBER",                "STRING",  "NULLABLE"),
        bigquery.SchemaField("REPORT_DATE",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("REPORT_TIME",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("REPORT_SEQ_NO",                "STRING",  "NULLABLE"),
        bigquery.SchemaField("DOT_NUMBER",                   "STRING",  "NULLABLE"),
        bigquery.SchemaField("CI_STATUS_CODE",               "STRING",  "NULLABLE"),
        bigquery.SchemaField("FINAL_STATUS_DATE",            "STRING",  "NULLABLE"),
        bigquery.SchemaField("LOCATION",                     "STRING",  "NULLABLE"),
        bigquery.SchemaField("CITY_CODE",                    "STRING",  "NULLABLE"),
        bigquery.SchemaField("CITY",                         "STRING",  "NULLABLE"),
        bigquery.SchemaField("STATE",                        "STRING",  "NULLABLE"),
        bigquery.SchemaField("COUNTY_CODE",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("TRUCK_BUS_IND",                "STRING",  "NULLABLE"),
        bigquery.SchemaField("TRAFFICWAY_ID",                "STRING",  "NULLABLE"),
        bigquery.SchemaField("ACCESS_CONTROL_ID",            "STRING",  "NULLABLE"),
        bigquery.SchemaField("ROAD_SURFACE_CONDITION_ID",    "STRING",  "NULLABLE"),
        bigquery.SchemaField("CARGO_BODY_TYPE_ID",           "STRING",  "NULLABLE"),
        bigquery.SchemaField("GVW_RATING_ID",                "STRING",  "NULLABLE"),
        bigquery.SchemaField("VEHICLE_IDENTIFICATION_NUMBER","STRING",  "NULLABLE"),
        bigquery.SchemaField("VEHICLE_LICENSE_NUMBER",       "STRING",  "NULLABLE"),
        bigquery.SchemaField("VEHICLE_LIC_STATE",            "STRING",  "NULLABLE"),
        bigquery.SchemaField("VEHICLE_HAZMAT_PLACARD",       "STRING",  "NULLABLE"),
        bigquery.SchemaField("WEATHER_CONDITION_ID",         "STRING",  "NULLABLE"),
        bigquery.SchemaField("VEHICLE_CONFIGURATION_ID",     "STRING",  "NULLABLE"),
        bigquery.SchemaField("LIGHT_CONDITION_ID",           "STRING",  "NULLABLE"),
        bigquery.SchemaField("HAZMAT_RELEASED",              "STRING",  "NULLABLE"),
        bigquery.SchemaField("AGENCY",                       "STRING",  "NULLABLE"),
        bigquery.SchemaField("VEHICLES_IN_ACCIDENT",         "STRING", "NULLABLE"),
        bigquery.SchemaField("FATALITIES",                   "STRING", "NULLABLE"),
        bigquery.SchemaField("INJURIES",                     "STRING", "NULLABLE"),
        bigquery.SchemaField("TOW_AWAY",                     "STRING",  "NULLABLE"),
        bigquery.SchemaField("FEDERAL_RECORDABLE",           "STRING",  "NULLABLE"),
        bigquery.SchemaField("STATE_RECORDABLE",             "STRING",  "NULLABLE"),
        bigquery.SchemaField("SNET_VERSION_NUMBER",          "STRING",  "NULLABLE"),
        bigquery.SchemaField("SNET_SEQUENCE_ID",             "STRING",  "NULLABLE"),
        bigquery.SchemaField("TRANSACTION_CODE",             "STRING",  "NULLABLE"),
        bigquery.SchemaField("TRANSACTION_DATE",             "STRING",  "NULLABLE"),
        bigquery.SchemaField("UPLOAD_FIRST_BYTE",            "STRING",  "NULLABLE"),
        bigquery.SchemaField("UPLOAD_DOT_NUMBER",            "STRING",  "NULLABLE"),
        bigquery.SchemaField("UPLOAD_SEARCH_INDICATOR",      "STRING",  "NULLABLE"),
        bigquery.SchemaField("UPLOAD_DATE",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("ADD_DATE",                     "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_CARRIER_ID",             "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_CARRIER_NAME",           "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_CARRIER_STREET",         "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_CARRIER_CITY",           "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_CARRIER_CITY_CODE",      "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_CARRIER_STATE",          "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_CARRIER_ZIP_CODE",       "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_COLONIA",                "STRING",  "NULLABLE"),
        bigquery.SchemaField("DOCKET_NUMBER",                "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_CARRIER_INTERSTATE",     "STRING",  "NULLABLE"),
        bigquery.SchemaField("NO_ID_FLAG",                   "STRING",  "NULLABLE"),
        bigquery.SchemaField("STATE_NUMBER",                 "STRING",  "NULLABLE"),
        bigquery.SchemaField("STATE_ISSUING_NUMBER",         "STRING",  "NULLABLE"),
        bigquery.SchemaField("CRASH_EVENT_SEQ_ID_DESC",      "STRING",  "NULLABLE"),
    ],

    # Inspections: only the two columns we need
    "inspections_raw": [
        bigquery.SchemaField("CHANGE_DATE",  "STRING",  "NULLABLE"),
        bigquery.SchemaField("INSPECTION_ID",  "STRING", "NULLABLE"),
        bigquery.SchemaField("DOT_NUMBER",  "STRING",  "NULLABLE"),
    ],

    # Census: all 23 columns from the FMCSA census file
    "census_raw": [
        bigquery.SchemaField("MCS150_DATE", "STRING", "NULLABLE"),
        bigquery.SchemaField("ADD_DATE", "STRING", "NULLABLE"),
        bigquery.SchemaField("STATUS_CODE", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOT_NUMBER", "STRING", "NULLABLE"),
        bigquery.SchemaField("DUN_BRADSTREET_NO", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHY_OMC_REGION", "STRING", "NULLABLE"),
        bigquery.SchemaField("SAFETY_INV_TERR", "STRING", "NULLABLE"),
        bigquery.SchemaField("CARRIER_OPERATION", "STRING", "NULLABLE"),
        bigquery.SchemaField("BUSINESS_ORG_ID", "STRING", "NULLABLE"),
        bigquery.SchemaField("MCS150_MILEAGE", "STRING", "NULLABLE"),
        bigquery.SchemaField("MCS150_MILEAGE_YEAR", "STRING", "NULLABLE"),
        bigquery.SchemaField("MCS151_MILEAGE", "STRING", "NULLABLE"),
        bigquery.SchemaField("TOTAL_CARS", "STRING", "NULLABLE"),
        bigquery.SchemaField("MCS150_UPDATE_CODE_ID", "STRING", "NULLABLE"),
        bigquery.SchemaField("PRIOR_REVOKE_FLAG", "STRING", "NULLABLE"),
        bigquery.SchemaField("PRIOR_REVOKE_DOT_NUMBER", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHONE", "STRING", "NULLABLE"),
        bigquery.SchemaField("FAX", "STRING", "NULLABLE"),
        bigquery.SchemaField("CELL_PHONE", "STRING", "NULLABLE"),
        bigquery.SchemaField("COMPANY_OFFICER_1", "STRING", "NULLABLE"),
        bigquery.SchemaField("COMPANY_OFFICER_2", "STRING", "NULLABLE"),
        bigquery.SchemaField("BUSINESS_ORG_DESC", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRUCK_UNITS", "STRING", "NULLABLE"),
        bigquery.SchemaField("POWER_UNITS", "STRING", "NULLABLE"),
        bigquery.SchemaField("BUS_UNITS", "STRING", "NULLABLE"),
        bigquery.SchemaField("FLEETSIZE", "STRING", "NULLABLE"),
        bigquery.SchemaField("REVIEW_ID", "STRING", "NULLABLE"),
        bigquery.SchemaField("RECORDABLE_CRASH_RATE", "STRING", "NULLABLE"),
        bigquery.SchemaField("MAIL_NATIONALITY_INDICATOR", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHY_NATIONALITY_INDICATOR", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHY_BARRIO", "STRING", "NULLABLE"),
        bigquery.SchemaField("MAIL_BARRIO", "STRING", "NULLABLE"),
        bigquery.SchemaField("CARSHIP", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOCKET1PREFIX", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOCKET1", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOCKET2PREFIX", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOCKET2", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOCKET3PREFIX", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOCKET3", "STRING", "NULLABLE"),
        bigquery.SchemaField("POINTNUM", "STRING", "NULLABLE"),
        bigquery.SchemaField("TOTAL_INTRASTATE_DRIVERS", "STRING", "NULLABLE"),
        bigquery.SchemaField("MCSIPSTEP", "STRING", "NULLABLE"),
        bigquery.SchemaField("MCSIPDATE", "STRING", "NULLABLE"),
        bigquery.SchemaField("HM_Ind", "STRING", "NULLABLE"),
        bigquery.SchemaField("INTERSTATE_BEYOND_100_MILES", "STRING", "NULLABLE"),
        bigquery.SchemaField("INTERSTATE_WITHIN_100_MILES", "STRING", "NULLABLE"),
        bigquery.SchemaField("INTRASTATE_BEYOND_100_MILES", "STRING", "NULLABLE"),
        bigquery.SchemaField("INTRASTATE_WITHIN_100_MILES", "STRING", "NULLABLE"),
        bigquery.SchemaField("TOTAL_CDL", "STRING", "NULLABLE"),
        bigquery.SchemaField("TOTAL_DRIVERS", "STRING", "NULLABLE"),
        bigquery.SchemaField("AVG_DRIVERS_LEASED_PER_MONTH", "STRING", "NULLABLE"),
        bigquery.SchemaField("CLASSDEF", "STRING", "NULLABLE"),
        bigquery.SchemaField("LEGAL_NAME", "STRING", "NULLABLE"),
        bigquery.SchemaField("DBA_NAME", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHY_STREET", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHY_CITY", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHY_COUNTRY", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHY_STATE", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHY_ZIP", "STRING", "NULLABLE"),
        bigquery.SchemaField("PHY_CNTY", "STRING", "NULLABLE"),
        bigquery.SchemaField("CARRIER_MAILING_STREET", "STRING", "NULLABLE"),
        bigquery.SchemaField("CARRIER_MAILING_STATE", "STRING", "NULLABLE"),
        bigquery.SchemaField("CARRIER_MAILING_CITY", "STRING", "NULLABLE"),
        bigquery.SchemaField("CARRIER_MAILING_COUNTRY", "STRING", "NULLABLE"),
        bigquery.SchemaField("CARRIER_MAILING_ZIP", "STRING", "NULLABLE"),
        bigquery.SchemaField("CARRIER_MAILING_CNTY", "STRING", "NULLABLE"),
        bigquery.SchemaField("CARRIER_MAILING_UND_DATE", "STRING", "NULLABLE"),
        bigquery.SchemaField("DRIVER_INTER_TOTAL", "STRING", "NULLABLE"),
        bigquery.SchemaField("EMAIL_ADDRESS", "STRING", "NULLABLE"),
        bigquery.SchemaField("REVIEW_TYPE", "STRING", "NULLABLE"),
        bigquery.SchemaField("REVIEW_DATE", "STRING", "NULLABLE"),
        bigquery.SchemaField("SAFETY_RATING", "STRING", "NULLABLE"),
        bigquery.SchemaField("SAFETY_RATING_DATE", "STRING", "NULLABLE"),
        bigquery.SchemaField("UNDELIV_PHY", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_GENFREIGHT", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_HOUSEHOLD", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_METALSHEET", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_MOTOVEH", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_DRIVETOW", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_LOGPOLE", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_BLDGMAT", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_MOBILEHOME", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_MACHLRG", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_PRODUCE", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_LIQGAS", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_INTERMODAL", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_PASSENGERS", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_OILFIELD", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_LIVESTOCK", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_GRAINFEED", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_COALCOKE", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_MEAT", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_GARBAGE", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_USMAIL", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_CHEM", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_DRYBULK", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_COLDFOOD", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_BEVERAGES", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_PAPERPROD", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_UTILITY", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_FARMSUPP", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_CONSTRUCT", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_WATERWELL", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_CARGOOTHR", "STRING", "NULLABLE"),
        bigquery.SchemaField("CRGO_CARGOOTHR_DESC", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNTRUCK", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNTRACT", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNTRAIL", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNCOACH", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNSCHOOL_1_8", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNSCHOOL_9_15", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNSCHOOL_16", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNBUS_16", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNVAN_1_8", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNVAN_9_15", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNLIMO_1_8", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNLIMO_9_15", "STRING", "NULLABLE"),
        bigquery.SchemaField("OWNLIMO_16", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMTRUCK", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMTRACT", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMTRAIL", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMCOACH", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMSCHOOL_1_8", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMSCHOOL_9_15", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMSCHOOL_16", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMBUS_16", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMVAN_1_8", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMVAN_9_15", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMLIMO_1_8", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMLIMO_9_15", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRMLIMO_16", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPTRUCK", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPTRACT", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPTRAIL", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPCOACH", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPSCHOOL_1_8", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPSCHOOL_9_15", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPSCHOOL_16", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPBUS_16", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPVAN_1_8", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPVAN_9_15", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPLIMO_1_8", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPLIMO_9_15", "STRING", "NULLABLE"),
        bigquery.SchemaField("TRPLIMO_16", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOCKET1_STATUS_CODE", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOCKET2_STATUS_CODE", "STRING", "NULLABLE"),
        bigquery.SchemaField("DOCKET3_STATUS_CODE", "STRING", "NULLABLE"),
    ],
}

# ── Table routing ─────────────────────────────────────────────────────────────

DATASET = "fmcsa_raw"
PROJECT = "carrier-vetting-tool"

def route_table(file_name: str) -> tuple[str, str]:
    """Return (table_id, table_short_name) for a given file name."""
    name = file_name.lower()
    if "crash" in name:
        short = "crashes_raw"
    elif "inspection" in name:
        short = "inspections_raw"
    else:
        short = "census_raw"
    return f"{PROJECT}.{DATASET}.{short}", short


# ── Table creation ─────────────────────────────────────────────────────────────

def ensure_table_exists(table_id: str, short_name: str) -> None:
    """Create the BQ table if it doesn't already exist."""
    schema = SCHEMAS[short_name]

    # For autodetect tables (schema=None), pass empty list —
    # BQ will infer the schema from the file on first load
    table = bigquery.Table(table_id, schema=schema if schema is not None else [])
    table.description = f"Raw FMCSA {short_name} — append-only"

    try:
        bq_client.create_table(table)
        print(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"Table {table_id} already exists — skipping create")
        else:
            raise


# ── File format detection ──────────────────────────────────────────────────────

def detect_format(file_name: str) -> tuple[bigquery.SourceFormat, str]:
    """
    Return (SourceFormat, delimiter).
    FMCSA files are typically pipe-delimited .txt or comma-delimited .csv.
    """
    name = file_name.lower()
    if name.endswith(".csv"):
        return bigquery.SourceFormat.CSV, ","
    return bigquery.SourceFormat.CSV, "|"


# ── Core load job ──────────────────────────────────────────────────────────────

def submit_load_job(
    bucket_name: str,
    file_name: str,
    table_id: str,
    short_name: str,
) -> bigquery.LoadJob:
    uri = f"gs://{bucket_name}/{file_name}"
    source_format, delimiter = detect_format(file_name)
    schema = SCHEMAS[short_name]

    if schema is None:
        # Autodetect — let BQ figure out everything
        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
            field_delimiter=delimiter,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
            quote_character='"',
            allow_quoted_newlines=True,
            max_bad_records=1000,
        )
    else:
        # Explicit schema
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=source_format,
            field_delimiter=delimiter,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
            ignore_unknown_values=True,
            max_bad_records=1000,
            null_marker="",
        )

    job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    print(f"Load job submitted: {job.job_id} | {uri} → {table_id}")
    return job


# ── Entry point ────────────────────────────────────────────────────────────────

def ingest_fmcsa(cloud_event):
    """Cloud Function entry point — triggered by GCS finalize event via Pub/Sub."""
    try:
        # ── 1. Parse cloud event ───────────────────────────────────────────────
        data = cloud_event.data

        if isinstance(data, (bytes, bytearray)):
            data = json.loads(data.decode("utf-8"))
        elif isinstance(data, str):
            data = json.loads(data)

        bucket_name = data["bucket"]
        file_name   = data["name"]

        print(f"Event received: gs://{bucket_name}/{file_name}")

        # Skip non-data files (e.g. .json manifests, temp files)
        skip_patterns = [r"\.json$", r"^\."]
        if any(re.search(p, file_name.lower()) for p in skip_patterns):
            print(f"Skipping non-data file: {file_name}")
            return "OK"

        # ── 2. Route ───────────────────────────────────────────────────────────
        table_id, short_name = route_table(file_name)
        print(f"Routing {file_name} → {table_id}")

        # ── 3. Ensure table exists ─────────────────────────────────────────────
        # For autodetect tables (crashes), skip manual creation —
        # BQ creates the table automatically on first load job
        schema = SCHEMAS[short_name]
        if schema is not None:
            ensure_table_exists(table_id, short_name)
        else:
            print(f"Autodetect table — skipping manual create, BQ will create on load")

        # ── 4. Submit load job ─────────────────────────────────────────────────
        job = submit_load_job(bucket_name, file_name, table_id, short_name)

        print(f"Done — load job {job.job_id} is running asynchronously")
        
        return "OK"

    except Exception as e:
        print(f"ERROR in ingest_fmcsa: {type(e).__name__}: {e}")
        raise
