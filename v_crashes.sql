CREATE OR REPLACE VIEW `carrier-vetting-tool.fmcsa_raw.v_crashes` AS

SELECT
  DOT_NUMBER,
  CRASH_ID,
  REPORT_STATE,
  REPORT_NUMBER,
  SAFE_CAST(REPORT_DATE AS DATE FORMAT 'MM/DD/YYYY')   AS report_date,
  STATE                                                AS crash_state,
  SAFE_CAST(FATALITIES AS INT64)                       AS fatalities,
  SAFE_CAST(INJURIES AS INT64)                         AS injuries,
  SAFE_CAST(TOW_AWAY AS INT64)                         AS tow_away,
  SAFE_CAST(VEHICLES_IN_ACCIDENT AS INT64)             AS vehicles_in_accident,
  HAZMAT_RELEASED,
  VEHICLE_HAZMAT_PLACARD,
  TRUCK_BUS_IND,
  CARGO_BODY_TYPE_ID,
  FEDERAL_RECORDABLE,
  STATE_RECORDABLE,
  WEATHER_CONDITION_ID,
  ROAD_SURFACE_CONDITION_ID,
  LIGHT_CONDITION_ID,
  VEHICLE_CONFIGURATION_ID,
  CRASH_CARRIER_NAME,
  CRASH_CARRIER_STATE
FROM `carrier-vetting-tool.fmcsa_raw.crashes_raw`
WHERE DOT_NUMBER IS NOT NULL
  AND DOT_NUMBER != ''
