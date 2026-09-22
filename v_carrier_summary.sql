-- v_carrier_summary
-- Pre-aggregates crash metrics in a CTE before joining to census
-- to prevent row multiplication on the DOT_NUMBER join.

CREATE OR REPLACE VIEW `carrier-vetting-tool.fmcsa_raw.v_carrier_summary` AS

WITH crash_agg AS (
  SELECT
    DOT_NUMBER,
    COUNT(*)                                        AS total_crashes,
    SUM(fatalities)                                 AS total_fatalities,
    SUM(injuries)                                   AS total_injuries,
    SUM(tow_away)                                   AS total_tow_aways,
    SUM(vehicles_in_accident)                       AS total_vehicles_involved,
    COUNTIF(HAZMAT_RELEASED = 'Y')                  AS hazmat_incidents,
    COUNTIF(FEDERAL_RECORDABLE = 'Y')               AS federal_recordable_crashes
  FROM `carrier-vetting-tool.fmcsa_raw.v_crashes`
  WHERE DOT_NUMBER IS NOT NULL
  GROUP BY DOT_NUMBER
),

inspection_agg AS (
  SELECT
    DOT_NUMBER,
    COUNT(*)                                        AS total_inspections
  FROM `carrier-vetting-tool.fmcsa_raw.v_inspections`
  WHERE DOT_NUMBER IS NOT NULL
  GROUP BY DOT_NUMBER
)

SELECT
  -- Carrier identity
  c.DOT_NUMBER,
  c.LEGAL_NAME,
  c.DBA_NAME,
  c.CARRIER_OPERATION,
  c.hazmat_indicator,
  c.PHY_CITY,
  c.PHY_STATE,
  c.POWER_UNITS,
  c.TOTAL_DRIVERS,
  c.FLEETSIZE,

  -- Safety rating
  c.SAFETY_RATING,
  c.SAFETY_RATING_DATE,
  c.REVIEW_TYPE,
  c.REVIEW_DATE,
  c.RECORDABLE_CRASH_RATE,

  -- Crash metrics (pre-aggregated)
  COALESCE(cr.total_crashes, 0)               AS total_crashes,
  COALESCE(cr.total_fatalities, 0)            AS total_fatalities,
  COALESCE(cr.total_injuries, 0)              AS total_injuries,
  COALESCE(cr.total_tow_aways, 0)             AS total_tow_aways,
  COALESCE(cr.hazmat_incidents, 0)            AS hazmat_incidents,
  COALESCE(cr.federal_recordable_crashes, 0)  AS federal_recordable_crashes,

  -- Inspection count
  COALESCE(i.total_inspections, 0)            AS total_inspections,

  -- Risk flag
  CASE
    WHEN c.SAFETY_RATING = 'Unsatisfactory'                    THEN 'HIGH'
    WHEN c.SAFETY_RATING = 'Conditional'                       THEN 'MEDIUM'
    WHEN COALESCE(cr.total_crashes, 0) > 5                     THEN 'MEDIUM'
    WHEN c.SAFETY_RATING = 'Satisfactory'                      THEN 'LOW'
    ELSE 'UNRATED'
  END AS risk_flag

FROM `carrier-vetting-tool.fmcsa_raw.v_census` c
LEFT JOIN crash_agg      cr ON c.DOT_NUMBER = cr.DOT_NUMBER
LEFT JOIN inspection_agg i  ON c.DOT_NUMBER = i.DOT_NUMBER
