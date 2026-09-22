CREATE OR REPLACE VIEW `carrier-vetting-tool.fmcsa_raw.v_inspections` AS

SELECT
  DOT_NUMBER,
  INSPECTION_ID,
  CHANGE_DATE
FROM `carrier-vetting-tool.fmcsa_raw.inspections_raw`
WHERE DOT_NUMBER IS NOT NULL
  AND DOT_NUMBER != ''
