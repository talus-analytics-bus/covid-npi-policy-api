-- Update `version` table to advance last updated date and most recent datum
-- date of COVID caseload data by one day
--
-- Making this update shifts the default/max date selectable on the Map page
-- (covidamp.org/policymaps) forward by one day so the most recent caseload
-- data can be viewed overlain on the policy data.
UPDATE version
SET
  "last_datum_date" = (CURRENT_DATE - INTERVAL '1 DAY')::DATE,
  "date" = CURRENT_DATE
WHERE
  "type" LIKE 'COVID-19 caseload data%';
