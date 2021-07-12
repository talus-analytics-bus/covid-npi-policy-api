-- Add `name_and_desc` field to policy and plan and populate
-- Policy
-- alter table policy add column name_and_desc text;
UPDATE
  "policy"
SET
  name_and_desc = CONCAT(policy_name, ': ', "desc")
WHERE
  policy_name IS NOT NULL
  AND policy_name != ''
  AND policy_name != 'Unspecified';
UPDATE
  "policy"
SET
  name_and_desc = "desc"
WHERE
  policy_name IS NULL
  OR policy_name = ''
  OR policy_name = 'Unspecified';
-- Plan
  -- alter table plan add column name_and_desc text;
UPDATE
  "plan"
SET
  name_and_desc = CONCAT("name", ': ', "desc")
WHERE
  "name" IS NOT NULL
  AND "name" != ''
  AND "name" != 'Unspecified';
UPDATE
  "plan"
SET
  name_and_desc = "desc"
WHERE
  "name" IS NULL
  OR "name" = ''
  OR "name" != 'Unspecified';