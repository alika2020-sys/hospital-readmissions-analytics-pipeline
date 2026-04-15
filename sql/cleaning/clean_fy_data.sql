-- ================================================
-- FILE: sql/cleaning/clean_fy_data.sql
-- Purpose: Clean the raw CMS FY2026 HRRP data
-- Steps:
--   1. Create a non-destructive working copy
--   2. Replace N/A and invalid strings with NULL
--   3. Cast columns to proper numeric types
--   4. Convert date strings to DATE type
--   5. Decode footnote codes into human-readable labels
-- Author: San
-- ================================================


-- ------------------------------------------------
-- STEP 1: Create a clean working copy
-- Never modify the raw table directly so the
-- original data remains intact and auditable
-- ------------------------------------------------
CREATE TABLE fy_clean AS
SELECT * FROM fy;


-- ------------------------------------------------
-- STEP 2: Replace N/A and invalid strings with NULL
-- These placeholders prevent proper numeric casting
-- and produce incorrect aggregations if left as text
-- ------------------------------------------------
UPDATE fy_clean
SET "Number of Discharges" = NULL
WHERE "Number of Discharges" = 'N/A';

UPDATE fy_clean
SET "Number of Readmissions" = NULL
WHERE "Number of Readmissions" IN ('N/A', 'Too Few to Report');

UPDATE fy_clean
SET "Excess Readmission Ratio" = NULL
WHERE "Excess Readmission Ratio" = 'N/A';

UPDATE fy_clean
SET "Predicted Readmission Rate" = NULL
WHERE "Predicted Readmission Rate" = 'N/A';

UPDATE fy_clean
SET "Expected Readmission Rate" = NULL
WHERE "Expected Readmission Rate" = 'N/A';


-- ------------------------------------------------
-- STEP 3: Cast text columns to proper numeric types
-- Must run AFTER step 2 — casting will fail if
-- N/A strings are still present in the columns
-- ------------------------------------------------
ALTER TABLE fy_clean
  ALTER COLUMN "Number of Discharges" TYPE INTEGER
    USING "Number of Discharges"::INTEGER,
  ALTER COLUMN "Number of Readmissions" TYPE INTEGER
    USING "Number of Readmissions"::INTEGER,
  ALTER COLUMN "Excess Readmission Ratio" TYPE NUMERIC(10,4)
    USING "Excess Readmission Ratio"::NUMERIC,
  ALTER COLUMN "Predicted Readmission Rate" TYPE NUMERIC(10,4)
    USING "Predicted Readmission Rate"::NUMERIC,
  ALTER COLUMN "Expected Readmission Rate" TYPE NUMERIC(10,4)
    USING "Expected Readmission Rate"::NUMERIC;


-- ------------------------------------------------
-- STEP 4: Convert date strings to proper DATE type
-- Original format is MM/DD/YYYY stored as text
-- Converting enables date arithmetic and filtering
-- ------------------------------------------------
ALTER TABLE fy_clean
  ALTER COLUMN "Start Date" TYPE DATE
    USING TO_DATE("Start Date", 'MM/DD/YYYY'),
  ALTER COLUMN "End Date" TYPE DATE
    USING TO_DATE("End Date", 'MM/DD/YYYY');


-- ------------------------------------------------
-- STEP 5: Add human-readable footnote descriptions
-- CMS uses numeric codes to explain missing data
-- Decoding these makes the data self-documenting
-- ------------------------------------------------
ALTER TABLE fy_clean
  ADD COLUMN footnote_description VARCHAR(200);

UPDATE fy_clean
SET footnote_description = CASE "Footnote"
  WHEN '1'  THEN 'No data available'
  WHEN '5'  THEN 'Results based on a shorter time period than required'
  WHEN '7'  THEN 'No cases meet the criteria for this measure'
  WHEN '29' THEN 'Too few discharges to reliably report'
  ELSE NULL
END;


-- ------------------------------------------------
-- STEP 6: Verify the cleaned table
-- ------------------------------------------------
SELECT
  COUNT(*)                            AS total_rows,
  COUNT("Number of Discharges")       AS has_discharges,
  COUNT("Excess Readmission Ratio")   AS has_ratio,
  COUNT("Number of Readmissions")     AS has_readmissions
FROM fy_clean;
