-- ================================================
-- FILE: sql/schema/create_star_schema.sql
-- Purpose: Build a star schema data warehouse from
-- the cleaned CMS FY2026 HRRP data
-- Tables created:
--   dim_facility      - hospital reference data
--   dim_measure       - readmission measure definitions
--   dim_date          - date range reference data
--   fact_readmissions - central fact table with metrics
-- Author: San
-- ================================================


-- ------------------------------------------------
-- STEP 1: Create dim_facility
-- Contains one row per unique hospital
-- Derived from distinct values in fy_clean
-- ------------------------------------------------
CREATE TABLE dim_facility AS
SELECT DISTINCT
    "Facility ID"   AS facility_id,
    "Facility Name" AS facility_name,
    "State"         AS state
FROM fy_clean;

-- Add primary key for fast lookups and JOIN performance
ALTER TABLE dim_facility ADD PRIMARY KEY (facility_id);


-- ------------------------------------------------
-- STEP 2: Create dim_measure
-- Contains one row per CMS readmission measure type
-- Manually seeded from CMS measure definitions
-- ------------------------------------------------
CREATE TABLE dim_measure (
    measure_id   SERIAL PRIMARY KEY,
    measure_code VARCHAR(50)  UNIQUE,
    measure_name VARCHAR(200)
);

INSERT INTO dim_measure (measure_code, measure_name) VALUES
    ('READM-30-AMI-HRRP',      'Acute Myocardial Infarction 30-Day Readmission'),
    ('READM-30-CABG-HRRP',     'Coronary Artery Bypass Graft 30-Day Readmission'),
    ('READM-30-COPD-HRRP',     'Chronic Obstructive Pulmonary Disease 30-Day Readmission'),
    ('READM-30-HF-HRRP',       'Heart Failure 30-Day Readmission'),
    ('READM-30-HIP-KNEE-HRRP', 'Hip and Knee Replacement 30-Day Readmission'),
    ('READM-30-PN-HRRP',       'Pneumonia 30-Day Readmission');


-- ------------------------------------------------
-- STEP 3: Create dim_date
-- Contains one row per unique date range
-- Extracts year and month for easier time filtering
-- ------------------------------------------------
CREATE TABLE dim_date AS
SELECT DISTINCT
    "Start Date"                             AS start_date,
    "End Date"                               AS end_date,
    EXTRACT(YEAR  FROM "Start Date")         AS start_year,
    EXTRACT(MONTH FROM "Start Date")         AS start_month,
    EXTRACT(YEAR  FROM "End Date")           AS end_year,
    EXTRACT(MONTH FROM "End Date")           AS end_month
FROM fy_clean;

-- Add surrogate primary key after table creation
ALTER TABLE dim_date ADD COLUMN date_id SERIAL PRIMARY KEY;


-- ------------------------------------------------
-- STEP 4: Create fact_readmissions
-- Central fact table joining all dimensions
-- Contains all measurable metrics per hospital/measure/date
-- ------------------------------------------------
CREATE TABLE fact_readmissions AS
SELECT
    f."Facility ID"                  AS facility_id,
    m.measure_id                     AS measure_id,
    d.date_id                        AS date_id,
    f."Number of Discharges"         AS number_of_discharges,
    f."Number of Readmissions"       AS number_of_readmissions,
    f."Excess Readmission Ratio"     AS excess_readmission_ratio,
    f."Predicted Readmission Rate"   AS predicted_readmission_rate,
    f."Expected Readmission Rate"    AS expected_readmission_rate,
    f.footnote_description
FROM fy_clean f
JOIN dim_measure m ON m.measure_code = f."Measure Name"
JOIN dim_date    d ON d.start_date   = f."Start Date"
                  AND d.end_date     = f."End Date";

-- Add surrogate primary key
ALTER TABLE fact_readmissions ADD COLUMN fact_id SERIAL PRIMARY KEY;

-- Add foreign key constraints to enforce referential integrity
ALTER TABLE fact_readmissions
    ADD CONSTRAINT fk_facility FOREIGN KEY (facility_id) REFERENCES dim_facility(facility_id),
    ADD CONSTRAINT fk_measure  FOREIGN KEY (measure_id)  REFERENCES dim_measure(measure_id),
    ADD CONSTRAINT fk_date     FOREIGN KEY (date_id)     REFERENCES dim_date(date_id);


-- ------------------------------------------------
-- STEP 5: Verify row counts across all tables
-- ------------------------------------------------
SELECT 'dim_facility'     AS table_name, COUNT(*) AS rows FROM dim_facility
UNION ALL
SELECT 'dim_measure',                    COUNT(*) FROM dim_measure
UNION ALL
SELECT 'dim_date',                       COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_readmissions',              COUNT(*) FROM fact_readmissions;


-- ------------------------------------------------
-- STEP 6: Test a joined query across the star schema
-- ------------------------------------------------
SELECT
    f.facility_name,
    f.state,
    m.measure_name,
    r.number_of_discharges,
    r.excess_readmission_ratio
FROM fact_readmissions r
JOIN dim_facility f ON r.facility_id = f.facility_id
JOIN dim_measure  m ON r.measure_id  = m.measure_id
JOIN dim_date     d ON r.date_id     = d.date_id
LIMIT 10;
