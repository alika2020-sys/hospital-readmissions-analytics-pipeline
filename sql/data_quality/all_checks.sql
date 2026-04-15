-- ================================================
-- FILE: sql/data_quality/all_checks.sql
-- Purpose: Run all 6 data quality checks before
-- any analysis is performed
-- Checks:
--   1. Null/Missing Value Check
--   2. Duplicate Records Check
--   3. Outlier Detection (3-sigma rule)
--   4. Referential Integrity Check
--   5. Range Validity Check
--   6. Completeness by State
-- Author: San
-- ================================================


-- ================================================
-- DATA QUALITY CHECK 1: Null/Missing Value Check
-- Purpose: Identify how many records are missing
-- key metrics in the fact table
-- ================================================
SELECT
    'fact_readmissions'                                                         AS table_name,
    COUNT(*)                                                                    AS total_rows,

    -- Count missing values for each key column
    SUM(CASE WHEN number_of_discharges IS NULL       THEN 1 ELSE 0 END)        AS missing_discharges,
    SUM(CASE WHEN number_of_readmissions IS NULL     THEN 1 ELSE 0 END)        AS missing_readmissions,
    SUM(CASE WHEN excess_readmission_ratio IS NULL   THEN 1 ELSE 0 END)        AS missing_excess_ratio,
    SUM(CASE WHEN predicted_readmission_rate IS NULL THEN 1 ELSE 0 END)        AS missing_predicted_rate,
    SUM(CASE WHEN expected_readmission_rate IS NULL  THEN 1 ELSE 0 END)        AS missing_expected_rate,

    -- Calculate percentage of missing excess ratio records
    ROUND(100.0 * SUM(CASE WHEN excess_readmission_ratio IS NULL
        THEN 1 ELSE 0 END) / COUNT(*), 2)                                      AS pct_missing_ratio

FROM fact_readmissions;


-- ================================================
-- DATA QUALITY CHECK 2: Duplicate Records Check
-- Purpose: Ensure no hospital appears more than once
-- for the same measure and time period
-- ================================================
SELECT
    facility_id,
    measure_id,
    date_id,
    COUNT(*) AS duplicate_count  -- anything > 1 is a duplicate
FROM fact_readmissions
GROUP BY facility_id, measure_id, date_id

-- Only return groups that have more than one record
HAVING COUNT(*) > 1

ORDER BY duplicate_count DESC;


-- ================================================
-- DATA QUALITY CHECK 3: Outlier Detection
-- Purpose: Find hospitals with statistically abnormal
-- excess readmission ratios using 3 standard deviations
-- from the mean (3-sigma rule)
-- ================================================
WITH stats AS (
    -- Calculate national mean, standard deviation,
    -- and interquartile range for reference
    SELECT
        AVG(excess_readmission_ratio)                    AS mean_ratio,
        STDDEV(excess_readmission_ratio)                 AS stddev_ratio,
        PERCENTILE_CONT(0.25) WITHIN GROUP
            (ORDER BY excess_readmission_ratio)          AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP
            (ORDER BY excess_readmission_ratio)          AS q3
    FROM fact_readmissions
    WHERE excess_readmission_ratio IS NOT NULL
)
SELECT
    f.facility_name,
    f.state,
    m.measure_name,
    r.excess_readmission_ratio,
    ROUND(s.mean_ratio, 4)                               AS mean_ratio,
    ROUND(s.stddev_ratio, 4)                             AS stddev_ratio,

    -- Flag values more than 3 standard deviations from the mean
    CASE
        WHEN r.excess_readmission_ratio > s.mean_ratio + (3 * s.stddev_ratio) THEN 'High Outlier'
        WHEN r.excess_readmission_ratio < s.mean_ratio - (3 * s.stddev_ratio) THEN 'Low Outlier'
        ELSE 'Normal'
    END AS outlier_status

FROM fact_readmissions r
JOIN dim_facility f ON r.facility_id = f.facility_id
JOIN dim_measure  m ON r.measure_id  = m.measure_id

-- CROSS JOIN brings the stats into every row for comparison
CROSS JOIN stats s

WHERE r.excess_readmission_ratio IS NOT NULL
  AND (
    -- Only return actual outliers
    r.excess_readmission_ratio > s.mean_ratio + (3 * s.stddev_ratio) OR
    r.excess_readmission_ratio < s.mean_ratio - (3 * s.stddev_ratio)
  )
ORDER BY r.excess_readmission_ratio DESC;


-- ================================================
-- DATA QUALITY CHECK 4: Referential Integrity Check
-- Purpose: Ensure every record in the fact table
-- has a valid matching record in each dimension table
-- Orphaned records indicate a broken relationship
-- ================================================

-- Check for missing facility references
SELECT 'Missing facility' AS issue, COUNT(*) AS count
FROM fact_readmissions r
LEFT JOIN dim_facility f ON r.facility_id = f.facility_id
WHERE f.facility_id IS NULL  -- NULL means no match found in dim_facility

UNION ALL

-- Check for missing measure references
SELECT 'Missing measure', COUNT(*)
FROM fact_readmissions r
LEFT JOIN dim_measure m ON r.measure_id = m.measure_id
WHERE m.measure_id IS NULL  -- NULL means no match found in dim_measure

UNION ALL

-- Check for missing date references
SELECT 'Missing date', COUNT(*)
FROM fact_readmissions r
LEFT JOIN dim_date d ON r.date_id = d.date_id
WHERE d.date_id IS NULL;  -- NULL means no match found in dim_date


-- ================================================
-- DATA QUALITY CHECK 5: Range Validity Check
-- Purpose: Confirm all numeric values fall within
-- realistic and expected boundaries
-- e.g. ratios should not be negative or above 2
-- rates should be between 0 and 100
-- ================================================
SELECT
    COUNT(*)                                                                   AS total_rows,

    -- Excess readmission ratio should be between 0 and 2
    SUM(CASE WHEN excess_readmission_ratio < 0     THEN 1 ELSE 0 END)         AS negative_ratios,
    SUM(CASE WHEN excess_readmission_ratio > 2     THEN 1 ELSE 0 END)         AS ratios_above_2,

    -- Number of discharges should never be negative
    SUM(CASE WHEN number_of_discharges < 0         THEN 1 ELSE 0 END)         AS negative_discharges,

    -- Readmission rate is a percentage so must be between 0 and 100
    SUM(CASE WHEN predicted_readmission_rate > 100 THEN 1 ELSE 0 END)         AS rates_above_100pct,
    SUM(CASE WHEN predicted_readmission_rate < 0   THEN 1 ELSE 0 END)         AS negative_rates

FROM fact_readmissions;


-- ================================================
-- DATA QUALITY CHECK 6: Completeness by State
-- Purpose: Identify which states have the most
-- missing data so we know where reporting gaps exist
-- High missing % may indicate reporting compliance
-- issues in certain regions
-- ================================================
SELECT
    f.state,
    COUNT(*)                                                                       AS total_records,

    -- Count missing values per state
    SUM(CASE WHEN r.excess_readmission_ratio IS NULL   THEN 1 ELSE 0 END)         AS missing_ratio,
    SUM(CASE WHEN r.number_of_readmissions IS NULL     THEN 1 ELSE 0 END)         AS missing_readmissions,

    -- Calculate what percentage of records are missing the excess ratio
    ROUND(100.0 * SUM(CASE WHEN r.excess_readmission_ratio IS NULL
        THEN 1 ELSE 0 END) / COUNT(*), 2)                                          AS pct_missing

FROM fact_readmissions r
JOIN dim_facility f ON r.facility_id = f.facility_id

-- Group by state to get per-state completeness
GROUP BY f.state

-- Show worst states first
ORDER BY pct_missing DESC;
