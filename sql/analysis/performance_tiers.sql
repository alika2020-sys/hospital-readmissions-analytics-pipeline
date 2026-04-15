-- ================================================
-- FILE: sql/analysis/performance_tiers.sql
-- Purpose: Categorize every hospital-measure combo
-- into a performance tier based on excess ratio
-- Tiers:
--   Excellent        = ratio < 0.90
--   Good             = ratio 0.90 to 0.99
--   Needs Improvement= ratio 1.00 to 1.09
--   Poor             = ratio >= 1.10
-- Technique: CASE WHEN + multi-table JOIN
-- Author: San
-- ================================================

SELECT
    f.facility_name,
    f.state,
    m.measure_name,
    r.excess_readmission_ratio,

    -- Rank each hospital within its state for this measure
    RANK() OVER (
        PARTITION BY f.state, m.measure_name
        ORDER BY r.excess_readmission_ratio DESC
    ) AS rank_in_state,

    -- Assign performance tier based on CMS benchmark thresholds
    CASE
        WHEN r.excess_readmission_ratio < 0.90  THEN 'Excellent'
        WHEN r.excess_readmission_ratio < 1.00  THEN 'Good'
        WHEN r.excess_readmission_ratio < 1.10  THEN 'Needs Improvement'
        ELSE                                         'Poor'
    END AS performance_tier

FROM fact_readmissions r
JOIN dim_facility f ON r.facility_id = f.facility_id
JOIN dim_measure  m ON r.measure_id  = m.measure_id
WHERE r.excess_readmission_ratio IS NOT NULL
ORDER BY r.excess_readmission_ratio DESC;
