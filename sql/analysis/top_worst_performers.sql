-- ================================================
-- FILE: sql/analysis/top_worst_performers.sql
-- Purpose: Identify hospitals in the top 10% worst
-- performers nationally by excess readmission ratio
-- These hospitals are priority candidates for
-- intervention and review
-- Technique: Subquery + PERCENTILE_CONT
-- Author: San
-- ================================================

SELECT
    f.facility_name,
    f.state,
    m.measure_name,
    r.excess_readmission_ratio,

    -- Rank nationally so stakeholders know relative severity
    RANK() OVER (
        ORDER BY r.excess_readmission_ratio DESC
    ) AS national_rank

FROM fact_readmissions r
JOIN dim_facility f ON r.facility_id = f.facility_id
JOIN dim_measure  m ON r.measure_id  = m.measure_id

-- Subquery calculates the 90th percentile cutoff
-- Only hospitals above this threshold are returned
WHERE r.excess_readmission_ratio > (
    SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (
        ORDER BY excess_readmission_ratio
    )
    FROM fact_readmissions
    WHERE excess_readmission_ratio IS NOT NULL
)

ORDER BY r.excess_readmission_ratio DESC;
