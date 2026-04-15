-- ================================================
-- FILE: sql/analysis/state_performance.sql
-- Purpose: Compare each state's average excess
-- readmission ratio against the national average
-- Labels states as Above or Below Average
-- Technique: CTEs + CROSS JOIN
-- Author: San
-- ================================================

WITH state_avg AS (
    -- Calculate average excess ratio per state
    SELECT
        f.state,
        AVG(r.excess_readmission_ratio) AS avg_ratio
    FROM fact_readmissions r
    JOIN dim_facility f ON r.facility_id = f.facility_id
    WHERE r.excess_readmission_ratio IS NOT NULL
    GROUP BY f.state
),

overall_avg AS (
    -- Calculate the single national average for comparison
    SELECT AVG(excess_readmission_ratio) AS national_avg
    FROM fact_readmissions
    WHERE excess_readmission_ratio IS NOT NULL
)

SELECT
    s.state,
    ROUND(s.avg_ratio, 4)       AS state_avg,
    ROUND(o.national_avg, 4)    AS national_avg,

    -- How much above or below the national average this state is
    ROUND(s.avg_ratio - o.national_avg, 4) AS difference_from_national,

    -- Performance label for non-technical stakeholders
    CASE
        WHEN s.avg_ratio > o.national_avg THEN 'Above Average'
        ELSE                                   'Below Average'
    END AS performance

FROM state_avg s

-- CROSS JOIN brings the single national average into every row
CROSS JOIN overall_avg o

ORDER BY s.avg_ratio DESC;
