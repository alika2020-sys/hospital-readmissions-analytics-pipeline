-- ================================================
-- FILE: sql/analysis/bottleneck_detection.sql
-- Purpose: Identify hospitals that consistently
-- underperform their state average across multiple
-- readmission measures (bottleneck facilities)
-- Technique: Window functions + CTEs
-- Author: San
-- ================================================

WITH hospital_performance AS (
    -- Calculate each hospital's deviation from
    -- its state average for every measure
    SELECT
        f.facility_id,
        f.facility_name,
        f.state,
        m.measure_name,
        r.excess_readmission_ratio,

        -- State average for this specific measure using window function
        AVG(r.excess_readmission_ratio) OVER (
            PARTITION BY f.state, m.measure_name
        ) AS state_avg_by_measure,

        -- How far above or below the state average this hospital is
        r.excess_readmission_ratio - AVG(r.excess_readmission_ratio) OVER (
            PARTITION BY f.state, m.measure_name
        ) AS deviation_from_state_avg

    FROM fact_readmissions r
    JOIN dim_facility f ON r.facility_id = f.facility_id
    JOIN dim_measure  m ON r.measure_id  = m.measure_id
    WHERE r.excess_readmission_ratio IS NOT NULL
),

bottleneck_count AS (
    -- Summarize performance across all measures per hospital
    SELECT
        facility_id,
        facility_name,
        state,
        COUNT(*)                                                          AS total_measures,

        -- Count how many measures this hospital is above its state avg
        SUM(CASE WHEN deviation_from_state_avg > 0 THEN 1 ELSE 0 END)   AS measures_above_state_avg,

        -- Average deviation tells us how far above average this hospital is overall
        ROUND(AVG(deviation_from_state_avg), 4)                          AS avg_deviation

    FROM hospital_performance
    GROUP BY facility_id, facility_name, state
)

SELECT
    facility_name,
    state,
    total_measures,
    measures_above_state_avg,
    avg_deviation,

    -- Rank hospitals within their state by how much they deviate above average
    RANK() OVER (
        PARTITION BY state
        ORDER BY avg_deviation DESC
    ) AS bottleneck_rank_in_state,

    -- Categorize severity of bottleneck status
    CASE
        WHEN measures_above_state_avg = total_measures             THEN 'Critical Bottleneck'
        WHEN measures_above_state_avg >= total_measures * 0.75     THEN 'High Risk'
        WHEN measures_above_state_avg >= total_measures * 0.50     THEN 'Moderate Risk'
        ELSE                                                             'On Track'
    END AS bottleneck_status

FROM bottleneck_count
ORDER BY avg_deviation DESC;
