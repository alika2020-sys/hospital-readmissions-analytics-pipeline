-- ================================================
-- FILE: sql/analysis/sla_violations.sql
-- Purpose: Detect hospitals violating CMS healthcare
-- readmission SLA thresholds
-- SLA Definitions:
--   Excess Readmission Ratio > 1.10  = violated
--   Predicted Readmission Rate > 15% = violated
--   Number of Discharges < 25        = volume too low
-- Technique: CTEs + CASE WHEN + Window Functions
-- Author: San
-- ================================================

WITH sla_checks AS (
    -- Flag each hospital-measure row against all 3 SLA rules
    SELECT
        f.facility_id,
        f.facility_name,
        f.state,
        m.measure_name,
        r.excess_readmission_ratio,
        r.predicted_readmission_rate,
        r.number_of_discharges,

        -- SLA Flag 1: Ratio more than 10% above expected
        CASE WHEN r.excess_readmission_ratio > 1.10   THEN 1 ELSE 0 END AS sla_excess_ratio_violated,

        -- SLA Flag 2: Predicted readmission rate exceeds 15%
        CASE WHEN r.predicted_readmission_rate > 15.0 THEN 1 ELSE 0 END AS sla_pred_rate_violated,

        -- SLA Flag 3: Volume too low for reliable benchmarking
        CASE WHEN r.number_of_discharges < 25         THEN 1 ELSE 0 END AS sla_low_volume_violated

    FROM fact_readmissions r
    JOIN dim_facility f ON r.facility_id = f.facility_id
    JOIN dim_measure  m ON r.measure_id  = m.measure_id
    WHERE r.excess_readmission_ratio   IS NOT NULL
      AND r.predicted_readmission_rate IS NOT NULL
      AND r.number_of_discharges       IS NOT NULL
),

sla_summary AS (
    -- Aggregate violations per hospital across all measures
    SELECT
        facility_id,
        facility_name,
        state,
        COUNT(*)                                                AS total_measures,
        SUM(sla_excess_ratio_violated)                         AS excess_ratio_violations,
        SUM(sla_pred_rate_violated)                            AS pred_rate_violations,
        SUM(sla_low_volume_violated)                           AS low_volume_violations,

        -- Total violations across all 3 SLA rules
        SUM(sla_excess_ratio_violated +
            sla_pred_rate_violated +
            sla_low_volume_violated)                           AS total_violations,

        ROUND(AVG(excess_readmission_ratio), 4)                AS avg_excess_ratio,
        ROUND(AVG(predicted_readmission_rate), 4)              AS avg_predicted_rate
    FROM sla_checks
    GROUP BY facility_id, facility_name, state
)

SELECT
    facility_name,
    state,
    total_measures,
    excess_ratio_violations,
    pred_rate_violations,
    low_volume_violations,
    total_violations,
    avg_excess_ratio,
    avg_predicted_rate,

    -- Violation rate: what % of possible SLA checks did this hospital fail
    ROUND(100.0 * total_violations / (total_measures * 3), 2)  AS sla_violation_rate_pct,

    -- National rank by total violations (1 = worst)
    RANK() OVER (ORDER BY total_violations DESC)               AS violation_rank,

    -- Overall SLA compliance status
    CASE
        WHEN total_violations = 0                THEN 'SLA Compliant'
        WHEN total_violations BETWEEN 1 AND 3    THEN 'Minor Violations'
        WHEN total_violations BETWEEN 4 AND 6    THEN 'Serious Violations'
        ELSE                                          'Critical - SLA Breach'
    END AS sla_status

FROM sla_summary
ORDER BY total_violations DESC;
