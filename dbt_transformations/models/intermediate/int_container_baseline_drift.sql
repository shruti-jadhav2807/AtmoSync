{% set baseline_window = var('baseline_window_readings', 12) | int %}
{% set zscore_cutoff = var('drift_zscore_cutoff', 2.0) | float %}

WITH enriched AS (
    SELECT *
    FROM {{ ref('int_container_telemetry_enriched') }}
),

-- Assumed statistical baseline: first N readings per container (default N = 12).
-- Used purely for relative anomaly detection, NOT confirmed biological spoilage.
container_baselines AS (
    SELECT
        container_id,
        COUNT(*)::int AS baseline_sample_count,
        ROUND(AVG(temperature_c), 2) AS baseline_avg_temp_c,
        ROUND(STDDEV_SAMP(temperature_c), 4) AS baseline_stddev_temp_c,
        ROUND(AVG(humidity_pct), 2) AS baseline_avg_humidity_pct,
        ROUND(STDDEV_SAMP(humidity_pct), 4) AS baseline_stddev_humidity_pct,
        ROUND(AVG(vibration_g), 2) AS baseline_avg_vibration_g
    FROM enriched
    WHERE reading_seq_num <= {{ baseline_window }}
    GROUP BY container_id
),

scored AS (
    SELECT
        e.*,
        b.baseline_sample_count,
        b.baseline_avg_temp_c,
        b.baseline_stddev_temp_c,
        b.baseline_avg_humidity_pct,
        b.baseline_stddev_humidity_pct,
        b.baseline_avg_vibration_g,
        ROUND((e.temperature_c - b.baseline_avg_temp_c)::numeric, 2) AS temp_drift_from_baseline_c,
        ROUND((e.humidity_pct - b.baseline_avg_humidity_pct)::numeric, 2) AS humidity_drift_from_baseline_pct,
        ROUND((e.vibration_g - b.baseline_avg_vibration_g)::numeric, 2) AS vibration_drift_from_baseline_g,
        -- NULLIF(..., 0) prevents division-by-zero when baseline standard deviation is zero or NULL.
        ROUND(
            ((e.temperature_c - b.baseline_avg_temp_c) / NULLIF(b.baseline_stddev_temp_c, 0))::numeric,
            2
        ) AS temp_baseline_zscore,
        ROUND(
            ((e.humidity_pct - b.baseline_avg_humidity_pct) / NULLIF(b.baseline_stddev_humidity_pct, 0))::numeric,
            2
        ) AS humidity_baseline_zscore
    FROM enriched AS e
    INNER JOIN container_baselines AS b
        ON e.container_id = b.container_id
)

SELECT
    *,
    -- Statistical anomaly heuristic flags (|z-score| >= drift_zscore_cutoff); NOT confirmed spoilage.
    COALESCE(ABS(temp_baseline_zscore) >= {{ zscore_cutoff }}, FALSE) AS is_statistical_temp_drift,
    COALESCE(ABS(humidity_baseline_zscore) >= {{ zscore_cutoff }}, FALSE) AS is_statistical_humidity_drift,
    (
        COALESCE(ABS(temp_baseline_zscore) >= {{ zscore_cutoff }}, FALSE)
        OR COALESCE(ABS(humidity_baseline_zscore) >= {{ zscore_cutoff }}, FALSE)
    ) AS is_statistical_drift_anomaly
FROM scored
