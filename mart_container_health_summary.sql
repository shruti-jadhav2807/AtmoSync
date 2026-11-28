WITH scored AS (
    SELECT *
    FROM {{ ref('int_container_baseline_drift') }}
),

latest_per_container AS (
    SELECT
        container_id,
        timestamp_utc AS latest_reading_utc,
        temperature_c AS latest_temp_c,
        humidity_pct AS latest_humidity_pct,
        vibration_g AS latest_vibration_g,
        temp_drift_from_baseline_c AS latest_temp_drift_c,
        humidity_drift_from_baseline_pct AS latest_humidity_drift_pct,
        temp_baseline_zscore AS latest_temp_zscore,
        humidity_baseline_zscore AS latest_humidity_zscore,
        is_statistical_drift_anomaly AS latest_is_statistical_drift_anomaly
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY container_id
                ORDER BY timestamp_utc DESC, reading_id DESC
            ) AS rn
        FROM scored
    ) AS ranked
    WHERE rn = 1
),

aggregates AS (
    SELECT
        container_id,
        MIN(product) AS product,
        MIN(route) AS route,
        MIN(origin_city) AS origin_city,
        MIN(destination_city) AS destination_city,
        COUNT(*)::int AS total_readings,
        MIN(timestamp_utc) AS first_reading_utc,
        MAX(baseline_sample_count)::int AS baseline_sample_count,
        MAX(baseline_avg_temp_c) AS baseline_avg_temp_c,
        MAX(baseline_avg_humidity_pct) AS baseline_avg_humidity_pct,
        MIN(temperature_c) AS min_temp_c,
        MAX(temperature_c) AS max_temp_c,
        ROUND(AVG(temperature_c), 2) AS avg_temp_c,
        MIN(humidity_pct) AS min_humidity_pct,
        MAX(humidity_pct) AS max_humidity_pct,
        ROUND(AVG(humidity_pct), 2) AS avg_humidity_pct,
        MIN(vibration_g) AS min_vibration_g,
        MAX(vibration_g) AS max_vibration_g,
        ROUND(AVG(vibration_g), 2) AS avg_vibration_g,
        MAX(ABS(temp_baseline_zscore)) AS peak_abs_temp_zscore,
        MAX(ABS(humidity_baseline_zscore)) AS peak_abs_humidity_zscore,
        COUNT(*) FILTER (WHERE is_statistical_temp_drift)::int AS statistical_temp_drift_count,
        COUNT(*) FILTER (WHERE is_statistical_humidity_drift)::int AS statistical_humidity_drift_count,
        COUNT(*) FILTER (WHERE is_statistical_drift_anomaly)::int AS statistical_drift_anomaly_count
    FROM scored
    GROUP BY container_id
)

SELECT
    a.container_id,
    a.product,
    a.route,
    a.origin_city,
    a.destination_city,
    a.total_readings,
    a.first_reading_utc,
    l.latest_reading_utc,
    l.latest_temp_c,
    l.latest_humidity_pct,
    l.latest_vibration_g,
    a.baseline_sample_count,
    a.baseline_avg_temp_c,
    a.baseline_avg_humidity_pct,
    l.latest_temp_drift_c,
    l.latest_humidity_drift_pct,
    l.latest_temp_zscore,
    l.latest_humidity_zscore,
    l.latest_is_statistical_drift_anomaly,
    a.min_temp_c,
    a.max_temp_c,
    a.avg_temp_c,
    a.min_humidity_pct,
    a.max_humidity_pct,
    a.avg_humidity_pct,
    a.min_vibration_g,
    a.max_vibration_g,
    a.avg_vibration_g,
    a.peak_abs_temp_zscore,
    a.peak_abs_humidity_zscore,
    a.statistical_temp_drift_count,
    a.statistical_humidity_drift_count,
    a.statistical_drift_anomaly_count
FROM aggregates AS a
INNER JOIN latest_per_container AS l
    ON a.container_id = l.container_id
