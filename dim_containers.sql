SELECT
    container_id,
    MIN(product) AS product,
    MIN(route) AS route,
    MIN(origin_city) AS origin_city,
    MIN(destination_city) AS destination_city,
    MIN(timestamp_utc) AS first_reading_utc,
    MAX(timestamp_utc) AS latest_reading_utc,
    COUNT(*)::int AS total_readings
FROM {{ ref('stg_sensor_readings') }}
GROUP BY container_id
