-- Fails if any telemetry reading has a timestamp more than 5 minutes in the future.
SELECT
    reading_id,
    container_id,
    timestamp_utc
FROM {{ ref('stg_sensor_readings') }}
WHERE timestamp_utc > NOW() + INTERVAL '5 minutes'
