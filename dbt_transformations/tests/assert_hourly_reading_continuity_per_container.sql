{{ config(severity='warn') }}

-- Monitoring/warning check: flags gaps larger than 1 hour between consecutive container readings.
SELECT
    reading_id,
    container_id,
    prev_timestamp_utc,
    timestamp_utc,
    hours_since_prev_reading
FROM {{ ref('int_container_telemetry_enriched') }}
WHERE hours_since_prev_reading IS NOT NULL
  AND hours_since_prev_reading > 1.0
