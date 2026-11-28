SELECT
    reading_id::bigint AS reading_id,
    timestamp_utc::timestamptz AS timestamp_utc,
    (timestamp_utc AT TIME ZONE 'UTC')::date AS reading_date_utc,
    DATE_TRUNC('hour', timestamp_utc AT TIME ZONE 'UTC') AS reading_hour_utc,
    TRIM(container_id)::varchar(10) AS container_id,
    LOWER(TRIM(product))::varchar(50) AS product,
    TRIM(route)::varchar(100) AS route,
    NULLIF(SPLIT_PART(TRIM(route), '-', 1), '') AS origin_city,
    NULLIF(SPLIT_PART(TRIM(route), '-', 2), '') AS destination_city,
    temperature_c::numeric(6, 2) AS temperature_c,
    humidity_pct::numeric(6, 2) AS humidity_pct,
    vibration_g::numeric(6, 2) AS vibration_g
FROM {{ source('atmosync_raw', 'sensor_readings') }}
