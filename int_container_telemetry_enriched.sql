WITH base AS (
    SELECT
        reading_id,
        timestamp_utc,
        reading_date_utc,
        reading_hour_utc,
        container_id,
        product,
        route,
        origin_city,
        destination_city,
        temperature_c,
        humidity_pct,
        vibration_g
    FROM {{ ref('stg_sensor_readings') }}
)

SELECT
    reading_id,
    timestamp_utc,
    reading_date_utc,
    reading_hour_utc,
    container_id,
    product,
    route,
    origin_city,
    destination_city,
    temperature_c,
    humidity_pct,
    vibration_g,
    ROW_NUMBER() OVER (
        PARTITION BY container_id
        ORDER BY timestamp_utc ASC, reading_id ASC
    ) AS reading_seq_num,
    LAG(timestamp_utc) OVER (
        PARTITION BY container_id
        ORDER BY timestamp_utc ASC, reading_id ASC
    ) AS prev_timestamp_utc,
    ROUND(
        (
            EXTRACT(
                EPOCH FROM (
                    timestamp_utc
                    - LAG(timestamp_utc) OVER (
                        PARTITION BY container_id
                        ORDER BY timestamp_utc ASC, reading_id ASC
                    )
                )
            ) / 3600.0
        )::numeric,
        2
    ) AS hours_since_prev_reading,
    ROUND(
        (
            temperature_c
            - LAG(temperature_c) OVER (
                PARTITION BY container_id
                ORDER BY timestamp_utc ASC, reading_id ASC
            )
        )::numeric,
        2
    ) AS temp_delta_1h,
    ROUND(
        (
            humidity_pct
            - LAG(humidity_pct) OVER (
                PARTITION BY container_id
                ORDER BY timestamp_utc ASC, reading_id ASC
            )
        )::numeric,
        2
    ) AS humidity_delta_1h,
    ROUND(
        (
            vibration_g
            - LAG(vibration_g) OVER (
                PARTITION BY container_id
                ORDER BY timestamp_utc ASC, reading_id ASC
            )
        )::numeric,
        2
    ) AS vibration_delta_1h,
    -- Assumed smoothing window: trailing 6 readings (6h on hourly cadence).
    ROUND(
        AVG(temperature_c) OVER (
            PARTITION BY container_id
            ORDER BY timestamp_utc ASC, reading_id ASC
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS rolling_6h_avg_temp_c,
    ROUND(
        AVG(humidity_pct) OVER (
            PARTITION BY container_id
            ORDER BY timestamp_utc ASC, reading_id ASC
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS rolling_6h_avg_humidity_pct,
    ROUND(
        AVG(vibration_g) OVER (
            PARTITION BY container_id
            ORDER BY timestamp_utc ASC, reading_id ASC
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS rolling_6h_avg_vibration_g
FROM base
