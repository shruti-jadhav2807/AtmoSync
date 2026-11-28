-- AtmoSync Phase 1: Safe schema hardening for public.sensor_readings
-- Preserves existing table, identity primary key (reading_id), and existing rows.

DO $$
DECLARE
    dup_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO dup_count
    FROM (
        SELECT container_id, timestamp_utc
        FROM public.sensor_readings
        GROUP BY container_id, timestamp_utc
        HAVING COUNT(*) > 1
    ) AS duplicates;

    IF dup_count > 0 THEN
        RAISE EXCEPTION 'Cannot add UNIQUE(container_id, timestamp_utc): found % duplicate key group(s)', dup_count;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.sensor_readings'::regclass
          AND conname = 'uq_sensor_readings_container_timestamp'
    ) THEN
        ALTER TABLE public.sensor_readings
            ADD CONSTRAINT uq_sensor_readings_container_timestamp
            UNIQUE (container_id, timestamp_utc);
    END IF;
END
$$;

-- Composite time-series index optimized for container + descending timestamp queries,
-- complementing the unique constraint index on (container_id, timestamp_utc).
CREATE INDEX IF NOT EXISTS idx_sensor_readings_container_ts_desc
    ON public.sensor_readings (container_id, timestamp_utc DESC);
