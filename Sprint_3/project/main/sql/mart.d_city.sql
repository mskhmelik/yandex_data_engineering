-- mart.d_city — full rebuild from staging

BEGIN;

CREATE SCHEMA IF NOT EXISTS mart;

-- Drop and recreate the table for a clean refresh
DROP TABLE IF EXISTS mart.d_city CASCADE;

CREATE TABLE mart.d_city AS
SELECT DISTINCT
    city_id::BIGINT       AS city_id,
    TRIM(city_name)       AS city_name,
    now()::timestamptz    AS created_at
FROM staging.user_order_log
WHERE city_id IS NOT NULL
  AND city_name IS NOT NULL;

-- Add constraints and indexes
ALTER TABLE mart.d_city
    ADD PRIMARY KEY (city_id);

CREATE INDEX IF NOT EXISTS ix_d_city_name ON mart.d_city (city_name);

COMMIT;
