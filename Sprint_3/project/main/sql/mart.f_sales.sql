-- Rebuild-friendly + idempotent load for a single {{ ds }}

BEGIN;

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.f_sales (
    uniq_id         text        PRIMARY KEY,          -- natural key from staging
    date_id         int         NOT NULL,
    item_id         bigint      NOT NULL,
    customer_id     bigint      NOT NULL,
    city_id         bigint      NOT NULL,
    status          text        NOT NULL CHECK (status IN ('shipped','refunded')),
    quantity        numeric(18,3) NOT NULL,
    payment_amount  numeric(18,2) NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Fast lookups
CREATE INDEX IF NOT EXISTS ix_f_sales_date_id  ON mart.f_sales(date_id);
CREATE INDEX IF NOT EXISTS ix_f_sales_item_id  ON mart.f_sales(item_id);
CREATE INDEX IF NOT EXISTS ix_f_sales_status   ON mart.f_sales(status);

WITH src AS (
    SELECT
        uol.uniq_id,
        dc.date_id,
        uol.item_id,
        uol.customer_id,
        uol.city_id,
        COALESCE(uol.status, 'shipped')                  AS status_norm,
        ABS(uol.quantity)                                AS q_abs,
        ABS(uol.payment_amount)                          AS pay_abs
    FROM staging.user_order_log uol
    JOIN mart.d_calendar dc
      ON CAST(uol.date_time AS date) = dc.date_actual
    WHERE CAST(uol.date_time AS date) = '{{ ds }}'
),
signed AS (
    SELECT
        uniq_id,
        date_id,
        item_id,
        customer_id,
        city_id,
        status_norm                                      AS status,
        -- make quantity and payment_amount negative for refunded records
        CASE WHEN status_norm = 'refunded' THEN -q_abs   ELSE q_abs  END AS quantity,
        CASE WHEN status_norm = 'refunded' THEN -pay_abs ELSE pay_abs END AS payment_amount
    FROM src
)
INSERT INTO mart.f_sales (
    uniq_id, date_id, item_id, customer_id, city_id, status, quantity, payment_amount
)
SELECT
    uniq_id, date_id, item_id, customer_id, city_id, status, quantity, payment_amount
FROM signed
ON CONFLICT (uniq_id) DO UPDATE
-- In case of conflict, update all fields except the primary key
SET
    date_id        = EXCLUDED.date_id,
    item_id        = EXCLUDED.item_id,
    customer_id    = EXCLUDED.customer_id,
    city_id        = EXCLUDED.city_id,
    status         = EXCLUDED.status,
    quantity       = EXCLUDED.quantity,
    payment_amount = EXCLUDED.payment_amount;

COMMIT;
