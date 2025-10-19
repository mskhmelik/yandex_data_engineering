INSERT INTO mart.f_sales (
    date_id,
    item_id,
    customer_id,
    city_id,
    quantity,
    payment_amount,
    status
)
SELECT
    dc.date_id,
    uol.item_id,
    uol.customer_id,
    uol.city_id,
    -- Adjust quantity and payment_amount for refunded orders
    CASE
        WHEN COALESCE(uol.status, 'shipped') = 'refunded'
            THEN -ABS(uol.quantity)
        ELSE  ABS(uol.quantity)
    END AS quantity,
    CASE
        WHEN COALESCE(uol.status, 'shipped') = 'refunded'
            THEN -ABS(uol.payment_amount)
        ELSE  ABS(uol.payment_amount)
    END AS payment_amount,
    -- Use 'shipped' as default status
    COALESCE(uol.status, 'shipped') AS status
FROM staging.user_order_log uol
LEFT JOIN mart.d_calendar AS dc
  ON CAST(uol.date_time AS date) = dc.date_actual
WHERE CAST(uol.date_time AS date) = '{{ ds }}';