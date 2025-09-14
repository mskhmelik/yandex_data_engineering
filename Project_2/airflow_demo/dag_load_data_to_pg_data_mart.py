from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import psycopg2

dag = DAG(
    dag_id="load_data_to_pg_data_mart",
    schedule_interval="0 0 * * *",
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
)

# 1. Get Airflow connection once
pg_conn_args = BaseHook.get_connection("pg_connection")

# 2. SQL blocks (same as before)
SQL_D_CALENDAR = """
DELETE FROM mart.d_calendar;
WITH
source_orders AS (
    SELECT DATE(uol.date_time) AS fact_date
    FROM stage.user_order_log uol
    WHERE uol.date_time IS NOT NULL
),
source_activity AS (
    SELECT DATE(ual.date_time) AS fact_date
    FROM stage.user_activity_log ual
    WHERE ual.date_time IS NOT NULL
),
source_research AS (
    SELECT DATE(cr.date_id) AS fact_date
    FROM stage.customer_research cr
    WHERE cr.date_id IS NOT NULL
),
source_dates AS (
    SELECT fact_date FROM source_orders
    UNION ALL
    SELECT fact_date FROM source_activity
    UNION ALL
    SELECT fact_date FROM source_research
),
dedup_dates AS (
    SELECT DISTINCT fact_date FROM source_dates
),
calendar_enriched AS (
    SELECT
        TO_CHAR(d.fact_date, 'YYYYMMDD')::INT AS date_id,
        d.fact_date                           AS fact_date,
        EXTRACT(DAY   FROM d.fact_date)::INT  AS day_num,
        EXTRACT(MONTH FROM d.fact_date)::INT  AS month_num,
        TO_CHAR(d.fact_date, 'FMMonth')       AS month_name,
        EXTRACT(YEAR  FROM d.fact_date)::INT  AS year_num
    FROM dedup_dates d
)
INSERT INTO mart.d_calendar (date_id, fact_date, day_num, month_num, month_name, year_num)
SELECT date_id, fact_date, day_num, month_num, month_name, year_num
FROM calendar_enriched
ORDER BY fact_date;
"""

SQL_D_CUSTOMER = """
DELETE FROM mart.d_customer;
WITH
source_orders AS (
    SELECT
        uol.customer_id AS customer_id,
        uol.city_id     AS city_id
    FROM stage.user_order_log uol
    WHERE uol.customer_id IS NOT NULL
),
customer_city AS (
    SELECT customer_id, MAX(city_id) AS city_id
    FROM source_orders
    GROUP BY customer_id
)
INSERT INTO mart.d_customer (customer_id, city_id)
SELECT customer_id, city_id
FROM customer_city
ORDER BY customer_id;
"""

SQL_D_ITEM = """
DELETE FROM mart.d_item;
WITH
source_items AS (
    SELECT
        uol.item_id,
        uol.item_name
    FROM stage.user_order_log uol
    WHERE uol.item_id  IS NOT NULL
      AND uol.item_name IS NOT NULL
),
dedup_items AS (
    SELECT DISTINCT item_id, item_name
    FROM source_items
)
INSERT INTO mart.d_item (item_id, item_name)
SELECT item_id, item_name
FROM dedup_items
ORDER BY item_id;
"""

SQL_F_ACTIVITY = """
DELETE FROM mart.f_activity;
WITH
source_actions AS (
    SELECT
        ual.action_id       AS activity_id,
        DATE(ual.date_time) AS fact_date
    FROM stage.user_activity_log ual
    WHERE ual.action_id  IS NOT NULL
      AND ual.date_time IS NOT NULL
),
actions_with_date_id AS (
    SELECT sa.activity_id, dc.date_id
    FROM source_actions sa
    JOIN mart.d_calendar dc
      ON dc.fact_date = sa.fact_date
),
activity_agg AS (
    SELECT activity_id, date_id, COUNT(*) AS click_number
    FROM actions_with_date_id
    GROUP BY activity_id, date_id
)
INSERT INTO mart.f_activity (activity_id, date_id, click_number)
SELECT activity_id, date_id, click_number
FROM activity_agg
ORDER BY activity_id, date_id;
"""

SQL_F_DAILY_SALES = """
DELETE FROM mart.f_daily_sales;
WITH
source_orders AS (
    SELECT
        DATE(uol.date_time) AS fact_date,
        uol.item_id,
        uol.customer_id,
        uol.quantity,
        uol.payment_amount
    FROM stage.user_order_log uol
    WHERE uol.date_time   IS NOT NULL
      AND uol.item_id     IS NOT NULL
      AND uol.customer_id IS NOT NULL
),
orders_agg AS (
    SELECT
        fact_date,
        item_id,
        customer_id,
        AVG(payment_amount / NULLIF(quantity, 0.0)) AS price,
        SUM(quantity)                               AS quantity,
        SUM(payment_amount)                         AS payment_amount
    FROM source_orders
    GROUP BY fact_date, item_id, customer_id
),
orders_with_date_id AS (
    SELECT
        dc.date_id,
        oa.item_id,
        oa.customer_id,
        oa.price,
        oa.quantity,
        oa.payment_amount
    FROM orders_agg oa
    JOIN mart.d_calendar dc
      ON dc.fact_date = oa.fact_date
)
INSERT INTO mart.f_daily_sales (date_id, item_id, customer_id, price, quantity, payment_amount)
SELECT date_id, item_id, customer_id, price, quantity, payment_amount
FROM orders_with_date_id
ORDER BY date_id, item_id, customer_id;
"""

def update_mart_d_tables():
    """
    Run full refresh of mart dimension tables: d_calendar, d_customer, d_item.

    Steps:
        1. Open a Postgres connection using Airflow connection 'pg_connection'.
        2. Execute DELETE + INSERT script for d_calendar. Commit.
        3. Execute DELETE + INSERT script for d_customer. Commit.
        4. Execute DELETE + INSERT script for d_item. Commit.
        5. Close connection.
    """
    # Connect and execute
    with psycopg2.connect(
        host=pg_conn_args.host,
        port=pg_conn_args.port,
        user=pg_conn_args.login,
        password=pg_conn_args.password,
        dbname=pg_conn_args.schema,   # Airflow's 'schema' is the DB name
    ) as conn:
        with conn.cursor() as cur:
            # d_calendar
            cur.execute(SQL_D_CALENDAR)
            conn.commit()

            # d_customer
            cur.execute(SQL_D_CUSTOMER)
            conn.commit()

            # d_item
            cur.execute(SQL_D_ITEM)
            conn.commit()

    return 200

def update_mart_f_tables():
    """
    Run full refresh of mart fact tables: f_activity, f_daily_sales.

    Steps:
        1. Open a Postgres connection using Airflow connection 'pg_connection'.
        2. Execute DELETE + INSERT script for f_activity. Commit.
        3. Execute DELETE + INSERT script for f_daily_sales. Commit.
        4. Close connection.
    """
    # Connect and execute
    with psycopg2.connect(
        host=pg_conn_args.host,
        port=pg_conn_args.port,
        user=pg_conn_args.login,
        password=pg_conn_args.password,
        dbname=pg_conn_args.schema,   # Airflow's 'schema' is the DB name
    ) as conn:
        with conn.cursor() as cur:
            # f_activity
            cur.execute(SQL_F_ACTIVITY)
            conn.commit()

            # f_daily_sales
            cur.execute(SQL_F_DAILY_SALES)
            conn.commit()

    return 200

t_update_mart_d_tables = PythonOperator(
    task_id='update_mart_d_tables',
    python_callable=update_mart_d_tables,
    dag=dag,
)

t_update_mart_f_tables = PythonOperator(
    task_id='update_mart_f_tables',
    python_callable=update_mart_f_tables,
    dag=dag,
)

# Dimensions first, then facts
t_update_mart_d_tables >> t_update_mart_f_tables