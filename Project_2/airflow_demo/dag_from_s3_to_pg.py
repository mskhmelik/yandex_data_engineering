import datetime
import time
import psycopg2

import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

### API settings ###
api_conn = BaseHook.get_connection('create_files_api')
api_endpoint = api_conn.host
api_token = api_conn.password

# set user constants for api
nickname = "mskhmelik"
cohort = "42"  # e.g. "13"

headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}

### POSTGRESQL settings ###
psql_conn = BaseHook.get_connection('pg_connection')
# init test connection
_conn = psycopg2.connect(
    f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'"
)
_cur = _conn.cursor()
_cur.close()
_conn.close()

# 1) Запрос генерации отчёта
def create_files_request(ti, api_endpoint, headers):
    method_url = '/generate_report'
    r = requests.post('https://' + api_endpoint + method_url, headers=headers)
    response_dict = json.loads(r.content)
    ti.xcom_push(key='task_id', value=response_dict['task_id'])
    print(f"task_id is {response_dict['task_id']}")
    return response_dict['task_id']

# 2) Ожидание готовности и получение report_id
def check_report(ti, api_endpoint, headers):
    task_ids = ti.xcom_pull(key='task_id', task_ids=['create_files_request'])
    task_id = task_ids[0]

    method_url = '/get_report'
    payload = {'task_id': task_id}
    report_id = None

    for i in range(4):
        time.sleep(70)
        r = requests.get('https://' + api_endpoint + method_url, params=payload, headers=headers)
        response_dict = json.loads(r.content)
        print(i, response_dict.get('status'))
        if response_dict.get('status') == 'SUCCESS':
            report_id = response_dict['data']['report_id']
            break

    if report_id is None:
        raise RuntimeError("Report was not ready after 4 checks (≈280s).")

    ti.xcom_push(key='report_id', value=report_id)
    print(f"report_id is {report_id}")
    return report_id

# 3) Загрузка 3 файлов из S3 в stage-таблицы
def upload_from_s3_to_pg(ti, nickname, cohort):
    report_ids = ti.xcom_pull(key='report_id', task_ids=['check_ready_report'])
    report_id = report_ids[0]

    storage_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT_NUMBER}/{NICKNAME}/{REPORT_ID}/{FILE_NAME}'
    personal_storage_url = (
        storage_url
        .replace("{COHORT_NUMBER}", cohort)
        .replace("{NICKNAME}", nickname)
        .replace("{REPORT_ID}", report_id)
    )

    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(
        f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'"
    )
    cur = conn.cursor()

    # customer_research
    df_customer_research = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "customer_research.csv"))
    df_customer_research.reset_index(drop=True, inplace=True)
    insert_cr = "insert into stage.customer_research (date_id,category_id,geo_id,sales_qty,sales_amt) VALUES {cr_val};"
    i = 0
    step = max(1, int(df_customer_research.shape[0] / 100))
    while i <= df_customer_research.shape[0]:
        print('df_customer_research', i, end='\r')
        batch = df_customer_research.loc[i:i + step]
        if batch.shape[0] > 0:
            cr_val = str([tuple(x) for x in batch.to_numpy()])[1:-1]
            cur.execute(insert_cr.replace('{cr_val}', cr_val))
            conn.commit()
        i += step + 1

    # user_order_log
    df_order_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_order_log.csv"))
    df_order_log.reset_index(drop=True, inplace=True)
    insert_uol = "insert into stage.user_order_log (date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount) VALUES {uol_val};"
    i = 0
    step = max(1, int(df_order_log.shape[0] / 100))
    while i <= df_order_log.shape[0]:
        print('df_order_log', i, end='\r')
        batch = df_order_log.drop(columns=['id'], axis=1).loc[i:i + step]
        if batch.shape[0] > 0:
            uol_val = str([tuple(x) for x in batch.to_numpy()])[1:-1]
            cur.execute(insert_uol.replace('{uol_val}', uol_val))
            conn.commit()
        i += step + 1

    # user_activity_log
    df_activity_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_activity_log.csv"))
    df_activity_log.reset_index(drop=True, inplace=True)
    insert_ual = "insert into stage.user_activity_log (date_time, action_id, customer_id, quantity) VALUES {ual_val};"
    i = 0
    step = max(1, int(df_activity_log.shape[0] / 100))
    while i <= df_activity_log.shape[0]:
        print('df_activity_log', i, end='\r')
        batch = df_activity_log.drop(columns=['id'], axis=1).loc[i:i + step]
        if batch.shape[0] > 0:
            ual_val = str([tuple(x) for x in batch.to_numpy()])[1:-1]
            cur.execute(insert_ual.replace('{ual_val}', ual_val))
            conn.commit()
        i += step + 1

    cur.close()
    conn.close()
    return 200

# 4) Обновление измерений (d_*)
def update_mart_d_tables(ti):
    sql_clear_facts = """
    DELETE FROM mart.f_activity;
    DELETE FROM mart.f_daily_sales;
    """
    sql_d_calendar = """
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
    dedup_dates AS (SELECT DISTINCT fact_date FROM source_dates),
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
    sql_d_customer = """
    DELETE FROM mart.d_customer;
    WITH source_orders AS (
        SELECT uol.customer_id AS customer_id, uol.city_id AS city_id
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
    sql_d_item = """
    DELETE FROM mart.d_item;
    WITH source_items AS (
        SELECT uol.item_id, uol.item_name
        FROM stage.user_order_log uol
        WHERE uol.item_id IS NOT NULL AND uol.item_name IS NOT NULL
    ),
    dedup_items AS (SELECT DISTINCT item_id, item_name FROM source_items)
    INSERT INTO mart.d_item (item_id, item_name)
    SELECT item_id, item_name
    FROM dedup_items
    ORDER BY item_id;
    """

    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(
        f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'"
    )
    cur = conn.cursor()
    cur.execute(sql_clear_facts); conn.commit()
    cur.execute(sql_d_calendar);  conn.commit()
    cur.execute(sql_d_customer);  conn.commit()
    cur.execute(sql_d_item);      conn.commit()
    cur.close()
    conn.close()
    return 200

# 5) Обновление витрин (f_*)
def update_mart_f_tables(ti):
    sql_f_activity = """
    DELETE FROM mart.f_activity;
    WITH
    source_actions AS (
        SELECT ual.action_id AS activity_id, DATE(ual.date_time) AS fact_date
        FROM stage.user_activity_log ual
        WHERE ual.action_id IS NOT NULL AND ual.date_time IS NOT NULL
    ),
    actions_with_date_id AS (
        SELECT sa.activity_id, dc.date_id
        FROM source_actions sa
        JOIN mart.d_calendar dc ON dc.fact_date = sa.fact_date
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
    sql_f_daily_sales = """
    DELETE FROM mart.f_daily_sales;
    WITH
    source_orders AS (
        SELECT DATE(uol.date_time) AS fact_date, uol.item_id, uol.customer_id, uol.quantity, uol.payment_amount
        FROM stage.user_order_log uol
        WHERE uol.date_time IS NOT NULL AND uol.item_id IS NOT NULL AND uol.customer_id IS NOT NULL
    ),
    orders_agg AS (
        SELECT
            fact_date, item_id, customer_id,
            AVG(payment_amount / NULLIF(quantity, 0.0)) AS price,
            SUM(quantity) AS quantity,
            SUM(payment_amount) AS payment_amount
        FROM source_orders
        GROUP BY fact_date, item_id, customer_id
    ),
    orders_with_date_id AS (
        SELECT dc.date_id, oa.item_id, oa.customer_id, oa.price, oa.quantity, oa.payment_amount
        FROM orders_agg oa
        JOIN mart.d_calendar dc ON dc.fact_date = oa.fact_date
    )
    INSERT INTO mart.f_daily_sales (date_id, item_id, customer_id, price, quantity, payment_amount)
    SELECT date_id, item_id, customer_id, price, quantity, payment_amount
    FROM orders_with_date_id
    ORDER BY date_id, item_id, customer_id;
    """

    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(
        f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'"
    )
    cur = conn.cursor()
    cur.execute(sql_f_activity)
    conn.commit()
    cur.execute(sql_f_daily_sales)
    conn.commit()
    cur.close()
    conn.close()
    return 200

# объявление DAG
dag = DAG(
    dag_id='full_pipeline_from_s3_to_pg',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

# Tasks
t_file_request = PythonOperator(
    task_id='create_files_request',
    python_callable=create_files_request,
    op_args=[api_endpoint, headers],
    dag=dag
)

t_check_report = PythonOperator(
    task_id='check_ready_report',
    python_callable=check_report,
    op_args=[api_endpoint, headers],
    dag=dag
)

t_upload_from_s3_to_pg = PythonOperator(
    task_id='upload_from_s3_to_pg',
    python_callable=upload_from_s3_to_pg,
    op_args=[nickname, cohort],
    dag=dag
)

t_update_mart_d_tables = PythonOperator(
    task_id='update_mart_d_tables',
    python_callable=update_mart_d_tables,
    dag=dag
)

t_update_mart_f_tables = PythonOperator(
    task_id='update_mart_f_tables',
    python_callable=update_mart_f_tables,
    dag=dag
)

# Chain
t_file_request >> t_check_report >> t_upload_from_s3_to_pg >> t_update_mart_d_tables >> t_update_mart_f_tables
