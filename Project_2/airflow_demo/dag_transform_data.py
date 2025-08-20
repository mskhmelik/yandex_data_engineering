from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='552_postgresql_export_fuction',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    params={"example_key": "example_value"},
)

def load_file_to_pg(filename, pg_table, conn_args):
    # 1) читаем CSV из сохранённой папки
    df = pd.read_csv(
        f"/lessons/5. Реализация ETL в Airflow/4. Extract как подключиться к хранилищу, чтобы получить файл/Задание 2/{filename}",
        index_col=0  # убираем возможную колонку Unnamed: 0
    )

    # 2) формируем SQL INSERT
    cols = ','.join(list(df.columns))
    insert_stmt = f"INSERT INTO stage.{pg_table} ({cols}) VALUES %s"

    # 3) подключаемся к Postgres на основе conn_args
    pg_conn = psycopg2.connect(
        host=conn_args.host,
        port=conn_args.port,
        user=conn_args.login,
        password=conn_args.password,
        dbname=conn_args.schema  # важно: dbname='de'
    )
    cur = pg_conn.cursor()

    # 4) вставка батчем
    rows = list(df.itertuples(index=False, name=None))
    psycopg2.extras.execute_values(cur, insert_stmt, rows)
    pg_conn.commit()

    cur.close()
    pg_conn.close()

# получаем параметры соединения из Airflow Connections
pg_conn_args = BaseHook.get_connection('pg_connection')

t_load_customer_research = PythonOperator(
    task_id='load_customer_research_to_stage',
    python_callable=load_file_to_pg,
    op_kwargs={
        'filename': 'customer_research.csv',
        'pg_table': 'customer_research',
        'conn_args': pg_conn_args
    },
    dag=dag
)

t_load_user_activity_log = PythonOperator(
    task_id='load_user_activity_log_to_stage',
    python_callable=load_file_to_pg,
    op_kwargs={
        'filename': 'user_activity_log.csv',
        'pg_table': 'user_activity_log',
        'conn_args': pg_conn_args
    },
    dag=dag
)

t_load_user_order_log = PythonOperator(
    task_id='load_user_order_log_to_stage',
    python_callable=load_file_to_pg,
    op_kwargs={
        'filename': 'user_order_log.csv',
        'pg_table': 'user_order_log',
        'conn_args': pg_conn_args
    },
    dag=dag
)

# при необходимости можно выставить порядок
t_load_customer_research >> [t_load_user_activity_log, t_load_user_order_log]