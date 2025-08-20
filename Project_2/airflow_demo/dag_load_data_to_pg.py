from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="transform_data",
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def load_file_to_pg(filename: str, pg_table: str, conn_args) -> None:
    """
    Load a CSV file into Postgres stage schema.
    """
    
    df = pd.read_csv(Path.cwd() / "data" / filename, index_col=0)

    cols = ",".join(list(df.columns))
    insert_stmt = f"INSERT INTO stage.{pg_table} ({cols}) VALUES %s"

    conn = psycopg2.connect(
        host=conn_args.host,
        port=conn_args.port,
        user=conn_args.login,
        password=conn_args.password,
        dbname=conn_args.schema,
    )

    try:
        with conn, conn.cursor() as cur:
            rows = list(df.itertuples(index=False, name=None))
            psycopg2.extras.execute_values(cur, insert_stmt, rows)
    finally:
        conn.close()

pg_conn_args = BaseHook.get_connection("pg_connection")

t_load_customer_research = PythonOperator(
    task_id="load_customer_research_to_stage",
    python_callable=load_file_to_pg,
    op_kwargs={
        "filename": "customer_research.csv",
        "pg_table": "customer_research",
        "conn_args": pg_conn_args,
    },
    dag=dag,
)

t_load_user_activity_log = PythonOperator(
    task_id="load_user_activity_log_to_stage",
    python_callable=load_file_to_pg,
    op_kwargs={
        "filename": "user_activity_log.csv",
        "pg_table": "user_activity_log",
        "conn_args": pg_conn_args,
    },
    dag=dag,
)

t_load_user_order_log = PythonOperator(
    task_id="load_user_order_log_to_stage",
    python_callable=load_file_to_pg,
    op_kwargs={
        "filename": "user_order_log.csv",
        "pg_table": "user_order_log",
        "conn_args": pg_conn_args,
    },
    dag=dag,
)

t_load_customer_research >> [t_load_user_activity_log, t_load_user_order_log]