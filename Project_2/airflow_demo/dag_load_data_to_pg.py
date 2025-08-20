from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

DIR_DATA = Path.cwd() / "data"

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
    # Read the CSV file into a DataFrame and prepare for insertion
    df = pd.read_csv(DIR_DATA / filename, index_col=0)
    
    # Map pandas dtypes to Postgres
    def to_pg_type(dtype) -> str:
        s = str(dtype)
        if "int" in s:
            return "INTEGER"
        if "float" in s:
            return "DOUBLE PRECISION"
        if "bool" in s:
            return "BOOLEAN"
        if "datetime" in s or "date" in s:
            return "TIMESTAMP"
        return "TEXT"

    col_defs = ", ".join(f"{col} {to_pg_type(dt)}" for col, dt in df.dtypes.items())
    cols_csv = ", ".join(df.columns)

    stmt_create_schema = "CREATE SCHEMA IF NOT EXISTS stage"
    stmt_create_table = f"CREATE TABLE IF NOT EXISTS stage.{pg_table} ({col_defs})"
    stmt_insert = f"INSERT INTO stage.{pg_table} ({cols_csv}) VALUES %s"

    rows = list(df.itertuples(index=False, name=None))
    
    # Connect to Postgres using connection arguments
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt_create_schema)
            cur.execute(stmt_create_table)
            if rows:
                execute_values(cur, stmt_insert, rows)
        conn.commit()

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

[t_load_customer_research, t_load_user_activity_log, t_load_user_order_log]