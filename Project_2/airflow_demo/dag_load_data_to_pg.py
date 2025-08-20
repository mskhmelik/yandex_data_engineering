from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

DATA_DIR = Path.cwd() / "data"

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
    
    # Get data types and create column definitions
    cols = ",".join(list(df.columns))
    col_definition = []
    for col, dtype in df.dtypes.items():
        if "int" in str(dtype):
            pg_type = "INTEGER"
        elif "float" in str(dtype):
            pg_type = "DOUBLE PRECISION"
        elif "bool" in str(dtype):
            pg_type = "BOOLEAN"
        elif "datetime" in str(dtype):
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"
        col_definition.append(f"{col} {pg_type}")

    # Statements
    stmt_create = (
        f"""
        CREATE TABLE IF NOT EXISTS stage.{pg_table} {', '.join(col_definition)}
        """
    )
    stmt_insert = f"INSERT INTO stage.{pg_table} ({cols}) VALUES %s"
    
    # Connect to Postgres using connection arguments
    conn_pg = psycopg2.connect(
        host=conn_args.host,
        port=conn_args.port,
        user=conn_args.login,
        password=conn_args.password,
        dbname=conn_args.schema,
    )
    cur = conn_pg.cursor()
    
    # Write data row-by-row to Postgres
    rows = list(df.itertuples(index=False, name=None))
    psycopg2.extras.execute_values(cur, stmt_insert, rows)
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

[t_load_customer_research, t_load_user_activity_log, t_load_user_order_log]