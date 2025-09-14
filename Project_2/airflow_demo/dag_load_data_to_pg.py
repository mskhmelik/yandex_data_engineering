from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import List

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

DIR_DATA = Path.cwd() / "data"

dag = DAG(
    dag_id="load_data_to_pg",
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def load_file_to_pg(filename: str, pg_table: str, date_cols: List[str], conn_args) -> None:
    """
    Load one CSV from DIR_DATA into Postgres as stage.<pg_table>.

    Steps:
        1. Read the CSV with pandas, parsing the specified columns as datetime.
        2. Infer Postgres column types from pandas dtypes via to_pg_type.
        3. Ensure schema "stage" exists; create the table if it does not.
        4. Bulk-insert all rows with psycopg2.extras.execute_values.
        5. Commit the transaction.

    Args:
        1. filename: CSV file name under DIR_DATA, e.g. "customer_research.csv".
        2. pg_table: Target table name without schema, e.g. "customer_research".
        3. date_cols: List of column names to explicitly parse as datetime.
        4. conn_args: Airflow Connection object; uses host, port, login, password, schema (database name).

    Returns:
        1. None

    Raises:
        1. FileNotFoundError if the CSV is missing.
        2. psycopg2.Error for database connection or SQL issues.
    """
    csv_path = DIR_DATA / filename
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    # Read CSV and parse date columns
    df = pd.read_csv(csv_path, parse_dates=date_cols)

    def to_pg_type(dtype) -> str:
        """
        Map a pandas dtype to a Postgres column type.

        Args:
        1. dtype: pandas/numpy dtype (e.g., int64, float64, bool, datetime64[ns], object).

        Returns:
        1. A Postgres type name as a string:
        - INTEGER for integer dtypes.
        - DOUBLE PRECISION for float dtypes.
        - BOOLEAN for bool dtypes.
        - TIMESTAMP for datetime/date dtypes.
        - TEXT for everything else (including object).
        """
        s = str(dtype)
        if "int" in s: return "INTEGER"
        if "float" in s: return "DOUBLE PRECISION"
        if "bool" in s: return "BOOLEAN"
        if "datetime" in s or "date" in s: return "TIMESTAMP"
        return "TEXT"

    # Build CREATE TABLE statement dynamically
    col_defs = ", ".join(f"{col} {to_pg_type(dt)}" for col, dt in df.dtypes.items())
    cols_csv = ", ".join(df.columns)

    stmt_create_schema = "CREATE SCHEMA IF NOT EXISTS stage"
    stmt_create_table = f"CREATE TABLE IF NOT EXISTS stage.{pg_table} ({col_defs})"
    stmt_insert = f"INSERT INTO stage.{pg_table} ({cols_csv}) VALUES %s"

    # Replace NaN/NaT with None so psycopg2 inserts NULL
    df_for_insert = df.where(pd.notnull(df), None)
    rows = list(df_for_insert.itertuples(index=False, name=None))

    # Connect and execute
    with psycopg2.connect(
        host=conn_args.host,
        port=conn_args.port,
        user=conn_args.login,
        password=conn_args.password,
        dbname=conn_args.schema,   # Airflow's 'schema' is the DB name
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt_create_schema)
            cur.execute(stmt_create_table)
            if rows:
                execute_values(cur, stmt_insert, rows)
        conn.commit()
        print(f"Inserted {len(rows)} rows into stage.{pg_table}")

pg_conn_args = BaseHook.get_connection("pg_connection")

t_load_customer_research = PythonOperator(
    task_id="load_customer_research_to_stage",
    python_callable=load_file_to_pg,
    op_kwargs={
        "filename": "customer_research.csv",
        "pg_table": "customer_research",
        "date_cols": ["date_id"],   # parse this column as datetime
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
        "date_cols": ["date_time"],  # parse this column as datetime
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
        "date_cols": ["date_time"],  # parse this column as datetime
        "conn_args": pg_conn_args,
    },
    dag=dag,
)

[t_load_customer_research, t_load_user_activity_log, t_load_user_order_log]
