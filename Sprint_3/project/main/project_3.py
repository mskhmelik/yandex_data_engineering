"""
Sprint 3 ETL: API -> staging -> marts

This DAG:
1) Triggers report creation, polls for completion, and requests a daily increment.
2) Loads cleaned increment data into staging (idempotent for the given {{ ds }}).
3) Refreshes marts: dimensions -> f_sales (upsert, refunds handled) -> f_customer_retention (weekly).
"""

import io
import json
import time
from datetime import timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# =============================================================================
# CONFIG
# =============================================================================

NICKNAME = "mskhmelik"
COHORT = "42"
HTTP_CONN_ID = "http_conn_id"
PG_CONN_ID = "postgresql_de"
BUSINESS_DT = "{{ ds }}"  # templated date string: YYYY-MM-DD

DEFAULT_ARGS = {
    "owner": NICKNAME,
    "email": ["student@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# =============================================================================
# HELPERS
# =============================================================================

def _get_api() -> tuple[str, requests.Session]:
    """
    Build API base URL and an authenticated requests.Session from the Airflow HTTP connection.

    Returns:
        tuple[str, requests.Session]: (base_url, session) where session has headers preset.

    Raises:
        ValueError: If the HTTP connection is missing the `api_key` in its Extra.
    """
    conn = HttpHook.get_connection(HTTP_CONN_ID)
    base = conn.host.rstrip("/")
    api_key = (conn.extra_dejson or {}).get("api_key")
    if not api_key:
        raise ValueError("Missing 'api_key' in HTTP connection extra.")
    headers = {
        "X-Nickname": NICKNAME,
        "X-Cohort": COHORT,
        "X-Project": "True",
        "X-API-KEY": api_key,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    s = requests.Session()
    s.headers.update(headers)
    return base, s


def _copy_df(df: pd.DataFrame, schema: str, table: str) -> int:
    """
    Load a DataFrame into Postgres via COPY FROM STDIN (fast path).

    Args:
        df (pd.DataFrame): Data to load.
        schema (str): Target schema name.
        table (str): Target table name.

    Returns:
        int: Number of rows inserted.
    """
    hook = PostgresHook(PG_CONN_ID)
    cols = ",".join([f'"{c}"' for c in df.columns])
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False, header=False)
    csv_buf.seek(0)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.copy_expert(f'COPY "{schema}"."{table}" ({cols}) FROM STDIN WITH CSV', csv_buf)
        conn.commit()
    return len(df)

# =============================================================================
# DAG
# =============================================================================

@dag(
    dag_id="sales_mart_main_dag",
    description="Sprint3 ETL: API -> staging -> marts (idempotent, refunds-aware)",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(7),
    catchup=True,
    max_active_runs=1,
    tags=["yandex", "sprint3", "etl"],
)
def sales_mart_main_dag():
    # -------------------------------------------------------------------------
    # EXTRACT TASKS
    # -------------------------------------------------------------------------

    @task(task_id="generate_report")
    def t_generate_report() -> str:
        """
        Trigger generation of the full report on the source API.

        Returns:
            str: task_id issued by the API.
        """
        base, s = _get_api()
        r = s.post(f"{base}/generate_report")
        r.raise_for_status()
        return r.json()["task_id"]

    @task(task_id="get_report")
    def t_get_report(task_id: str) -> str:
        """
        Poll the API for report completion and return the report_id.

        Args:
            task_id (str): The task id obtained from generate_report().

        Returns:
            str: report_id for the completed report.

        Raises:
            TimeoutError: If the report does not complete within the retry window.
            ValueError: If the API returns success without a report_id.
        """
        base, s = _get_api()
        backoff = 3.0
        for _ in range(20):
            r = s.get(f"{base}/get_report", params={"task_id": task_id})
            r.raise_for_status()
            payload = r.json()
            if payload.get("status") == "SUCCESS":
                rid = (payload.get("data") or {}).get("report_id")
                if not rid:
                    raise ValueError(f"SUCCESS but no report_id: {payload}")
                return rid
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 30.0)
        raise TimeoutError("Report not ready after retries.")

    @task(task_id="get_increment")
    def t_get_increment(report_id: str, date: str) -> str:
        """
        Request an incremental dataset for the specified date.

        Args:
            report_id (str): The report id produced by get_report().
            date (str): Processing date in YYYY-MM-DD format (templated {{ ds }}).

        Returns:
            str: increment_id for the date.

        Raises:
            ValueError: If the increment was not generated/found.
        """
        base, s = _get_api()
        r = s.get(f"{base}/get_increment", params={"report_id": report_id, "date": f"{date}T00:00:00"})
        r.raise_for_status()
        inc = (r.json().get("data") or {}).get("increment_id")
        if not inc:
            raise ValueError("Increment not found or empty.")
        return inc

    # -------------------------------------------------------------------------
    # LOAD TO STAGING
    # -------------------------------------------------------------------------

    @task(task_id="upload_user_order_inc")
    def t_upload_user_order_inc(increment_id: str, date: str) -> str:
        """
        Download, clean, and load the user_order_log increment into staging.

        Steps:
            1) Download CSV from Yandex Object Storage for the increment.
            2) Clean columns: drop legacy 'id', de-dup by 'uniq_id', default 'status' to 'shipped'.
            3) Delete same-day rows from staging for idempotency.
            4) COPY into staging.user_order_log.

        Args:
            increment_id (str): Increment id returned by get_increment().
            date (str): Processing date in YYYY-MM-DD format.

        Returns:
            str: A short message about loaded row count.
        """
        # 1) Download CSV
        s3_url = (
            f"https://storage.yandexcloud.net/s3-sprint3/"
            f"cohort_{COHORT}/{NICKNAME}/project/{increment_id}/user_order_log_inc.csv"
        )
        local = f"{date.replace('-', '')}_user_order_log_inc.csv"
        r = requests.get(s3_url, timeout=120)
        r.raise_for_status()
        with open(local, "wb") as f:
            f.write(r.content)

        # 2) Clean
        df = pd.read_csv(local)
        if "id" in df.columns:
            df = df.drop(columns=["id"])
        if "uniq_id" in df.columns:
            df = df.drop_duplicates(subset=["uniq_id"])
        if "status" not in df.columns:
            df["status"] = "shipped"

        # 3) Idempotent delete for the same business date
        hook = PostgresHook(PG_CONN_ID)
        hook.run(f"DELETE FROM staging.user_order_log WHERE date_time::date = '{date}';")

        # 4) Load
        inserted = _copy_df(df, schema="staging", table="user_order_log")
        return f"{inserted} rows -> staging.user_order_log"

    # -------------------------------------------------------------------------
    # MARTS (SQL OPERATORS)
    # -------------------------------------------------------------------------

    update_d_item = PostgresOperator(
        task_id="update_d_item",
        postgres_conn_id=PG_CONN_ID,
        sql="sql/mart.d_item.sql",
    )

    update_d_customer = PostgresOperator(
        task_id="update_d_customer",
        postgres_conn_id=PG_CONN_ID,
        sql="sql/mart.d_customer.sql",
    )

    update_d_city = PostgresOperator(
        task_id="update_d_city",
        postgres_conn_id=PG_CONN_ID,
        sql="sql/mart.d_city.sql",
    )

    update_f_sales = PostgresOperator(
        task_id="update_f_sales",
        postgres_conn_id=PG_CONN_ID,
        sql="sql/mart.f_sales.sql",   # upsert + refunds-aware
    )

    update_retention = PostgresOperator(
        task_id="update_f_customer_retention",
        postgres_conn_id=PG_CONN_ID,
        sql="sql/mart.f_customer_retention.sql",
    )

    # -------------------------------------------------------------------------
    # ORCHESTRATION
    # -------------------------------------------------------------------------

    task_id = t_generate_report()
    report_id = t_get_report(task_id)
    increment_id = t_get_increment(report_id, BUSINESS_DT)
    loaded = t_upload_user_order_inc(increment_id, BUSINESS_DT)

    loaded >> [update_d_item, update_d_customer, update_d_city]
    [update_d_item, update_d_customer, update_d_city] >> update_f_sales
    update_f_sales >> update_retention


dag = main_dag()