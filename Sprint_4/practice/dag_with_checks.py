from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import SQLCheckOperator, SQLValueCheckOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# === Callbacks ===
def check_success_insert_user_order_log(context):
    PostgresOperator(
        task_id="success_insert_user_order_log",
        sql="""
            INSERT INTO stage.dq_checks_results
            VALUES ('user_order_log','user_order_log_isNull', NOW(), 0);
        """,
    ).execute(context=context)

def check_failure_insert_user_order_log(context):
    PostgresOperator(
        task_id="failure_insert_user_order_log",
        sql="""
            INSERT INTO stage.dq_checks_results
            VALUES ('user_order_log','user_order_log_isNull', NOW(), 1);
        """,
    ).execute(context=context)

def check_success_insert_user_activity_log(context):
    PostgresOperator(
        task_id="success_insert_user_activity_log",
        sql="""
            INSERT INTO stage.dq_checks_results
            VALUES ('user_activity_log','user_activity_log_isNull', NOW(), 0);
        """,
    ).execute(context=context)

def check_failure_insert_user_activity_log(context):
    PostgresOperator(
        task_id="failure_insert_user_activity_log",
        sql="""
            INSERT INTO stage.dq_checks_results
            VALUES ('user_activity_log','user_activity_log_isNull', NOW(), 1);
        """,
    ).execute(context=context)

def check_success_insert_user_order_log2(context):
    PostgresOperator(
        task_id="success_insert_user_order_log2",
        sql="""
            INSERT INTO stage.dq_checks_results
            VALUES ('user_order_log','check_row_count_user_order_log', NOW(), 0);
        """,
    ).execute(context=context)

def check_failure_insert_user_order_log2(context):
    PostgresOperator(
        task_id="failure_insert_user_order_log2",
        sql="""
            INSERT INTO stage.dq_checks_results
            VALUES ('user_order_log','check_row_count_user_order_log', NOW(), 1);
        """,
    ).execute(context=context)

def check_success_insert_user_activity_log2(context):
    PostgresOperator(
        task_id="success_insert_user_activity_log2",
        sql="""
            INSERT INTO stage.dq_checks_results
            VALUES ('user_activity_log','check_row_count_user_activity_log', NOW(), 0);
        """,
    ).execute(context=context)

def check_failure_insert_user_activity_log2(context):
    PostgresOperator(
        task_id="failure_insert_user_activity_log2",
        sql="""
            INSERT INTO stage.dq_checks_results
            VALUES ('user_activity_log','check_row_count_user_activity_log', NOW(), 1);
        """,
    ).execute(context=context)


default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "conn_id": "postgres_default"
}

with DAG(
    dag_id="Sprin4_Task61",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:

    begin = DummyOperator(task_id="begin")

    sql_check = SQLCheckOperator(
        task_id="user_order_log_isNull",
        sql="user_order_log_isNull_check.sql",
        on_success_callback=check_success_insert_user_order_log,
        on_failure_callback=check_failure_insert_user_order_log,
    )

    sql_check2 = SQLCheckOperator(
        task_id="user_activity_log_isNull",
        sql="user_activity_log_isNull_check.sql",
        on_success_callback=check_success_insert_user_activity_log,
        on_failure_callback=check_failure_insert_user_activity_log,
    )

    sql_check3 = SQLValueCheckOperator(
        task_id="check_row_count_user_order_log",
        sql="Select count(distinct(customer_id)) from user_order_log",
        pass_value=3,
        tolerance=0.01,
        on_success_callback=check_success_insert_user_order_log2,
        on_failure_callback=check_failure_insert_user_order_log2,
    )

    sql_check4 = SQLValueCheckOperator(
        task_id="check_row_count_user_activity_log",
        sql="Select count(distinct(customer_id)) from user_activity_log",
        pass_value=3,
        tolerance=0.01,
        on_success_callback=check_success_insert_user_activity_log2,
        on_failure_callback=check_failure_insert_user_activity_log2,
    )

    end = DummyOperator(task_id="end")

    begin >> [sql_check, sql_check2, sql_check3, sql_check4] >> end

