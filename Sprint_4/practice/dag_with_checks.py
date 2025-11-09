from datetime import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.sql import SQLCheckOperator, SQLValueCheckOperator

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "conn_id": "postgres_default",  # inherited by SQL* operators, fixes KeyError
}

with DAG(
    dag_id="Sprin4_Task1",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    # template_searchpath=["/opt/airflow/dags/sql"],  # uncomment if .sql stored elsewhere
) as dag:

    # NULL checks (№2)
    sql_check = SQLCheckOperator(
        task_id="user_order_log_isNull",
        sql="user_order_log_isNull_check.sql",
    )
    sql_check2 = SQLCheckOperator(
        task_id="user_activity_log_isNull",
        sql="user_activity_log_isNull_check.sql",
    )

    # Value checks (№3)
    sql_check3 = SQLValueCheckOperator(
        task_id="check_row_count_user_order_log",
        sql="Select count(distinct(customer_id)) from user_order_log",
        pass_value=3,
        tolerance=0.01,
    )
    sql_check4 = SQLValueCheckOperator(
        task_id="check_row_count_user_activity_log",
        sql="Select count(distinct(customer_id)) from user_activity_log",
        pass_value=3,
        tolerance=0.01,
    )

    sql_check >> sql_check2 >> sql_check3 >> sql_check4
