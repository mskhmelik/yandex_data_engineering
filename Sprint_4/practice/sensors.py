from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow"
}

with DAG(
    dag_id="Sprin4_Task1",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:

    start = DummyOperator(task_id="Start")

    waiting_for_file = FileSensor(
        task_id="waiting_for_test_file",
        fs_conn_id="fs_local",
        filepath="/data/test.txt",
        poke_interval=10
    )

    finish = DummyOperator(task_id="Finish")

    start >> waiting_for_file >> finish
