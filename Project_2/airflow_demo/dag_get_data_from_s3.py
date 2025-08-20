import datetime
import os

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = Path.cwd() / "data"

dag = DAG(
    dag_id="get_data_from_s3",
    schedule_interval="0 0 * * *",
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def download_from_s3(file_names):
    base_url = r"https://storage.yandexcloud.net/s3-sprint3-static/lessons/"
    out_dir = DIR_DATA
    os.makedirs(out_dir, exist_ok=True)

    saved_paths = []
    for name in file_names:
        df = pd.read_csv(base_url + name)
        out_path = os.path.join(out_dir, name)
        df.to_csv(out_path, index=False)
        saved_paths.append(out_path)

    return saved_paths

t_download_from_s3 = PythonOperator(
    task_id="download_from_s3",
    python_callable=download_from_s3,
    op_kwargs={
        "file_names": [
            "customer_research.csv",
            "user_activity_log.csv",
            "user_order_log.csv",
        ]
    },
    dag=dag
)

t_download_from_s3
