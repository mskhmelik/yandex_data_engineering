import datetime
import os
from pathlib import Path

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

# Local folder to save downloaded files
DATA_DIR = Path.cwd() / "data"

def download_from_s3(file_names):
    """
    Read CSVs over HTTP and write them to ./data/.
    Skip download if the file already exists.
    Returns a list of saved file paths.
    """
    base_url = r"https://storage.yandexcloud.net/s3-sprint3-static/lessons/"
    os.makedirs(DATA_DIR, exist_ok=True)

    saved_paths = []
    for name in file_names:
        out_path = DATA_DIR / name

        if out_path.exists():
            print(f"File already exists, skipping: {out_path}")
            saved_paths.append(str(out_path))
            continue

        url = base_url + name
        df = pd.read_csv(url)
        df.to_csv(out_path, index=False)
        saved_paths.append(str(out_path))
        print(f"Downloaded and saved: {out_path}")

    print(f"Total files present: {len(saved_paths)}")
    return saved_paths

dag = DAG(
    dag_id="get_data_from_s3",
    schedule_interval="0 0 * * *",           # every day at midnight
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

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
    dag=dag,
)

t_download_from_s3