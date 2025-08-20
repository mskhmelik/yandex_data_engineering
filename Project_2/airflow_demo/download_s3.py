import os
import pandas as pd

def download_from_s3(file_names):
    base_url = "https://storage.yandexcloud.net/s3-sprint3-static/lessons/"
    out_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(out_dir, exist_ok=True)

    saved_paths = []
    for name in file_names:
        df = pd.read_csv(base_url + name)
        out_path = os.path.join(out_dir, name)
        df.to_csv(out_path, index=False)
        saved_paths.append(out_path)

    return saved_paths


if __name__ == "__main__":
    files = [
        "customer_research.csv",
        "user_activity_log.csv",
        "user_order_log.csv",
    ]
    result = download_from_s3(files)
    print("Saved files:", result)
