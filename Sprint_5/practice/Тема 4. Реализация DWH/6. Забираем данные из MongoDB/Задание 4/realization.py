from pymongo import MongoClient
import ssl

# === fill these from Airflow Variables manually ===
MONGO_HOST = "rc1a-ba83ae33hvt4pokq.mdb.yandexcloud.net:27018"
MONGO_USER = "student"
MONGO_PASSWORD = "student1"
MONGO_DB = "db-mongo"
MONGO_RS = "rs01"
MONGO_CERT_PATH = "/opt/airflow/certificates/PracticumSp5MongoDb.crt"


def main():
    uri = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}/"
        f"?replicaSet={MONGO_RS}&authSource={MONGO_DB}&tls=true"
    )

    client = MongoClient(
        uri,
        tls=True,
        tlsCAFile=MONGO_CERT_PATH,
        serverSelectionTimeoutMS=5000,
    )

    print("Connected OK")

    print("\nDatabases:")
    for db in client.list_database_names():
        print(" -", db)

    print(f"\nCollections in database '{MONGO_DB}':")
    database = client[MONGO_DB]
    for col in database.list_collection_names():
        print(" -", col)


if __name__ == "__main__":
    main()
