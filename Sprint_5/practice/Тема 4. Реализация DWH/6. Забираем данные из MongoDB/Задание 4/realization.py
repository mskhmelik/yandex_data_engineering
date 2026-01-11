import json
import logging
from datetime import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)

DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"

SESSION_LIMIT = 1000

WF_SETTINGS_TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    workflow_key varchar PRIMARY KEY,
    workflow_settings jsonb NOT NULL
);
"""


def _json_dumps_safe(obj) -> str:
    # Mongo docs can contain ObjectId, datetime, etc.
    return json.dumps(obj, default=str, ensure_ascii=False)


def _get_last_loaded_ts(conn, workflow_key: str) -> datetime:
    with conn.cursor() as cur:
        cur.execute(WF_SETTINGS_TABLE_DDL)
        cur.execute(
            """
            SELECT (workflow_settings->>'last_loaded_ts') AS last_loaded_ts
            FROM stg.srv_wf_settings
            WHERE workflow_key = %(wk)s;
            """,
            {"wk": workflow_key},
        )
        row = cur.fetchone()

    if not row or not row[0]:
        return datetime(2022, 1, 1)

    return datetime.fromisoformat(row[0])


def _save_last_loaded_ts(conn, workflow_key: str, last_loaded_ts: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
            VALUES (%(wk)s, jsonb_build_object('last_loaded_ts', (%(ts)s)::text))
            ON CONFLICT (workflow_key) DO UPDATE
            SET workflow_settings = EXCLUDED.workflow_settings;
            """,
            {"wk": workflow_key, "ts": last_loaded_ts.isoformat()},
        )


def _fetch_mongo_docs(mongo_db, collection_name: str, last_loaded_ts: datetime, limit: int):
    filter_ = {"update_ts": {"$gt": last_loaded_ts}}
    sort_ = [("update_ts", 1)]
    return list(mongo_db.get_collection(collection_name).find(filter=filter_, sort=sort_, limit=limit))


def _upsert_docs(conn, target_table: str, docs) -> None:
    # target_table must be one of:
    # stg.ordersystem_restaurants / stg.ordersystem_users / stg.ordersystem_orders
    sql = f"""
        INSERT INTO {target_table}(object_id, object_value, update_ts)
        VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
        ON CONFLICT (object_id) DO UPDATE
        SET
            object_value = EXCLUDED.object_value,
            update_ts = EXCLUDED.update_ts;
    """

    with conn.cursor() as cur:
        for d in docs:
            cur.execute(
                sql,
                {
                    "object_id": str(d["_id"]),
                    "object_value": _json_dumps_safe(d),
                    "update_ts": d["update_ts"],
                },
            )


def _load_collection(
    dwh_pg_connect,
    mongo_db,
    collection_name: str,
    target_table: str,
    workflow_key: str,
    limit: int,
) -> int:
    with dwh_pg_connect.connection() as conn:
        last_ts = _get_last_loaded_ts(conn, workflow_key)
        log.info("%s: last checkpoint = %s", collection_name, last_ts)

        docs = _fetch_mongo_docs(mongo_db, collection_name, last_ts, limit)
        log.info("%s: found %s docs", collection_name, len(docs))

        if not docs:
            return 0

        # 3+4 in one transaction: upsert + save checkpoint
        _upsert_docs(conn, target_table, docs)

        new_last_ts = max(d["update_ts"] for d in docs)
        _save_last_loaded_ts(conn, workflow_key, new_last_ts)

        log.info("%s: saved new checkpoint = %s", collection_name, new_last_ts)
        return len(docs)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "mongo"],
    is_paused_upon_creation=False,
)
def ordersystem_mongo_to_stg():

    # DWH connection (Postgres)
    dwh_pg_connect = ConnectionBuilder.pg_conn(DWH_CONN_ID)

    # Mongo connection params from Airflow Variables
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    def _mongo_db():
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        return mongo_connect.client()

    @task()
    def load_restaurants():
        mongo_db = _mongo_db()
        return _load_collection(
            dwh_pg_connect=dwh_pg_connect,
            mongo_db=mongo_db,
            collection_name="restaurants",
            target_table="stg.ordersystem_restaurants",
            workflow_key="ordersystem_restaurants_origin_to_stg_workflow",
            limit=SESSION_LIMIT,
        )

    @task()
    def load_users():
        mongo_db = _mongo_db()
        return _load_collection(
            dwh_pg_connect=dwh_pg_connect,
            mongo_db=mongo_db,
            collection_name="users",
            target_table="stg.ordersystem_users",
            workflow_key="ordersystem_users_origin_to_stg_workflow",
            limit=SESSION_LIMIT,
        )

    @task()
    def load_orders():
        mongo_db = _mongo_db()
        return _load_collection(
            dwh_pg_connect=dwh_pg_connect,
            mongo_db=mongo_db,
            collection_name="orders",
            target_table="stg.ordersystem_orders",
            workflow_key="ordersystem_orders_origin_to_stg_workflow",
            limit=SESSION_LIMIT,
        )

    r = load_restaurants()
    u = load_users()
    o = load_orders()

    r >> u >> o


dag = ordersystem_mongo_to_stg()
