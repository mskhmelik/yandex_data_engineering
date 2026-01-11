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

WF_KEY = "ordersystem_orders_origin_to_stg_workflow"

WF_SETTINGS_TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    workflow_key varchar PRIMARY KEY,
    workflow_settings jsonb NOT NULL
);
"""


def _json_dumps_safe(obj) -> str:
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
    # cast to text to avoid psycopg3 "IndeterminateDatatype"
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


def _fetch_orders(mongo_db, last_loaded_ts: datetime, limit: int):
    filter_ = {"update_ts": {"$gt": last_loaded_ts}}
    sort_ = [("update_ts", 1)]
    return list(mongo_db.get_collection("orders").find(filter=filter_, sort=sort_, limit=limit))


def _upsert_orders(conn, docs) -> None:
    sql = """
        INSERT INTO stg.ordersystem_orders(object_id, object_value, update_ts)
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


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "mongo", "orders"],
    is_paused_upon_creation=False,
)
def ordersystem_orders_to_stg():

    dwh_pg_connect = ConnectionBuilder.pg_conn(DWH_CONN_ID)

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_orders():
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        mongo_db = mongo_connect.client()

        total_loaded = 0

        with dwh_pg_connect.connection() as conn:
            last_ts = _get_last_loaded_ts(conn, WF_KEY)
            log.info("orders: starting checkpoint = %s", last_ts)

            while True:
                docs = _fetch_orders(mongo_db, last_ts, SESSION_LIMIT)
                if not docs:
                    break

                _upsert_orders(conn, docs)

                batch_last_ts = max(d["update_ts"] for d in docs)
                _save_last_loaded_ts(conn, WF_KEY, batch_last_ts)

                total_loaded += len(docs)
                log.info("orders: loaded batch=%s, new checkpoint=%s, total=%s", len(docs), batch_last_ts, total_loaded)

                # move watermark forward for next batch in this same run
                last_ts = batch_last_ts

                # if batch smaller than limit, no more data right now
                if len(docs) < SESSION_LIMIT:
                    break

        return total_loaded

    load_orders()


dag = ordersystem_orders_to_stg()
