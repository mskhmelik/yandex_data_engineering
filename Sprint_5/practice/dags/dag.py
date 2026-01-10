import json
import logging
from datetime import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


class UserReader:
    def __init__(self, mongo_connect: MongoConnect):
        self._mongo = mongo_connect

    def _get_collection(self):
        # preferred path if the course lib provides it
        if hasattr(self._mongo, "get_collection"):
            return self._mongo.get_collection("users")

        # fallback: MongoConnect has .client but it might be a METHOD
        client = getattr(self._mongo, "client", None)
        if client is None:
            raise RuntimeError("MongoConnect has no get_collection() and no .client")

        if callable(client):
            client = client()  # ✅ this fixes your error

        # db name might also be a method/attr depending on lib
        db_name = getattr(self._mongo, "db", None) or getattr(self._mongo, "database", None)
        if callable(db_name):
            db_name = db_name()

        if not db_name:
            db_name = Variable.get("MONGO_DB_DATABASE_NAME")

        return client[db_name]["users"]


    def get_users(self, last_loaded_ts: datetime, limit: int):
        col = self._get_collection()

        # Mongo query: update_ts > watermark
        cursor = (
            col.find({"update_ts": {"$gt": last_loaded_ts}})
            .sort("update_ts", 1)
            .limit(limit)
        )
        return list(cursor)


class PgSaverUsers:
    def save_object(self, conn, object_id: str, update_ts: datetime, doc: dict) -> None:
        # store full doc as json string
        payload = json.dumps(doc, default=str, ensure_ascii=False)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.ordersystem_users (object_id, object_value, update_ts)
                VALUES (%s, %s, %s)
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts;
                """,
                (object_id, payload, update_ts),
            )


class UserLoader:
    _LOG_THRESHOLD = 100
    _SESSION_LIMIT = 1000

    WF_KEY = "ordersystem_users_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, reader: UserReader, pg_dest, pg_saver: PgSaverUsers, logger: logging.Logger) -> None:
        self.reader = reader
        self.pg_dest = pg_dest
        self.pg_saver = pg_saver
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            # 1) read settings
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()},
                )

            last_loaded_ts = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY])
            self.log.info("users load starting from checkpoint: %s", last_loaded_ts)

            # 2) read from mongo
            load_queue = self.reader.get_users(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info("Found %s documents to sync from users collection.", len(load_queue))
            if not load_queue:
                self.log.info("No new users. Quit.")
                return 0

            # 3) save rows into stg.ordersystem_users (in this transaction)
            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d["update_ts"], d)
                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info("processed %s users of %s", i, len(load_queue))

            # 4) update watermark (also in this transaction)
            new_last_ts = max([t["update_ts"] for t in load_queue]).isoformat()
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = new_last_ts
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, json.dumps(wf_setting.workflow_settings))

            self.log.info("users load finished. new checkpoint: %s", new_last_ts)
            return len(load_queue)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin"],
    is_paused_upon_creation=True,
)
def sprint5_stg_order_system_users():

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def users_load():
        pg_saver = PgSaverUsers()

        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        reader = UserReader(mongo_connect)

        loader = UserLoader(reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    users_load()  # one task


order_users_stg_dag = sprint5_stg_order_system_users()
