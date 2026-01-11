from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class UserReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_users(self, load_threshold: datetime, limit: int) -> List[Dict]:
        filter_ = {"update_ts": {"$gt": load_threshold}}
        sort = [("update_ts", 1)]
        docs = list(
            self.dbs.get_collection("users").find(filter=filter_, sort=sort, limit=limit)
        )
        return docs
    

from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaverUsers:
    def save_object(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.ordersystem_users(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {"id": id, "val": str_val, "update_ts": update_ts},
            )


from datetime import datetime
from logging import Logger

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str

from examples.stg.order_system_users_dag.user_reader import UserReader
from examples.stg.order_system_users_dag.pg_saver import PgSaverUsers


class UserLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 1000  # as your requirement (example said 1000)

    WF_KEY = "ordersystem_users_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: UserReader, pg_dest: PgConnect, pg_saver: PgSaverUsers, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()},
                )

            last_loaded_ts = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY])
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_users(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from users collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d["update_ts"], d)
                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing users.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue]).isoformat()
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")
            return len(load_queue)

import logging
import pendulum

from airflow.decorators import dag, task
from airflow.models.variable import Variable

from examples.stg.order_system_restaurants_dag.pg_saver import PgSaver
from examples.stg.order_system_restaurants_dag.restaurant_loader import RestaurantLoader
from examples.stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader

from examples.stg.order_system_users_dag.pg_saver import PgSaverUsers
from examples.stg.order_system_users_dag.user_loader import UserLoader
from examples.stg.order_system_users_dag.user_reader import UserReader

from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin"],
    is_paused_upon_creation=True,
)
def sprint5_stg_order_system():

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = RestaurantReader(mongo_connect)
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    @task()
    def load_users():
        pg_saver = PgSaverUsers()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = UserReader(mongo_connect)
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    r = load_restaurants()
    u = load_users()

    r >> u


order_stg_dag = sprint5_stg_order_system()  # noqa
