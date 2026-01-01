import psycopg2

DDL = """
CREATE SCHEMA IF NOT EXISTS stg;

DROP TABLE IF EXISTS stg.bonussystem_users;
DROP TABLE IF EXISTS stg.bonussystem_ranks;
DROP TABLE IF EXISTS stg.bonussystem_events;

CREATE TABLE stg.bonussystem_users (
    id integer NOT NULL,
    order_user_id text NOT NULL
);

CREATE TABLE stg.bonussystem_ranks (
    id integer NOT NULL,
    name varchar(2048) NOT NULL,
    bonus_percent numeric(19, 5) NOT NULL,
    min_payment_threshold numeric(19, 5) NOT NULL
);

CREATE TABLE stg.bonussystem_events (
    id integer NOT NULL,
    event_ts timestamp NOT NULL,
    event_type varchar NOT NULL,
    event_value text NOT NULL
);

CREATE INDEX idx_bonussystem_events__event_ts
    ON stg.bonussystem_events (event_ts);
"""

def main():
    conn = psycopg2.connect(
        host="localhost",
        port=15432,
        database="de",
        user="jovyan",
        password="jovyan"
    )

    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(DDL)

    conn.close()
    print("STG bonus system schema recreated successfully.")

if __name__ == "__main__":
    main()
