import psycopg2

SQL = """
CREATE SCHEMA IF NOT EXISTS dds;

DROP TABLE IF EXISTS dds.srv_wf_settings;

CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
    id serial PRIMARY KEY,
    workflow_key varchar NOT NULL,
    workflow_settings json NOT NULL
);
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
        cur.execute(SQL)

    conn.close()
    print("dds.srv_wf_settings created successfully.")

if __name__ == "__main__":
    main()
