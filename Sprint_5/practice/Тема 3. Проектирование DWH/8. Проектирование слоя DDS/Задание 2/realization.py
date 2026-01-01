import psycopg2

SQL = """
CREATE TABLE IF NOT EXISTS dds.dm_users (
    id serial PRIMARY KEY,
    user_id varchar NOT NULL,
    user_name varchar NOT NULL,
    user_login varchar NOT NULL
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
    print("Table dds.dm_users created successfully.")

if __name__ == "__main__":
    main()
