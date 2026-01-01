import psycopg2

SQL = """
CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
    id serial PRIMARY KEY,
    restaurant_id varchar NOT NULL,
    restaurant_name varchar NOT NULL,
    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL
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
    print("Table dds.dm_restaurants created successfully.")

if __name__ == "__main__":
    main()
