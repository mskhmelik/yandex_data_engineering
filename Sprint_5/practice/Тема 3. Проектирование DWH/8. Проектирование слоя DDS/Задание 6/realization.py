import psycopg2

SQL = """
CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
    id serial PRIMARY KEY,
    ts timestamp NOT NULL,
    year smallint NOT NULL
        CONSTRAINT dm_timestamps_year_check
        CHECK (year >= 2022 AND year < 2500),
    month smallint NOT NULL
        CONSTRAINT dm_timestamps_month_check
        CHECK (month >= 1 AND month <= 12),
    day smallint NOT NULL
        CONSTRAINT dm_timestamps_day_check
        CHECK (day >= 1 AND day <= 31),
    time time NOT NULL,
    date date NOT NULL
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
    print("Table dds.dm_timestamps created successfully.")

if __name__ == "__main__":
    main()
