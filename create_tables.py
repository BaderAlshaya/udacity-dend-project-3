import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop All Tables"""
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def create_tables(cur, conn):
    """Create All Tables"""
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except Exception as e:
        print(e)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()