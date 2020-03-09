import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load Data from S3 Bucket to Staging Tables"""
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def insert_tables(cur, conn):
    """Insert Data from Staging Tables to Fact and Dim Tables"""
    for query in insert_table_queries:
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()