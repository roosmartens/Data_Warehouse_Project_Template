import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to staging tables
    
    Args:
    cur: cursor object
    conn: connection object
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("Table loaded")


def insert_tables(cur, conn):
    """
    Insert data from staging tables to analytics tables

    Args:
    cur: cursor object
    conn: connection object
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("Table inserted")


def main():
    """
    Main function to load data from S3 to staging tables and insert data from staging tables to analytics tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        host=config.get('CLUSTER', 'host'),
        dbname=config.get('CLUSTER', 'db_name'),
        user=config.get('CLUSTER', 'db_user'),
        password=config.get('CLUSTER', 'db_password'),
        port=config.get('CLUSTER', 'db_port')
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()