import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables if they exist
    
    Args:
    cur: cursor object
    conn: connection object
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("Table dropped")


def create_tables(cur, conn):
    """
    Create tables
    
    Args:
    cur: cursor object
    conn: connection object
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("Table created")


def main():
    """
    Main function to drop and create tables
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

    drop_tables(cur, conn)
    create_tables(cur, conn)
    # drop_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()