import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """create a database named flyingdb"""
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create flyingdb database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS flyingdb")
    cur.execute("CREATE DATABASE flyingdb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=flyingdb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """ Drop all tables if exists before creating them using the query mentioned in sql_queries.py"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ create all tables using the query mentioned in sql_queries.py"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()