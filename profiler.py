import psycopg2


def get_db_connection():
    """
    Connects with a specified database using env variables
    """
    conn_str = "host='localhost' dbname='postgres' user='admin' password='admin'"
    try:
        conn = psycopg2.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Error while connecting to postgres instance: {e}")
    return None 

def profile_query():
    """
    Create the profile of a query, the performance it have in the database.
    """

if __name__ == '__main__':
    conn = get_db_connection()
    if conn:
        print(conn)
        print("Success!")
    else:
        print("Not success!")
