import psycopg2

def get_db_connection(host='localhost', dbname='iot_metrics', user='admin', password='admin'):
    """
    Connects with a specified database using env variables
    """
    conn_str = f"host={host} dbname={dbname} user={user} password={password}"
    try:
        conn = psycopg2.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Error while connecting to postgres instance: {e}")
    return None 

if __name__ == '__main__':
    print('aeiou')
