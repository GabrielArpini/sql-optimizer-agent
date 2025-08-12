import requests
from pathlib import Path
import os
from profiler import get_db_connection

def create_tables(conn):
    """Creates the TPC-H schema tables in the database."""
    # This function remains the same as before
    commands = (
        """
        CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                                N_NAME       CHAR(25) NOT NULL,
                                N_REGIONKEY  INTEGER NOT NULL,
                                N_COMMENT    VARCHAR(152),
                                PRIMARY KEY (N_NATIONKEY));
        """,
        """
        CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                                R_NAME       CHAR(25) NOT NULL,
                                R_COMMENT    VARCHAR(152),
                                PRIMARY KEY (R_REGIONKEY));
        """,
        """
        CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                              P_NAME       VARCHAR(55) NOT NULL,
                              P_MFGR       CHAR(25) NOT NULL,
                              P_BRAND      CHAR(10) NOT NULL,
                              P_TYPE       VARCHAR(25) NOT NULL,
                              P_SIZE       INTEGER NOT NULL,
                              P_CONTAINER  CHAR(10) NOT NULL,
                              P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                              P_COMMENT    VARCHAR(23) NOT NULL,
                              PRIMARY KEY (P_PARTKEY));
        """,
        """
        CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                                 S_NAME        CHAR(25) NOT NULL,
                                 S_ADDRESS     VARCHAR(40) NOT NULL,
                                 S_NATIONKEY   INTEGER NOT NULL,
                                 S_PHONE       CHAR(15) NOT NULL,
                                 S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                                 S_COMMENT     VARCHAR(101) NOT NULL,
                                 PRIMARY KEY (S_SUPPKEY));
        """,
        """
        CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                                 PS_SUPPKEY     INTEGER NOT NULL,
                                 PS_AVAILQTY    INTEGER NOT NULL,
                                 PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                                 PS_COMMENT     VARCHAR(199) NOT NULL,
                                 PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY));
        """,
        """
        CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                                 C_NAME        VARCHAR(25) NOT NULL,
                                 C_ADDRESS     VARCHAR(40) NOT NULL,
                                 C_NATIONKEY   INTEGER NOT NULL,
                                 C_PHONE       CHAR(15) NOT NULL,
                                 C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                                 C_MKTSEGMENT  CHAR(10) NOT NULL,
                                 C_COMMENT     VARCHAR(117) NOT NULL,
                                 PRIMARY KEY (C_CUSTKEY));
        """,
        """
        CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                               O_CUSTKEY        INTEGER NOT NULL,
                               O_ORDERSTATUS    CHAR(1) NOT NULL,
                               O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                               O_ORDERDATE      DATE NOT NULL,
                               O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                               O_CLERK          CHAR(15) NOT NULL, 
                               O_SHIPPRIORITY   INTEGER NOT NULL,
                               O_COMMENT        VARCHAR(79) NOT NULL,
                               PRIMARY KEY (O_ORDERKEY));
        """,
        """
        CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                                 L_PARTKEY     INTEGER NOT NULL,
                                 L_SUPPKEY     INTEGER NOT NULL,
                                 L_LINENUMBER  INTEGER NOT NULL,
                                 L_QUANTITY    DECIMAL(15,2) NOT NULL,
                                 L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                                 L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                                 L_TAX         DECIMAL(15,2) NOT NULL,
                                 L_RETURNFLAG  CHAR(1) NOT NULL,
                                 L_LINESTATUS  CHAR(1) NOT NULL,
                                 L_SHIPDATE    DATE NOT NULL,
                                 L_COMMITDATE  DATE NOT NULL,
                                 L_RECEIPTDATE DATE NOT NULL,
                                 L_SHIPINSTRUCT CHAR(25) NOT NULL,
                                 L_SHIPMODE     CHAR(10) NOT NULL,
                                 L_COMMENT      VARCHAR(44) NOT NULL,
                                 PRIMARY KEY (L_ORDERKEY, L_LINENUMBER));
        """
    )
    with conn.cursor() as curs:
        exists_count = 0
        table_names = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders', 'lineitem']
        curs.execute("""SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'""")
        found_tables = curs.fetchall()
        for table in table_names:
            if (table,) in found_tables: #Fetchall returns sets with tables (e.g ('region',), ('part',))
                exists_count += 1
        if exists_count == len(table_names):
            return
        
        try:
            for i,command in enumerate(commands):
                print(f"Executing {i+1} command")
                curs.execute(command)
            print("Tables created successfully!")
            conn.commit()
        except Exception as e:
            print(f"Error executing commands: {e}")
            conn.rollback()


def load_data(conn, data_dir="."):
    """Loads data from .tbl files into the corresponding tables."""
    table_names = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders', 'lineitem']
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM nation;")
            if cur.fetchone()[0] > 0:
                print("Data already loaded. Skipping population.")
                return

            print("Loading data into tables...")
            for table in table_names:
                file_path = os.path.join(data_dir, f"{table}.tbl")
                
                if os.path.exists(file_path):
                    print(f"Loading {table}...")
                    with open(file_path, 'r') as f:
                        cur.copy_expert(f"COPY {table} FROM STDIN WITH DELIMITER '|'", f)
                else:
                    print(f"Data file not found for table {table}")
        
        conn.commit()
        print("All data loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")
        conn.rollback()    

def main():
    conn = get_db_connection()
    create_tables(conn)
    load_data(conn)
     
if __name__ == '__main__':
    main()
    



