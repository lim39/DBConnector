import pandas as pd
from db_connector.db_connector import DBConnector

# Example configurations for different DBMS (adjust as needed)
DB_CONFIGS = {
    "pg": {
        "dbms": "pg",
        "user": "test_user",
        "pw": "test_password",
        "dbname": "test_db",
        "host": "localhost",
        "port": 5432,
    },
    "mysql": {
        "dbms": "mysql",
        "user": "test_user",
        "pw": "test_password",
        "dbname": "test_db",
        "host": "localhost",
        "port": 3306,
    },
    "mssql": {
        "dbms": "mssql",
        "user": "test_user",
        "pw": "test_password",
        "dbname": "test_db",
        "host": "localhost",
        "port": 1433,
    },
    "oracle": {
        "dbms": "oracle",
        "user": "test_user",
        "pw": "test_password",
        "dbname": "test_service",
        "host": "localhost",
        "port": 1521,
    },
    "sf": {
        "dbms": "sf",
        "user": "test_user",
        "pw": "test_password",
        "dbname": "test_db",
        "account": "test_account",
        "warehouse": "test_warehouse",
    },
}

def test_db_connector_initialization(dbms):
    """Test initialization of DBConnector for various DBMS."""
    config = DB_CONFIGS[dbms]
    db = DBConnector(**config)
    assert db is not None
    db.close()


def test_read_to_df(dbms,query=None):
    """Test read_to_df method for various DBMS."""
    config = DB_CONFIGS[dbms]
    db = DBConnector(**config)

    # Example query for testing (adjust to match your test DB)
    query = query or "SELECT 1 AS test_col"
    df = db.read_to_df(query)
    assert isinstance(df, pd.DataFrame)
    db.close()

    print(type(df))
    print(df)


def test_execution_query(dbms,query):
    """Test execution_query method for various DBMS."""
    config = DB_CONFIGS[dbms]
    db = DBConnector(**config)

    # Example query for testing (adjust to match your test DB)    
    assert db.execution_query(query) is True

    db.close()


if __name__=="__main__":    
    test_db_connector_initialization(dbms='sf')
    test_db_connector_initialization(dbms='pg')
    test_db_connector_initialization(dbms='mysql')
    
    test_read_to_df(dbms='sf')
    test_read_to_df(dbms='pg')
    test_read_to_df(dbms='mysql')

    create_query=f"create TABLE test_table (id INT);"
    drop_query=f"drop TABLE test_table ;"
    test_execution_query(dbms='sf',query=create_query)
    test_execution_query(dbms='sf',query=drop_query)
    test_execution_query(dbms='pg',query=create_query)
    test_execution_query(dbms='pg',query=drop_query)
    test_execution_query(dbms='mysql',query=create_query)
    test_execution_query(dbms='mysql',query=drop_query)


    dbms='mysql' # sf, pg
    config = DB_CONFIGS[dbms]
    db = DBConnector(**config)

    tbName='test_table'
    schema='public'
    
    # drop table
    drop_query =f"drop TABLE if exists {schema}.{tbName};"
    mysql_drop_query =f"drop TABLE if exists {tbName};"
    db.execution_query(drop_query)
    
    # Create table
    create_query = f"create TABLE {tbName} (id INT, name VARCHAR(255))"
    db.execution_query(create_query)

    data = {"id": [1, 2, 3], "name": ["영희", "철수", "Charlie"]}
    df = pd.DataFrame(data)

    # Insert data
    assert db.insert_df(df, tbName,schema) is True

    # Verify data
    result_df = db.read_to_df(f"select * FROM {tbName}")
    assert len(result_df) == len(df)

    print(result_df)