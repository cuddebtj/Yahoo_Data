import pandas as pd
import pyodbc
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv



def table_upload_sql(dataframe, table_name, if_exists="append", index=False, data_schema, chunksize):
    """
    Easy upload for tables to SQL Server
    """
    load_dotenv()
    sql_driver = os.getenv("sql_driver")
    sql_server = os.getenv("sql_server")
    sql_database = os.getenv("sql_database")
    sql_username = os.getenv("sql_username")
    sql_password = os.getnenv("sql_password")

    connection_string = (
        "DRIVER={"
        + sql_driver
        + "};SERVER="
        + sql_server
        + ";DATABASE="
        + sql_database
        + ";UID="
        + sql_username
        + ';PWD='
        + sql_password
        + ";Trusted_Connection=yes;"    
    )

    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    conn = engine.connect()

    dataframe.to_sql(table_name, engine=engine, if_exists=if_exists, index=index, schema=data_schema, chunksize=chunksize)

    conn.close()