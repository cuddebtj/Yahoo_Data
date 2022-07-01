import pandas as pd
import pyodbc
import os
from gspread_pandas import Spread
from pathlib import Path
from datetime import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, DateTime, Float, INT
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import logging

logger = logging.getLogger("sqlalchemy")
logger.setLevel(logging.ERROR)

load_dotenv()

PATH = Path.cwd().parents[0]


def get_season():
    """
    Calculate the season the NFL Fantasy Season is in
    If season has hit August first, it will still be previous season
    """
    date = dt.today()
    nfl_start = dt(date.year, 9, 1)
    nfl_end = dt(date.year + 1, 2, 28)
    if nfl_start <= date <= nfl_end:
        season = int(nfl_start.year)
    else:
        season = int(nfl_start.year) - 1

    return season


def google_sheet_upload(new_df):
    """
    Get data already in google sheet and combine it with the new data
    Re-load it to google sheet
    """
    try:
        spread = Spread("Historical Data", sheet="Data")
        old_df = spread.sheet_to_df()
        old_df["Week"] = old_df["Week"].astype(int)
        df = pd.concat([old_df, new_df])
        df.reset_index(drop=True, inplace=True)
        df.drop(["key"], axis=1, inplace=True)
        df.drop_duplicates(inplace=True)
        spread.df_to_sheet(
            df, index=False, headers=False, start=(2, 2), replace=False, fill_value=""
        )
    except Exception as e:
        print(e)


def sql_upload_table(
    dataframe, table_name, data_schema, chunksize, if_exists="append", index=False
):
    """
    Easy upload for tables to SQL Server
    """

    sql_driver = os.getenv("sql_driver")
    sql_server = os.getenv("sql_server")
    sql_database = os.getenv("sql_database")
    sql_username = os.getenv("sql_username")
    sql_password = os.getenv("sql_password")

    connection_string = (
        "DRIVER={"
        + sql_driver
        + "};SERVER="
        + sql_server
        + ";DATABASE="
        + sql_database
        + ";UID="
        + sql_username
        + ";PWD="
        + sql_password
        + ";Trusted_Connection=yes;"
    )

    def sqlcol(dfparam):
        dtypedict = {}
        for i, j in zip(dfparam.columns, dfparam.dtypes):
            if "object" in str(j):
                dtypedict.update({i: NVARCHAR(length=255)})

            if "datetime" in str(j):
                dtypedict.update({i: DateTime()})

            if "float" in str(j):
                dtypedict.update({i: Float(precision=2, asdecimal=True)})

            if "int" in str(j):
                dtypedict.update({i: INT()})

        return dtypedict

    dtype_outputs = sqlcol(dataframe)

    try:

        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": connection_string}
        )
        engine = create_engine(connection_url)
        conn = engine.connect()

        dataframe.to_sql(
            table_name,
            con=engine,
            if_exists=if_exists,
            index=index,
            schema=data_schema,
            chunksize=chunksize,
            dtype=dtype_outputs,
        )

        conn.close()

        print(f"{table_name} successfully updated or added to SQL server.")

    except Exception as e:
        print(f"{table_name} was not created.\n{e}")


def sql_grab_table(sql_string):
    """
    Easy grab table from SQL Server
    """
    if not sql_string:
        print('Please entry either a table name or query to pull data from sql.')

    else:
        sql_driver = os.getenv("sql_driver")
        sql_server = os.getenv("sql_server")
        sql_database = os.getenv("sql_database")
        sql_username = os.getenv("sql_username")
        sql_password = os.getenv("sql_password")
        connection_string = (
            "DRIVER={"
            + sql_driver
            + "};SERVER="
            + sql_server
            + ";DATABASE="
            + sql_database
            + ";UID="
            + sql_username
            + ";PWD="
            + sql_password
            + ";Trusted_Connection=yes;"
        )

        try:
            connection_url = URL.create(
                "mssql+pyodbc", query={"odbc_connect": connection_string}
            )
            engine = create_engine(connection_url)
            conn = engine.connect()

            df = pd.read_sql(sql_string, con=conn)

            conn.close()

            print(f"{sql_string} successfully pulled from SQL server.")

            return df

        except Exception as e:
            print(f"{sql_string} was not successfully pulled from SQL server.\n{e}")



def nfl_weeks_pull():
    """
    Function to call assests files for Yahoo API Query
    """
    try:
        nfl_weeks = sql_grab_table("NFLWeeks")
        return nfl_weeks
    except Exception as e:
        print(e)


def game_keys_pull(first="yes"):
    """
    Function to call game_keys
    """
    try:
        if "YES" == str(first).upper():
            game_keys = pd.read_csv(PATH / "assests" / "ID.csv")
            return game_keys

        elif "NO" == str(first).upper():
            game_keys = sql_grab_table("GameKeys")
            return game_keys

    except Exception as e:
        print(e)
