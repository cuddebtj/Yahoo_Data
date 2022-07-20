import pandas as pd
import numpy as np
import math
import itertools

from pathlib import Path
from datetime import datetime as dt

from db_psql_model import DatabaseCursor

PATH = list(Path().cwd().parent.glob("**/private.yaml"))[0]


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


def nfl_weeks_pull():
    """
    Function to call assests files for Yahoo API Query
    """

    try:
        db_cursor = DatabaseCursor(PATH, options="-c search_path=prod")
        nfl_weeks = db_cursor.copy_data_from_postgres("SELECT * FROM prod.nfl_weeks")
        nfl_weeks["end"] = pd.to_datetime(nfl_weeks["end"])
        nfl_weeks["start"] = pd.to_datetime(nfl_weeks["start"])
        return nfl_weeks

    except Exception as e:
        print(e)


def game_keys_pull(first="yes"):
    """
    Function to call game_keys
    """
    try:
        if "YES" == str(first).upper():
            game_keys = pd.read_csv(PATH.parent / "assests" / "game_keys.csv")
            return game_keys

        elif "NO" == str(first).upper():
            db_cursor = DatabaseCursor(PATH, options="-c search_path=prod")
            game_keys = db_cursor.copy_data_from_postgres(
                "SELECT * FROM prod.game_keys"
            )
            return game_keys

    except Exception as e:
        print(e)


def data_upload(df: pd.DataFrame, first_time, table_name, query, path, option):

    try:
        if str(first_time).upper == "YES":
            df.drop_duplicates(ignore_index=True, inplace=True)
            df.sort_index(axis=1, inplace=True)
            DatabaseCursor(path, options=option).copy_table_to_postgres_new(
                df, table_name, first_time="yes"
            )

        elif str(first_time).upper() == "NO":
            psql = DatabaseCursor(path, options=option).copy_data_from_postgres(query)
            df = pd.concat([psql, df])
            df.drop_duplicates(ignore_index=True, inplace=True)
            DatabaseCursor(path, options=option).copy_table_to_postgres_new(
                df, table_name, first_time="no"
            )

    except Exception as e:
        print(f"Error: {e}")
