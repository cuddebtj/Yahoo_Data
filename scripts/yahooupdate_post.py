import json
import logging
import time
import pandas as pd
import numpy as np
import os
import yfpy
from yfpy import YahooFantasySportsQuery
from yfpy import get_logger
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from gspread_pandas import Spread
load_dotenv()

def yahoo_update():
    path = "/home/cuddebtj/python-projects/Yahoo_Data"

    today = np.datetime64("today", "D")
    nfl_dates_df = pd.read_csv(os.path.join(path,"nfl-weeks.csv"), parse_dates=["Start_Date", "End_Date"])
    end = nfl_dates_df[nfl_dates_df["End_Date"] >= today]
    nfl_week = end[end["Start_Date"] <= today]
    nfl_week = nfl_week["Week"].iloc[0]

    weeks = list(range(nfl_week,0,-1))

    df_id = pd.read_csv(os.path.join(path,"ID.csv"), dtype="str")
    season = "2021"

    league_id = df_id[df_id["season"] == season]["league_ID"].values[0]
    game_id = df_id[df_id["season"] == season]["game_ID"].values[0]

    game_code = "nfl"
    data_dir = os.path.join(path,"data")
    auth_dir = Path().cwd()
    working_dir = os.path.join(path,"test")

    consumer_key = os.getenv("yahoo_consumer_key")
    consumer_secret = os.getenv("yahoo_consumer_secret")
    browser_callback = True
    logget = get_logger(__name__)
    log_output = False
    logging.getLogger("yfpy.query").setLevel(level=logging.INFO)

    yahoo_query = YahooFantasySportsQuery(
        auth_dir,
        league_id,
        game_id=game_id,
        game_code=game_code,
        offline=False,
        all_output_as_json=False,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        browser_callback=browser_callback
    )
    
    team_info = yahoo_query.get_league_teams()
    team_json = []
    for t in range(len(team_info)):
        team_json.append(json.loads(str(team_info[t]["team"])))

    team_dict = {}
    team_name = []
    team_id = []
    for m in range(len(team_json)):
        team_name.append(team_json[m]["managers"]["manager"]["nickname"])
        team_id.append(team_json[m]["team_id"])
    team_dict["name"] = team_name
    team_dict["team_id"] = team_id
    team_ids = pd.DataFrame.from_dict(team_dict)

    week_stats = pd.DataFrame()
    for id_ in team_ids["team_id"]:
        current_week_stats = yahoo_query.get_team_stats_by_week(id_, chosen_week="current")
        week_dict = json.loads(str(current_week_stats["team_points"]))
        week_df = pd.DataFrame(week_dict, index=[0])
        week_df["team_id"] = id_
        week_df["name"] = team_ids["name"][int(id_)-1]
        week_stats = pd.concat([week_stats, week_df])
    week_stats.reset_index(drop=True, inplace=True)
    week_stats["GType"] = "-"
    week_stats["key"] = ""
    week_stats["TeamB"] = "-"
    week_stats["ScoreB"] = "-"
    week_stats = week_stats[["key", "week", "GType", "name", "total", "TeamB", "ScoreB"]]
    week_stats.rename(columns={"week": "Week", "name": "TeamA", "total": "ScoreA"}, inplace=True)
    week_stats.replace(to_replace=["Wesley", "ryan", "Peter Billups"], value=["Wes", "Ryan", "Pete"], regex=True, inplace=True)

    spread = Spread("MoM 2021 Schedule", sheet="Schedule")
    old_df = spread.sheet_to_df()
    old_df["Week"] = old_df["Week"].astype(int)
    old_df = old_df[old_df["Week"] < week_stats["Week"][0]]
    new_df = pd.concat([old_df, week_stats])
    new_df.reset_index(drop=True, inplace=True)
    new_df.drop(["key"], axis=1, inplace=True)
    spread.df_to_sheet(new_df, index=False, headers=False, start=(2,2), replace=False, fill_value="")

    with open(path +"/cronoutput.txt", "a") as f:
        f.write("Done: Post season update on " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "\n")

    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    
if __name__ == "__main__":
    yahoo_update()
