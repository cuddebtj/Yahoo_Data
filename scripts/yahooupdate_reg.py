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
    
    weeks_df = pd.DataFrame()
    
    for week in weeks:
        
        scoreboard = yahoo_query.get_league_scoreboard_by_week(week)

        json_ = json.loads(str(scoreboard))
        score_df = pd.json_normalize(json_["matchups"])
        teams_df = pd.json_normalize(score_df["matchup.teams"])
        teamsa1_df = pd.json_normalize(teams_df[0]).add_suffix(".a_team")
        teamsb1_df = pd.json_normalize(teams_df[1]).add_suffix(".b_team")
        teamsa2_df = pd.json_normalize(teams_df[1]).add_suffix(".a_team")
        teamsb2_df = pd.json_normalize(teams_df[0]).add_suffix(".b_team")

        match_type = score_df[["matchup.is_consolation", "matchup.is_playoffs"]]
        fa_df = pd.concat([teamsa1_df, teamsb1_df, match_type], axis=1)
        fb_df = pd.concat([teamsa2_df, teamsb2_df, match_type], axis=1)

        f_df = pd.concat([fa_df, fb_df]).reset_index(drop=True)
        f_df.insert(1,"GType", 
                    np.where(f_df["matchup.is_consolation"] == 1, "Toilet", 
                             np.where(f_df["matchup.is_playoffs"] == 1, "Playoffs", "Reg")))

        cols = ["team.team_points.week.a_team", "GType", 
                "team.managers.manager.nickname.a_team", "team.team_points.total.a_team", 
                "team.managers.manager.nickname.b_team", "team.team_points.total.b_team"]

        final_df = f_df[cols]
        final_df.columns = ["Week", "GType", "TeamA", "ScoreA", "TeamB", "ScoreB"]
        final_df.replace(to_replace=["Wesley", "ryan", "Peter Billups"], value=["Wes", "Ryan", "Pete"], regex=True, inplace=True)
        weeks_df = pd.concat([final_df, weeks_df])
    
    spread = Spread("MoM 2021 Schedule", sheet="Schedule")
    spread.df_to_sheet(weeks_df, index=False, headers=False, start=(2,2), replace=False, fill_value="")

    with open(path +"/cronoutput.txt", "a") as f:
        f.write("Done: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "\n")

    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    
if __name__ == "__main__":
    yahoo_update()
