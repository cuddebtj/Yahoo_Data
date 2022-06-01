import json
import logging
import pandas as pd
import numpy as np
import os
from pathlib import Path
from yfpy import YahooFantasySportsQuery
from yfpy import get_logger
from dotenv import load_dotenv


from utils import get_season, league_season_info


load_dotenv()
LOGGET = get_logger(__name__)
LOG_OUTPUT = False
logging.getLogger("yfpy.query").setLevel(level=logging.INFO)

PATH = Path.cwd().parents[0]
DATA_DIR = PATH / "data"

SEASON = get_season()
NFL_DATES_DF, LEAGUE_ID_DF = league_season_info()

TODAY = np.datetime64("today", "D")
NFL_WEEK = NFL_DATES_DF["Week"][
    (NFL_DATES_DF["End_Date"] >= TODAY) & (NFL_DATES_DF["Start_Date"] <= TODAY)
].values[0]
LEAGUE_ID = LEAGUE_ID_DF[LEAGUE_ID_DF["season"] == SEASON]["league_ID"].values[0]
GAME_ID = LEAGUE_ID_DF[LEAGUE_ID_DF["season"] == SEASON]["game_ID"].values[0]
WEEKS = list(range(NFL_WEEK, 0, -1))

CONSUMER_KEY = os.getenv("yahoo_client_id")
CONSUMER_SECRET = os.getenv("yahoo_client_secret")

try:
    yahoo_query = YahooFantasySportsQuery(
        auth_dir=PATH,
        league_id=LEAGUE_ID,
        game_id=GAME_ID,
        game_code="nfl",
        offline=False,
        all_output_as_json=False,
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        browser_callback=True,
    )

except Exception as e:
    print(e)


def yahoo_update_post_season():

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
        current_week_stats = yahoo_query.get_team_stats_by_week(
            id_, chosen_week="current"
        )
        week_dict = json.loads(str(current_week_stats["team_points"]))
        week_df = pd.DataFrame(week_dict, index=[0])
        week_df["team_id"] = id_
        week_df["name"] = team_ids["name"][int(id_) - 1]
        week_stats = pd.concat([week_stats, week_df])
    week_stats.reset_index(drop=True, inplace=True)
    week_stats["GType"] = "-"
    week_stats["key"] = ""
    week_stats["TeamB"] = "-"
    week_stats["ScoreB"] = "-"
    week_stats = week_stats[
        ["key", "week", "GType", "name", "total", "TeamB", "ScoreB"]
    ]
    week_stats.rename(
        columns={"week": "Week", "name": "TeamA", "total": "ScoreA"}, inplace=True
    )
    week_stats.replace(
        to_replace=["Wesley", "ryan", "Peter Billups"],
        value=["Wes", "Ryan", "Pete"],
        regex=True,
        inplace=True,
    )


def yahoo_update_regular_season():

    weeks_df = pd.DataFrame()

    for week in WEEKS:

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
        f_df.insert(
            1,
            "GType",
            np.where(
                f_df["matchup.is_consolation"] == 1,
                "Toilet",
                np.where(f_df["matchup.is_playoffs"] == 1, "Playoffs", "Reg"),
            ),
        )

        cols = [
            "team.team_points.week.a_team",
            "GType",
            "team.managers.manager.nickname.a_team",
            "team.team_points.total.a_team",
            "team.managers.manager.nickname.b_team",
            "team.team_points.total.b_team",
        ]

        final_df = f_df[cols]
        final_df.columns = ["Week", "GType", "TeamA", "ScoreA", "TeamB", "ScoreB"]
        final_df.replace(
            to_replace=["Wesley", "ryan", "Peter Billups"],
            value=["Wes", "Ryan", "Pete"],
            regex=True,
            inplace=True,
        )
        weeks_df = pd.concat([final_df, weeks_df])
