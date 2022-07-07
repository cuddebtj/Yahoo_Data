import pandas as pd
import numpy as np
import yaml
from time import sleep
from pathlib import Path

from utils import get_season, nfl_weeks_pull, game_keys_pull
from yahoo_query import league_season_data

PATH = list(Path().cwd().parent.glob("**/private.yaml"))[0]

try:
    with open(PATH) as file:
        CREDS = yaml.load(file, Loader=yaml.FullLoader)

except Exception as error:
    print(error)

TODAY = np.datetime64("today", "D")
YEAR = TODAY.astype("datetime64[Y]").astype(int) + 1970
NFL_WEEKS = nfl_weeks_pull()
GAME_KEYS = game_keys_pull(first="no")
SEASON = get_season()

try:
    LEAGUE_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["league_ID"].values[0]
except Exception as e:
    print(e)
GAME_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["game_id"].values[0]

try:
    NFL_WEEK = NFL_WEEKS["week"][
        (NFL_WEEKS["end"] >= TODAY) & (NFL_WEEKS["start"] <= TODAY)
    ].values[0]

except Exception as e:
    print(e)
    NFL_WEEK = np.nan
    pass


CONSUMER_KEY = CREDS["YFPY_CONSUMER_KEY"]
CONSUMER_SECRET = CREDS["YFPY_CONSUMER_SECRET"]

league = league_season_data(
    auth_dir=PATH.parent,
    league_id=LEAGUE_ID,
    game_id=GAME_ID,
    game_code="nfl",
    offline=False,
    all_output_as_json=False,
    consumer_key=CONSUMER_KEY,
    consumer_secret=CONSUMER_SECRET,
    browser_callback=True,
)

if TODAY == np.datetime64(f"{YEAR}-08-31"):
    game_keys = league.all_game_keys()
    sleep(5)
    nfl_weeks = league.all_nfl_weeks()
    sleep(5)
    meta = league.metadata(first_time="no")
    sleep(5)
    settings, roster, stat_cat = league.set_roster_pos_stat_cat(first_time="no")
    sleep(5)
    teams = league.teams_and_standings(first_time="no")
    sleep(5)
    players = league.players_list(first_time="no")

if (
    TODAY
    == NFL_WEEKS["end"][(NFL_WEEKS["week"] == 15) & (NFL_WEEKS["game_id"] == GAME_ID)]
):
    teams = league.teams_and_standings(first_time="no")

if (
    TODAY
    == NFL_WEEKS["end"][
        (NFL_WEEKS["game_id"] == GAME_ID) & (NFL_WEEKS["week"] == 1)
    ].values[0]
):
    draft = league.draft_results(first_time="no")

week_roster = league.team_roster_by_week(first_time="no", nfl_week=NFL_WEEK)
sleep(5)
matchups = league.matchups_by_week(first_time="no", nfl_week=NFL_WEEK)
