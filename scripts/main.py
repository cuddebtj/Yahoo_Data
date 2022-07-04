import pandas as pd
import numpy as np
import yaml
from pathlib import Path

from utils import get_season, nfl_weeks_pull, game_keys_pull
from yahoo_query import league_season_data

PATH = Path.cwd().parents[0]
PATH = PATH / "Yahoo_Data"
try:
    with open(PATH / "private.yaml") as file:
        CREDS = yaml.load(file, Loader=yaml.FullLoader)

except Exception as error:
    print(error)

# TODAY = np.datetime64("today", "D")
TODAY = np.datetime64("2021-09-28")
NFL_WEEKS = nfl_weeks_pull()
GAME_KEYS = game_keys_pull(first="no")
SEASON = get_season()
LEAGUE_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["league_ID"].values[0]
GAME_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["game_id"].values[0]
NFL_WEEK = NFL_WEEKS["week"][(NFL_WEEKS["end"] >= TODAY) & (NFL_WEEKS["start"] <= TODAY)].values[0]
# nfl_weeks_list = list(NFL_WEEKS["week"][NFL_WEEKS["game_id"] == GAME_ID])


CONSUMER_KEY = CREDS["YFPY_CONSUMER_KEY"]
CONSUMER_SECRET = CREDS["YFPY_CONSUMER_SECRET"]

league = league_season_data(
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
# game_keys = league.all_game_keys()
# nfl_weeks = league.all_nfl_weeks()

# meta = league.metadata(first_time="no")
# settings, roster, stat_cat = league.set_roster_pos_stat_cat(first_time="no")
# draft = league.draft_results(first_time="no")
# teams = league.teams_and_standings(first_time="no")
# players = league.players_list(first_time="no")

# for week in nfl_weeks_list[:-1]:
#     week_roster = league.team_roster_by_week(first_time='no', nfl_week=week)

# for week in nfl_weeks_list[:-1]:
#     matchups = league.matchups_by_week(first_time='no', nfl_week=week)

# week_roster = league.team_roster_by_week(first_time='no', nfl_week=NFL_WEEK)
# matchups = league.matchups_by_week(first_time='no', nfl_week=NFL_WEEK)
