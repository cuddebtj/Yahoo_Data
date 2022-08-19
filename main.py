import numpy as np
import yaml
from time import sleep
from pathlib import Path

from scripts.utils import get_season, nfl_weeks_pull, game_keys_pull
from scripts.output_txt import log_print
from scripts.yahoo_query import league_season_data

PATH = list(Path().cwd().parent.glob("**/private.yaml"))[0]

try:
    with open(PATH) as file:
        CREDS = yaml.load(file, Loader=yaml.SafeLoader)

    CONSUMER_KEY = CREDS["YFPY_CONSUMER_KEY"]
    CONSUMER_SECRET = CREDS["YFPY_CONSUMER_SECRET"]

except Exception as error:
    print(error)


try:
    TODAY = np.datetime64("today", "D")
    YEAR = TODAY.astype("datetime64[Y]").astype(int) + 1970
    NFL_WEEKS = nfl_weeks_pull()
    MAX_WEEK = NFL_WEEKS["week"].max()
    GAME_KEYS = game_keys_pull(first="no")
    SEASON = get_season()
    LEAGUE_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["league_id"].values[0]
    GAME_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["game_id"].values[0]
    try:
        NFL_WEEK = NFL_WEEKS["week"][
            (NFL_WEEKS["end"] >= TODAY) & (NFL_WEEKS["start"] <= TODAY)
        ].values[0]
    except:
        NFL_WEEK = np.nan

except Exception as e:
    log_print(
        error=e,
        module_="main.py",
        today='Unknown',
        year='Unknown',
        max_week='Unknown',
        season='Unknown',
        at_line="27",
    )
    LEAGUE_ID = '103661'
    GAME_ID = '406'
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
    league.all_game_keys()
    league.all_nfl_weeks()
    
else:
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

    if TODAY == np.datetime64(f"{YEAR}-08-31", "D"):
        game_keys = league.all_game_keys()
        sleep(5)
        nfl_weeks = league.all_nfl_weeks()
        sleep(5)
        meta = league.metadata(first_time="no")
        sleep(5)
        settings, roster, stat_cat = league.set_roster_pos_stat_cat(first_time="no")
        sleep(5)
        # players = league.players_list(first_time="no")

    if (
        TODAY
        == NFL_WEEKS["end"][
            (NFL_WEEKS["week"] == MAX_WEEK) & (NFL_WEEKS["game_id"] == GAME_ID)
        ].values[0]
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
    sleep(5)
    team_points = league.team_points_by_week(first_time="no", nfl_week=NFL_WEEK)
