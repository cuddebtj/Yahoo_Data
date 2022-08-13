import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from time import sleep

from scripts.utils import nfl_weeks_pull, game_keys_pull
from scripts.yahoo_query import league_season_data

dates = [
    # np.datetime64("2021-09-28"),
    np.datetime64("2020-09-28"),
    np.datetime64("2019-09-28"),
    np.datetime64("2018-09-28"),
    np.datetime64("2017-09-28"),
    np.datetime64("2016-09-28"),
    np.datetime64("2015-09-28"),
    np.datetime64("2014-09-28"),
    np.datetime64("2013-09-28"),
    np.datetime64("2012-09-28"),
]

PATH = list(Path().cwd().parent.glob("**/private.yaml"))[0]
NFL_WEEKS = nfl_weeks_pull()
GAME_KEYS = game_keys_pull(first="no")

for today in dates:

    SEASON = today.astype("datetime64[Y]").item().year
    LEAGUE_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["league_ID"].values[0]
    GAME_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["game_id"].values[0]
    nfl_weeks_list = list(NFL_WEEKS["week"][NFL_WEEKS["game_id"] == GAME_ID])

    try:
        with open(PATH) as file:
            credentials = yaml.load(file, Loader=yaml.FullLoader)

    except Exception as error:
        print(error)

    CONSUMER_KEY = credentials["YFPY_CONSUMER_KEY"]
    CONSUMER_SECRET = credentials["YFPY_CONSUMER_SECRET"]

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

    if int(SEASON) == 2020:
        league.all_game_keys()
        league.all_nfl_weeks()
        league.metadata(first_time="yes")
        league.set_roster_pos_stat_cat(first_time="yes")
        league.draft_results(first_time="yes")
        league.teams_and_standings(first_time="yes")
        # league.players_list(first_time="yes")

        league.matchups_by_week_regseason(first_time="yes", nfl_week=1)
        league.team_roster_by_week(first_time="yes", nfl_week=1)
        league.team_points_by_week(first_time="yes", nfl_week=1)
        for week in nfl_weeks_list[1:]:
            league.matchups_by_week_regseason(first_time="no", nfl_week=week)
            league.team_roster_by_week(first_time="no", nfl_week=week)
            league.team_points_by_week(first_time="no", nfl_week=week)
            sleep(15)

    else:
        league.metadata(first_time="no")
        league.set_roster_pos_stat_cat(first_time="no")
        league.draft_results(first_time="no")
        league.teams_and_standings(first_time="no")
        # league.players_list(first_time="no")
        
        for week in nfl_weeks_list:
            league.matchups_by_week_regseason(first_time="no", nfl_week=week)
            league.team_roster_by_week(first_time="no", nfl_week=week)
            league.team_points_by_week(first_time="no", nfl_week=week)
