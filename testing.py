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


dates = [
    np.datetime64("2021-09-08", "D"),
    # np.datetime64("2021-09-09", "D"),
    # np.datetime64("2021-09-12", "D"),
    # np.datetime64("2021-09-13", "D"),
    # np.datetime64("2021-09-14", "D"),
    # np.datetime64("2021-09-20", "D"),
    # np.datetime64("2021-09-27", "D"),
    # np.datetime64("2021-10-04", "D"),
    # np.datetime64("2021-10-11", "D"),
    # np.datetime64("2021-10-18", "D"),
    # np.datetime64("2021-10-25", "D"),
    # np.datetime64("2021-11-01", "D"),
    # np.datetime64("2021-11-08", "D"),
    # np.datetime64("2021-11-15", "D"),
    # np.datetime64("2021-11-22", "D"),
    # np.datetime64("2021-11-29", "D"),
    # np.datetime64("2021-12-06", "D"),
    # np.datetime64("2021-12-13", "D"),
    # np.datetime64("2021-12-20", "D"),
    # np.datetime64("2021-12-27", "D"),
    # np.datetime64("2022-01-03", "D"),
    # np.datetime64("2022-01-04", "D"),
    # np.datetime64("2022-01-09", "D"),
]

for TODAY in dates:
    try:
        NFL_WEEKS = nfl_weeks_pull()
        GAME_KEYS = game_keys_pull(first="no")
        SEASON = get_season(TODAY)
        GAME_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["game_id"].values[0]
        LEAGUE_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["league_id"].values[0]

        NFL_WEEKS = NFL_WEEKS[(NFL_WEEKS['game_id'] == GAME_ID)]
        MAX_WEEK = NFL_WEEKS["week"].max()
        season_start = np.datetime64(NFL_WEEKS["start"][(NFL_WEEKS["week"] == 1)].values[0]) - np.timedelta64(1, 'D')
        season_end = np.datetime64(NFL_WEEKS["end"][(NFL_WEEKS["week"] == MAX_WEEK-1)].values[0]) + np.timedelta64(1, 'D')
        
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
            at_line="62",
        )

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

    

    if (TODAY == season_start):
        game_keys = league.all_game_keys()
        nfl_weeks = league.all_nfl_weeks()
        meta = league.metadata(first_time="no")
        settings, roster_pos, stat_cat = league.set_roster_pos_stat_cat(first_time="no")
        teams = league.teams_and_standings(first_time="no")
        draft = league.draft_results(first_time="no")
        # players = league.players_list(first_time="no")

    if (TODAY == season_end):
        teams = league.teams_and_standings(first_time="no")

    if not np.isnan(NFL_WEEK):
        week_roster = league.team_roster_by_week(first_time="no", nfl_week=NFL_WEEK)
        matchups = league.matchups_by_week(first_time="no", nfl_week=NFL_WEEK)
        team_points = league.team_points_by_week(first_time="no", nfl_week=NFL_WEEK)

    # sleep(600)
