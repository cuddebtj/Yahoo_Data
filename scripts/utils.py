import pandas as pd
import numpy as np
import math
import itertools
import logging
from pathlib import Path
from datetime import datetime as dt

from db_psql_model import DatabaseCursor
from tournament import Tournament


logging.basicConfig()
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)

PATH = list(Path().cwd().parent.glob("**/private.yaml"))[0]
OPTION_DEV = "-c search_path=dev"
OPTION_PROD = "-c search_path=prod"


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
        if str(first_time).upper() == "YES":
            df.drop_duplicates(ignore_index=True, inplace=True)
            df.sort_index(axis=1, inplace=True)
            DatabaseCursor(path, options=option).copy_table_to_postgres_new(
                df, table_name, first_time="YES"
            )

        elif str(first_time).upper() == "NO":
            psql = DatabaseCursor(path, options=option).copy_data_from_postgres(query)
            df = pd.concat([psql, df])
            df.drop_duplicates(ignore_index=True, inplace=True)
            DatabaseCursor(path, options=option).copy_table_to_postgres_new(
                df, table_name, first_time="NO"
            )

    except Exception as e:
        print(f"Error: {e}")


def reg_season(game_id):
    """
    Fucntion to calculate regular season rankings, scores, wins/losses, and matchups
    """
    matchups_query = f"SELECT * FROM dev.reg_season_matchups WHERE game_id = {game_id}"
    teams_query = f"SELECT team_key, name, nickname, game_id FROM dev.league_teams WHERE game_id = {game_id}"
    settings_query = f"SELECT * FROM dev.league_settings WHERE game_id = {game_id}"
    matchups = (
        DatabaseCursor(PATH, options=OPTION_DEV)
        .copy_data_from_postgres(matchups_query)
        .drop_duplicates()
    )

    teams = (
        DatabaseCursor(PATH, options=OPTION_DEV)
        .copy_data_from_postgres(teams_query)
        .drop_duplicates()
    )
    settings_query = f"SELECT * FROM dev.league_settings WHERE game_id = {game_id}"
    settings = (
        DatabaseCursor(PATH, options=OPTION_DEV)
        .copy_data_from_postgres(settings_query)
        .drop_duplicates()
    )

    # logic to help create playoff brackets
    playoff_start_week = settings["playoff_start_week"][
        settings["game_id"] == game_id
    ].values[0]

    # logic to create regular season board - matchups each week with running rankings
    one_reg_season = matchups[
        (matchups["game_id"] == game_id) & (matchups["week"] < playoff_start_week)
    ]
    one_reg_season["win_loss"] = np.where(
        one_reg_season["winner_team_key"] == one_reg_season["team_a_team_key"], "W", "L"
    )
    one_reg_season["pts_system_weekly_rank"] = one_reg_season.groupby(
        ["week", "game_id"]
    )["team_a_points"].rank("first", ascending=False)
    one_reg_season["pts_score"] = np.where(
        one_reg_season["pts_system_weekly_rank"] <= 5, 1, 0
    )
    one_reg_season = one_reg_season.merge(
        teams, how="left", left_on="team_a_team_key", right_on="team_key"
    )
    one_reg_season["team_a_name"] = one_reg_season["name"].fillna(
        one_reg_season["team_a_team_key"]
    )
    one_reg_season["team_a_nickname"] = one_reg_season["nickname"].fillna(
        one_reg_season["team_a_team_key"]
    )
    one_reg_season["game_id_a"] = one_reg_season["game_id_x"].fillna(
        one_reg_season["game_id_y"]
    )
    one_reg_season.drop(["name", "nickname"], axis=1, inplace=True)
    one_reg_season = one_reg_season.merge(
        teams, how="left", left_on="team_b_team_key", right_on="team_key"
    )
    one_reg_season["team_b_name"] = one_reg_season["name"].fillna(
        one_reg_season["team_b_team_key"]
    )
    one_reg_season["team_b_nickname"] = one_reg_season["nickname"].fillna(
        one_reg_season["team_b_team_key"]
    )
    one_reg_season.drop(["name", "nickname"], axis=1, inplace=True)
    one_reg_season["game_id"] = one_reg_season["game_id_a"]

    one_reg_season = one_reg_season.rename(
        columns={
            "team_a_team_key": "team_key",
            "team_a_name": "team_name",
            "team_a_nickname": "team_nickname",
            "team_a_points": "team_pts",
            "team_a_projected_points": "team_pro_pts",
            "team_b_team_key": "opp_team_key",
            "team_b_name": "opp_team_name",
            "team_b_nickname": "opp_team_nickname",
            "team_b_points": "opp_pts",
            "team_b_projected_points": "opp_pro_pts",
        }
    )

    one_reg_season = one_reg_season[
        [
            "game_id",
            "team_key",
            "team_name",
            "team_nickname",
            "week",
            "win_loss",
            "pts_score",
            "team_pts",
            "team_pro_pts",
            "opp_team_key",
            "opp_team_name",
            "opp_team_nickname",
            "opp_pts",
            "opp_pro_pts",
        ]
    ]

    one_reg_season["ttl_pts_for_run"] = one_reg_season.groupby(["team_key"])[
        "team_pts"
    ].cumsum()
    one_reg_season["pts_for_rank_run"] = (
        one_reg_season.groupby(["week"])["ttl_pts_for_run"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    one_reg_season["ttl_pro_pts_for_run"] = one_reg_season.groupby(["team_key"])[
        "team_pro_pts"
    ].cumsum()
    one_reg_season["pro_pts_for_rank_run"] = (
        one_reg_season.groupby(["week"])["ttl_pro_pts_for_run"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    one_reg_season["ttl_pts_agst_run"] = one_reg_season.groupby(["team_key"])[
        "opp_pts"
    ].cumsum()
    one_reg_season["pts_agst_rank_run"] = (
        one_reg_season.groupby(["week"])["ttl_pts_agst_run"]
        .rank(method="max", ascending=True)
        .astype(int)
    )

    one_reg_season["ttl_pro_pts_agst_run"] = one_reg_season.groupby(["team_key"])[
        "opp_pro_pts"
    ].cumsum()
    one_reg_season["pro_pts_agst_rank_run"] = (
        one_reg_season.groupby(["week"])["ttl_pro_pts_agst_run"]
        .rank(method="max", ascending=True)
        .astype(int)
    )

    one_reg_season["ttl_wins_run"] = (
        one_reg_season["win_loss"].eq("W").groupby(one_reg_season["team_key"]).cumsum()
    )
    one_reg_season["ttl_loss_run"] = (
        one_reg_season["win_loss"].eq("L").groupby(one_reg_season["team_key"]).cumsum()
    )
    one_reg_season["w_l_rank_run"] = (
        one_reg_season.groupby(["week"])["ttl_wins_run"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    one_reg_season["ttl_pts_score_run"] = one_reg_season.groupby(["team_key"])[
        "pts_score"
    ].cumsum()
    one_reg_season["ttl_2pt_score_run"] = (
        one_reg_season["ttl_pts_score_run"] + one_reg_season["ttl_wins_run"]
    )
    one_reg_season["2pt_score_rank_run"] = (
        one_reg_season.groupby(["week"])["ttl_2pt_score_run"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    if game_id >= 390:
        one_reg_season["tuple"] = one_reg_season[
            ["2pt_score_rank_run", "pts_for_rank_run"]
        ].apply(tuple, axis=1)
    else:
        one_reg_season["tuple"] = one_reg_season[
            ["w_l_rank_run", "pts_for_rank_run"]
        ].apply(tuple, axis=1)

    one_reg_season.insert(
        5,
        "reg_season_rank_run",
        one_reg_season.groupby(["week"])["tuple"]
        .rank(method="min", ascending=True)
        .astype(int),
    )

    one_reg_season.sort_values(
        ["week", "reg_season_rank_run"], ascending=[True, True], inplace=True
    )

    DatabaseCursor(PATH, options=OPTION_PROD).copy_table_to_postgres_new(
        df=one_reg_season, table=f"reg_season_board_{str(game_id)}", first_time="YES"
    )

    return one_reg_season


def post_season(one_reg_season, game_id, nfl_week):
    """
    Function to calculate post_season winners/losers, create final rank for the season
    """
    settings_query = f"SELECT * FROM dev.league_settings WHERE game_id = {game_id}"
    settings = (
        DatabaseCursor(PATH, options=OPTION_DEV)
        .copy_data_from_postgres(settings_query)
        .drop_duplicates()
    )
    meta_query = f"SELECT game_id, league_id, end_week FROM dev.league_metadata WHERE game_id = {game_id}"
    metadata = (
        DatabaseCursor(PATH, options=OPTION_DEV)
        .copy_data_from_postgres(meta_query)
        .drop_duplicates()
    )

    teams_query = f"SELECT team_key, name, nickname, game_id FROM dev.league_teams WHERE game_id = {game_id}"
    teams = (
        DatabaseCursor(PATH, options=OPTION_DEV)
        .copy_data_from_postgres(teams_query)
        .drop_duplicates()
    )

    weekly_points_query = f"SELECT * FROM dev.weekly_team_pts WHERE game_id = {game_id}"
    weekly_points = (
        DatabaseCursor(PATH, options=OPTION_DEV)
        .copy_data_from_postgres(weekly_points_query)
        .drop_duplicates()
    )

    settings = settings.merge(metadata, how="left", on=["game_id", "league_id"])

    # logic to help create playoff brackets
    playoff_start_week = settings["playoff_start_week"][
        settings["game_id"] == game_id
    ].values[0]
    playoff_end_week = settings["end_week"][settings["game_id"] == game_id].values[0]
    playoff_weeks = range(playoff_start_week, playoff_end_week + 1)
    max_teams = settings["max_teams"][settings["game_id"] == game_id].values[0]
    num_playoff_teams = settings["num_playoff_teams"][
        settings["game_id"] == game_id
    ].values[0]
    num_conso_teams = settings["num_playoff_consolation_teams"][
        settings["game_id"] == game_id
    ].values[0]
    max_teams = settings["max_teams"][settings["game_id"] == game_id].values[0]
    num_toliet_teams = (
        (
            max_teams - num_playoff_teams
            if num_playoff_teams >= (max_teams / 2)
            else num_playoff_teams
        )
        if num_conso_teams != 0
        else False
    )

    reg_season_rank = one_reg_season[
        ["game_id", "team_key", "team_name", "team_nickname", "reg_season_rank_run"]
    ].tail(max_teams)

    one_playoff_season = weekly_points[
        (weekly_points["game_id"] == game_id)
        & (weekly_points["week"] >= playoff_start_week)
    ]
    one_playoff_season = one_playoff_season.merge(
        teams, how="left", left_on="team_key", right_on="team_key"
    )
    one_playoff_season = one_playoff_season[
        [
            "game_id_x",
            "team_key",
            "name",
            "nickname",
            "week",
            "final_points",
            "projected_points",
        ]
    ]
    one_playoff_season = one_playoff_season.rename(
        columns={
            "game_id_x": "game_id",
            "name": "team_name",
            "nickname": "team_nickname",
            "final_points": "team_pts",
            "projected_points": "team_pro_pts",
        }
    )
    one_playoff_season.sort_values(["team_key", "week"], inplace=True)
    one_playoff_season["ttl_pts_for_run"] = one_playoff_season.groupby(["team_key"])[
        "team_pts"
    ].cumsum()
    one_playoff_season["ttl_pro_pts_for_run"] = one_playoff_season.groupby(
        ["team_key"]
    )["team_pro_pts"].cumsum()

    one_playoff_season = one_playoff_season.merge(
        reg_season_rank,
        how="left",
        on=["game_id", "team_key", "team_name", "team_nickname"],
    )

    one_playoff_season.rename(
        columns={"reg_season_rank_run": "reg_season_rank"}, inplace=True
    )

    one_playoff_season.sort_values(
        ["week", "reg_season_rank"], ascending=[True, True], inplace=True
    )

    playoff_teams = list(
        one_playoff_season["team_key"][
            one_playoff_season["reg_season_rank"] <= num_playoff_teams
        ].unique()
    )
    if num_toliet_teams:
        if game_id == 314:
            conso_teams = list(
                one_playoff_season["team_key"][
                    one_playoff_season["reg_season_rank"]
                    <= (max_teams - num_playoff_teams)
                ].unique()
            )
            conso_teams = [tm for tm in conso_teams if tm not in playoff_teams]
            toilet_teams = False

        elif game_id >= 406:
            conso_teams = list(
                one_playoff_season["team_key"][
                    one_playoff_season["reg_season_rank"] > num_playoff_teams
                ].unique()
            )
            toilet_teams = list(
                one_playoff_season["team_key"][
                    one_playoff_season["reg_season_rank"]
                    > (max_teams - num_conso_teams)
                ].unique()
            )
            conso_teams = [tm for tm in conso_teams if tm not in toilet_teams]

        else:
            conso_teams = False
            toilet_teams = list(
                one_playoff_season["team_key"][
                    one_playoff_season["reg_season_rank"]
                    > (max_teams - num_toliet_teams)
                ].unique()
            )
    else:
        toilet_teams = False
        conso_teams = False

    playoff_end_week_mask = one_playoff_season["week"] == playoff_end_week
    one_playoff_season.insert(1, "finish", np.nan)

    playoff_bracket = Tournament(playoff_teams)
    for week in playoff_weeks:
        for match in playoff_bracket.get_active_matches():
            right_comp = match.get_participants()[0].get_competitor()
            right_score = one_playoff_season["team_pts"][
                (one_playoff_season["team_key"] == right_comp)
                & (one_playoff_season["week"] == week)
            ].values[0]
            right_pro_score = one_playoff_season["team_pro_pts"][
                (one_playoff_season["team_key"] == right_comp)
                & (one_playoff_season["week"] == week)
            ].values[0]
            left_comp = match.get_participants()[1].get_competitor()
            left_score = one_playoff_season["team_pts"][
                (one_playoff_season["team_key"] == left_comp)
                & (one_playoff_season["week"] == week)
            ].values[0]
            left_pro_score = one_playoff_season["team_pro_pts"][
                (one_playoff_season["team_key"] == left_comp)
                & (one_playoff_season["week"] == week)
            ].values[0]
            one_playoff_season.loc[
                (one_playoff_season["team_key"] == right_comp)
                & (one_playoff_season["week"] == week),
                "po_opp",
            ] = left_comp
            one_playoff_season.loc[
                (one_playoff_season["team_key"] == right_comp)
                & (one_playoff_season["week"] == week),
                "po_opp_pts",
            ] = left_score
            one_playoff_season.loc[
                (one_playoff_season["team_key"] == right_comp)
                & (one_playoff_season["week"] == week),
                "po_opp_pro_pts",
            ] = left_pro_score
            one_playoff_season.loc[
                (one_playoff_season["team_key"] == left_comp)
                & (one_playoff_season["week"] == week),
                "po_opp",
            ] = right_comp
            one_playoff_season.loc[
                (one_playoff_season["team_key"] == left_comp)
                & (one_playoff_season["week"] == week),
                "po_opp_pts",
            ] = right_score
            one_playoff_season.loc[
                (one_playoff_season["team_key"] == left_comp)
                & (one_playoff_season["week"] == week),
                "po_opp_pro_pts",
            ] = right_pro_score
            if right_score > left_score:
                match.set_winner(right_comp)
            elif right_score < left_score:
                match.set_winner(left_comp)
    for rk, tm in playoff_bracket.get_final().items():
        one_playoff_season.loc[
            (one_playoff_season["team_key"] == tm) & playoff_end_week_mask, "finish"
        ] = int(rk)

    if conso_teams:
        if len(conso_teams) < len(playoff_teams):
            conso_bracket = Tournament(conso_teams)
            for week in playoff_weeks:
                for match in conso_bracket.get_active_matches():
                    right_comp = match.get_participants()[0].get_competitor()
                    right_score = one_playoff_season["team_pts"][
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1)
                    ].values[0]
                    right_pro_score = one_playoff_season["team_pro_pts"][
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1)
                    ].values[0]
                    left_comp = match.get_participants()[1].get_competitor()
                    left_score = one_playoff_season["team_pts"][
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1)
                    ].values[0]
                    left_pro_score = one_playoff_season["team_pro_pts"][
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1)
                    ].values[0]
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp",
                    ] = left_comp
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp_pts",
                    ] = left_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp_pro_pts",
                    ] = left_pro_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp",
                    ] = right_comp
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp_pts",
                    ] = right_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp_pro_pts",
                    ] = right_pro_score
                    if right_score > left_score:
                        match.set_winner(right_comp)
                    elif right_score < left_score:
                        match.set_winner(left_comp)
                    elif right_score == left_score:
                        match.set_winner(left_comp)

        else:
            conso_bracket = Tournament(conso_teams)
            for week in playoff_weeks:
                for match in conso_bracket.get_active_matches():
                    right_comp = match.get_participants()[0].get_competitor()
                    right_score = one_playoff_season["team_pts"][
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week)
                    ].values[0]
                    right_pro_score = one_playoff_season["team_pro_pts"][
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week)
                    ].values[0]
                    left_comp = match.get_participants()[1].get_competitor()
                    left_score = one_playoff_season["team_pts"][
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week)
                    ].values[0]
                    left_pro_score = one_playoff_season["team_pro_pts"][
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week)
                    ].values[0]
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp",
                    ] = left_comp
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp_pts",
                    ] = left_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp_pro_pts",
                    ] = left_pro_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp",
                    ] = right_comp
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp_pts",
                    ] = right_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp_pro_pts",
                    ] = right_pro_score
                    if right_score > left_score:
                        match.set_winner(right_comp)
                    elif right_score < left_score:
                        match.set_winner(left_comp)
                    elif right_score == left_score:
                        match.set_winner(left_comp)

        for rk, tm in conso_bracket.get_final().items():
            one_playoff_season.loc[
                (one_playoff_season["team_key"] == tm) & playoff_end_week_mask, "finish"
            ] = int(rk) + len(playoff_teams)

    if toilet_teams:
        if len(toilet_teams) < len(playoff_teams):
            toilet_bracket = Tournament(toilet_teams)
            for week in playoff_weeks:
                for match in toilet_bracket.get_active_matches():
                    right_comp = match.get_participants()[0].get_competitor()
                    right_score = one_playoff_season["team_pts"][
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1)
                    ].values[0]
                    right_pro_score = one_playoff_season["team_pro_pts"][
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1)
                    ].values[0]
                    left_comp = match.get_participants()[1].get_competitor()
                    left_score = one_playoff_season["team_pts"][
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1)
                    ].values[0]
                    left_pro_score = one_playoff_season["team_pro_pts"][
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1)
                    ].values[0]
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp",
                    ] = left_comp
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp_pts",
                    ] = left_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp_pro_pts",
                    ] = left_pro_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp",
                    ] = right_comp
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp_pts",
                    ] = right_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week + 1),
                        "po_opp_pro_pts",
                    ] = right_pro_score
                    if right_score > left_score:
                        match.set_winner(right_comp)
                    elif right_score < left_score:
                        match.set_winner(left_comp)
                    elif right_score == left_score:
                        match.set_winner(left_comp)

        else:
            toilet_bracket = Tournament(toilet_teams)
            for week in playoff_weeks:
                for match in toilet_bracket.get_active_matches():
                    right_comp = match.get_participants()[0].get_competitor()
                    right_score = one_playoff_season["team_pts"][
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week)
                    ].values[0]
                    right_pro_score = one_playoff_season["team_pro_pts"][
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week)
                    ].values[0]
                    left_comp = match.get_participants()[1].get_competitor()
                    left_score = one_playoff_season["team_pts"][
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week)
                    ].values[0]
                    left_pro_score = one_playoff_season["team_pro_pts"][
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week)
                    ].values[0]
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp",
                    ] = left_comp
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp_pts",
                    ] = left_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == right_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp_pro_pts",
                    ] = left_pro_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp",
                    ] = right_comp
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp_pts",
                    ] = right_score
                    one_playoff_season.loc[
                        (one_playoff_season["team_key"] == left_comp)
                        & (one_playoff_season["week"] == week),
                        "po_opp_pro_pts",
                    ] = right_pro_score
                    if right_score > left_score:
                        match.set_winner(right_comp)
                    elif right_score < left_score:
                        match.set_winner(left_comp)
                    elif right_score == left_score:
                        match.set_winner(left_comp)

        for rk, tm in toilet_bracket.get_final().items():
            one_playoff_season.loc[
                (one_playoff_season["team_key"] == tm) & playoff_end_week_mask, "finish"
            ] = (
                int(rk) + len(playoff_teams) + (len(conso_teams) if conso_teams else 0)
            )

    try:
        one_playoff_season.loc[
            playoff_end_week_mask, "finish"
        ] = one_playoff_season.loc[playoff_end_week_mask, "finish"].fillna(
            one_playoff_season["reg_season_rank"]
        )

        one_playoff_season.sort_values(["week", "finish"], inplace=True)

        DatabaseCursor(PATH, options=OPTION_PROD).copy_table_to_postgres_new(
            df=one_playoff_season,
            table=f"playoff_board_{str(game_id)}",
            first_time="YES",
        )
    except Exception as e:
        print("Error: Upload unsuccessful.", e, sep="\n")
