import pandas as pd
import numpy as np
import logging
import time
from pyparsing import col
from sqlalchemy import column
from yfpy import YahooFantasySportsQuery
from yfpy.utils import complex_json_handler, unpack_data
from yfpy import get_logger

from utils import (
    sql_upload_table,
    sql_grab_table,
)


class league_season_data(object):

    LOGGET = get_logger(__name__)
    LOG_OUTPUT = False
    logging.getLogger("yfpy.query").setLevel(level=logging.INFO)

    def __init__(
        self,
        auth_dir=None,
        league_id=None,
        game_id=None,
        game_code="nfl",
        offline=False,
        all_output_as_json=False,
        consumer_key=None,
        consumer_secret=None,
        browser_callback=True,
    ):
        self._auth_dir = auth_dir
        self._consumer_key = str(consumer_key)
        self._consumer_secret = str(consumer_secret)
        self._browser_callback = browser_callback

        self.league_id = str(league_id)
        self.game_id = str(game_id)
        self.game_code = str(game_code)

        self.offline = offline
        self.all_output_as_json = all_output_as_json

        self.yahoo_query = YahooFantasySportsQuery(
            auth_dir=self._auth_dir,
            league_id=self.league_id,
            game_id=self.game_id,
            game_code=self.game_code,
            offline=self.offline,
            all_output_as_json=self.all_output_as_json,
            consumer_key=self._consumer_key,
            consumer_secret=self._consumer_secret,
            browser_callback=self._browser_callback,
        )

    def metadata(self, first_time="no"):
        response = complex_json_handler(self.yahoo_query.get_league_metadata())
        league_metadata = pd.json_normalize(response)

        league_metadata["game_id"] = self.game_id
        league_metadata.drop_duplicates(ignore_index=True, inplace=True)

        if str(first_time).upper() == "YES":
            sql_upload_table(
                dataframe=league_metadata,
                table_name="LeagueMetaData",
                data_schema="dbo",
                chunksize=500,
                if_exists="replace",
                index=False,
            )

        elif str(first_time).upper() == "NO":
            sql_upload_table(
                dataframe=league_metadata,
                table_name="LeagueMetaData",
                data_schema="dbo",
                chunksize=500,
                if_exists="append",
                index=False,
            )

        return league_metadata

    def set_roster_pos_stat_cat(self, first_time="no"):
        response = complex_json_handler(self.yahoo_query.get_league_settings())

        league_settings = pd.json_normalize(response)
        league_settings.drop(
            ["roster_positions", "stat_categories.stats", "stat_modifiers.stats"],
            axis=1,
            inplace=True,
        )

        league_settings["game_id"] = self.game_id
        league_settings["league_id"] = self.league_id
        league_settings.drop_duplicates(ignore_index=True, inplace=True)

        roster_positions = pd.DataFrame()
        for r in response["roster_positions"]:
            row = pd.json_normalize(complex_json_handler(r["roster_position"]))
            roster_positions = pd.concat([roster_positions, row])

        roster_positions["game_id"] = self.game_id
        roster_positions["league_id"] = self.league_id
        roster_positions.drop_duplicates(ignore_index=True, inplace=True)

        stat_categories = pd.DataFrame()
        for r in response["stat_categories"]["stats"]:
            row = pd.json_normalize(complex_json_handler(r["stat"]))
            try:
                row["position_type"] = complex_json_handler(
                    complex_json_handler(r["stat"])["stat_position_types"][
                        "stat_position_type"
                    ]
                )["position_type"]
            except:
                pass

            try:
                row["is_only_display_stat"] = complex_json_handler(
                    complex_json_handler(r["stat"])["stat_position_types"][
                        "stat_position_type"
                    ]
                )["is_only_display_stat"]
            except:
                row["is_only_display_stat"] = 0

            try:
                row.drop("stat_position_types.stat_position_type", axis=1, inplace=True)
            except:
                pass

            stat_categories = pd.concat([stat_categories, row])

        stat_categories["game_id"] = self.game_id
        stat_categories["league_id"] = self.league_id

        stat_modifiers = pd.DataFrame()
        for r in response["stat_modifiers"]["stats"]:
            row = pd.json_normalize(complex_json_handler(r["stat"]))
            stat_modifiers = pd.concat([stat_modifiers, row])

        stat_modifiers.rename(columns={"value": "stat_modifier"}, inplace=True)

        stat_categories = stat_categories.merge(
            stat_modifiers, how="outer", on="stat_id"
        )
        stat_categories.drop_duplicates(ignore_index=True, inplace=True)

        if str(first_time).upper() == "YES":
            sql_upload_table(
                dataframe=league_settings,
                table_name="LeagueSettings",
                data_schema="dbo",
                chunksize=500,
                if_exists="replace",
                index=False,
            )

            sql_upload_table(
                dataframe=roster_positions,
                table_name="RosterPositions",
                data_schema="dbo",
                chunksize=500,
                if_exists="replace",
                index=False,
            )

            sql_upload_table(
                dataframe=stat_categories,
                table_name="StatCategories",
                data_schema="dbo",
                chunksize=500,
                if_exists="replace",
                index=False,
            )

        elif str(first_time).upper() == "NO":
            sql_upload_table(
                dataframe=league_settings,
                table_name="LeagueSettings",
                data_schema="dbo",
                chunksize=500,
                if_exists="append",
                index=False,
            )

            sql_upload_table(
                dataframe=roster_positions,
                table_name="RosterPositions",
                data_schema="dbo",
                chunksize=500,
                if_exists="append",
                index=False,
            )

            sql_upload_table(
                dataframe=stat_categories,
                table_name="StatCategories",
                data_schema="dbo",
                chunksize=500,
                if_exists="append",
                index=False,
            )

        return league_settings, roster_positions, stat_categories

    def players_list(self, first_time="no"):
        players = pd.DataFrame()
        try:
            response = self.yahoo_query.get_league_players()
        except Exception as e:
            if "token_expired" in str(e):
                self.yahoo_query._authenticate()
            else:
                print("Retrying after sleeping 30 min.")
                time.sleep(1800)
                self.yahoo_query._authenticate()

            response = self.yahoo_query.get_league_players()

        for r in response:
            player = pd.json_normalize(complex_json_handler(r["player"]))
            try:
                draft_analysis = pd.json_normalize(
                    complex_json_handler(
                        self.yahoo_query.get_player_draft_analysis(
                            player["player_key"][0]
                        )
                    )
                )

            except Exception as e:
                if "token_expired" in str(e):
                    self.yahoo_query._authenticate()
                else:
                    print("Retrying after sleeping 30 min.")
                    time.sleep(1800)
                    self.yahoo_query._authenticate()

                draft_analysis = pd.json_normalize(
                    complex_json_handler(
                        self.yahoo_query.get_player_draft_analysis(
                            player["player_key"][0]
                        )
                    )
                )

            draft_analysis = draft_analysis[
                [
                    "draft_analysis.average_pick",
                    "draft_analysis.average_round",
                    "draft_analysis.average_cost",
                    "draft_analysis.percent_drafted",
                ]
            ]
            player = pd.concat([player, draft_analysis], axis=1)
            if "status" not in player.columns:
                player["status"] = np.nan

            players = pd.concat([players, player], ignore_index=True)
            time.sleep(2.5)

        players["eligible_positions"] = [
            ", ".join(map(str, l)) for l in players["eligible_positions"]
        ]
        players["game_id"] = self.game_id
        players["league_id"] = self.league_id

        players.drop_duplicates(
            subset=[
                "display_position",
                "editorial_player_key",
                "editorial_team_key",
                "player_id",
                "player_key",
            ],
            ignore_index=True,
            inplace=True,
        )
        if str(first_time).upper() == "YES":
            sql_upload_table(
                dataframe=players,
                table_name="Players",
                data_schema="dbo",
                chunksize=500,
                if_exists="replace",
                index=False,
            )

        elif str(first_time).upper() == "NO":
            sql_upload_table(
                dataframe=players,
                table_name="Players",
                data_schema="dbo",
                chunksize=500,
                if_exists="append",
                index=False,
            )

        return players

    def draft_results(self, first_time="no"):
        response = self.yahoo_query.get_league_draft_results()
        draft_results = pd.DataFrame()
        for r in response:
            row = pd.json_normalize(complex_json_handler(r["draft_result"]))
            draft_results = pd.concat([draft_results, row])

        draft_results["game_id"] = self.game_id
        draft_results["league_id"] = self.league_id
        draft_results.drop_duplicates(ignore_index=True, inplace=True)

        if str(first_time).upper() == "YES":
            sql_upload_table(
                dataframe=draft_results,
                table_name="DraftResults",
                data_schema="dbo",
                chunksize=500,
                if_exists="replace",
                index=False,
            )

        elif str(first_time).upper() == "NO":
            sql_upload_table(
                dataframe=draft_results,
                table_name="DraftResults",
                data_schema="dbo",
                chunksize=500,
                if_exists="append",
                index=False,
            )

        return draft_results

    def matchups_by_week(self, first_time="no", nfl_week=None):
        if nfl_week == None:
            print("Please include nfl_week in class creation")
        else:
            m = []
            team_a = pd.DataFrame()
            team_b = pd.DataFrame()
            response = self.yahoo_query.get_league_matchups_by_week(nfl_week)
            for data in response:
                m.append(complex_json_handler(data["matchup"]))
            matchups = pd.DataFrame()
            for r in m:
                matchup = pd.json_normalize(r)
                matchup = matchup[
                    [
                        "is_consolation",
                        "is_matchup_recap_available",
                        "is_playoffs",
                        "is_tied",
                        "matchup_recap_title",
                        "matchup_recap_url",
                        "status",
                        "week",
                        "week_end",
                        "week_start",
                        "winner_team_key",
                    ]
                ]
                try:
                    team_a = pd.json_normalize(
                        complex_json_handler(r["matchup_grades"][0]["matchup_grade"])
                    )
                    team_a["points"] = complex_json_handler(r["teams"][0]["team"])[
                        "team_points"
                    ]["total"]
                    team_a["projected_points"] = complex_json_handler(
                        r["teams"][0]["team"]
                    )["team_projected_points"]["total"]

                except:
                    team_a = pd.json_normalize(
                        complex_json_handler(r["teams"][0]["team"])
                    )
                    team_a = team_a[
                        ["team_key", "team_points.total", "team_projected_points.total"]
                    ]
                    team_a["grade"] = np.nan
                    team_a.rename(
                        columns={
                            "team_points.total": "points",
                            "team_projected_points.total": "projected_points",
                        },
                        inplace=True,
                    )

                team_a = team_a.add_prefix("team_a_")

                try:
                    team_b = pd.json_normalize(
                        complex_json_handler(r["matchup_grades"][1]["matchup_grade"])
                    )
                    team_b["points"] = complex_json_handler(r["teams"][1]["team"])[
                        "team_points"
                    ]["total"]
                    team_b["projected_points"] = complex_json_handler(
                        r["teams"][1]["team"]
                    )["team_projected_points"]["total"]

                except:
                    team_b = pd.json_normalize(
                        complex_json_handler(r["teams"][1]["team"])
                    )
                    team_b = team_b[
                        ["team_key", "team_points.total", "team_projected_points.total"]
                    ]
                    team_b["grade"] = np.nan
                    team_b.rename(
                        columns={
                            "team_points.total": "points",
                            "team_projected_points.total": "projected_points",
                        },
                        inplace=True,
                    )

                team_b = team_b.add_prefix("team_b_")

                matchup = pd.concat([matchup, team_a, team_b], axis=1)

                matchups = pd.concat([matchups, matchup])

            try:
                matchups.drop(["teams", "matchup_grades"], axis=1, inplace=True)

            except:
                pass

            matchups["game_id"] = self.game_id
            matchups["league_id"] = self.league_id
            matchups.drop_duplicates(inplace=True, ignore_index=True)

            if str(first_time).upper() == "YES":
                sql_upload_table(
                    dataframe=matchups,
                    table_name="Matchups",
                    data_schema="dbo",
                    chunksize=500,
                    if_exists="replace",
                    index=False,
                )

            elif str(first_time).upper() == "NO":
                sql_upload_table(
                    dataframe=matchups,
                    table_name="Matchups",
                    data_schema="dbo",
                    chunksize=500,
                    if_exists="append",
                    index=False,
                )

            return matchups

    def teams_and_standings(self, first_time="no"):
        response = self.yahoo_query.get_league_standings()
        teams = complex_json_handler(response)
        teams_standings = pd.DataFrame()
        for t in teams["teams"]:
            row = pd.json_normalize(complex_json_handler(t["team"]))
            if "managers.manager" not in row.columns:
                manager = pd.json_normalize(
                    complex_json_handler(row["managers"][0][0]["manager"])
                )
            else:
                manager = pd.json_normalize(
                    complex_json_handler(row["managers.manager"][0])
                )
            row = pd.concat([row, manager], axis=1)
            teams_standings = pd.concat([teams_standings, row])
        try:
            teams_standings.drop(columns=["managers"], inplace=True)
        except:
            pass
        try:
            teams_standings.drop(columns=["managers.manager"], inplace=True)
        except:
            pass
        try:
            teams_standings.drop(columns=["image_url"], inplace=True)
        except:
            pass
        try:
            teams_standings.drop(columns=["draft_recap_url"], inplace=True)
        except:
            pass
        try:
            teams_standings.drop(columns=["league_scoring_type"], inplace=True)
        except:
            pass
        try:
            teams_standings.drop(columns=["team_points.season"], inplace=True)
        except:
            pass
        try:
            teams_standings.drop(columns=["team_points.total"], inplace=True)
        except:
            pass
        teams_standings.drop(
            columns=[
                "url",
                "team_logos.team_logo",
                "guid",
                "roster_adds.coverage_type",
                "team_points.coverage_type",
            ],
            inplace=True,
        )
        teams_standings["name"] = teams_standings["name"].str.decode("utf-8")
        if "draft_position" in teams_standings.columns:
            teams_standings.drop(columns="draft_position", inplace=True)

        teams_standings["game_id"] = self.game_id
        teams_standings["league_id"] = self.league_id
        teams_standings.drop_duplicates(ignore_index=True, inplace=True)

        if str(first_time).upper() == "YES":
            sql_upload_table(
                dataframe=teams_standings,
                table_name="Teams",
                data_schema="dbo",
                chunksize=500,
                if_exists="replace",
                index=False,
            )

        elif str(first_time).upper() == "NO":
            sql_upload_table(
                dataframe=teams_standings,
                table_name="Teams",
                data_schema="dbo",
                chunksize=500,
                if_exists="append",
                index=False,
            )

        return teams_standings

    def team_roster_by_week(self, first_time="no", nfl_week=None):

        team_ids = sql_grab_table("Teams")
        team_ids = list(
            team_ids["team_id"][
                (team_ids["game_id"] == str(self.game_id))
                & (team_ids["league_id"] == str(self.league_id))
            ]
        )

        team_week_rosters = pd.DataFrame()
        for team in team_ids:
            try:
                response = complex_json_handler(
                    self.yahoo_query.get_team_roster_by_week(str(team), nfl_week)
                )

            except Exception as e:
                if "token expired" in str(e):
                    self.yahoo_query._authenticate()

                else:
                    print(f"Error, sleeping for 30 min before retrying.\n{e}")
                    time.sleep(1800)
                    self.yahoo_query._authenticate()

                response = complex_json_handler(
                    self.yahoo_query.get_team_roster_by_week(str(team), nfl_week)
                )

            team_roster = pd.DataFrame()
            time.sleep(2)

            for r in response["players"]:
                row = pd.json_normalize(complex_json_handler(r["player"]))
                team_roster = pd.concat([team_roster, row])
                team_roster["team_id"] = team
                team_roster["week"] = nfl_week

            team_week_rosters = pd.concat([team_week_rosters, team_roster])

        team_week_rosters["game_id"] = self.game_id
        team_week_rosters["league_id"] = self.league_id
        team_week_rosters["eligible_positions"] = [
            ", ".join(map(str, l)) for l in team_week_rosters["eligible_positions"]
        ]
        team_week_rosters.drop_duplicates(ignore_index=True, inplace=True)

        if str(first_time).upper() == "YES":
            sql_upload_table(
                dataframe=team_week_rosters,
                table_name="TeamRosters",
                data_schema="dbo",
                chunksize=500,
                if_exists="replace",
                index=False,
            )

        elif str(first_time).upper() == "NO":
            sql_upload_table(
                dataframe=team_week_rosters,
                table_name="TeamRosters",
                data_schema="dbo",
                chunksize=500,
                if_exists="append",
                index=False,
            )

        return team_week_rosters

    def player_stats_by_week(self, first_time="no", nfl_week=None):
        if nfl_week == None:
            print("Please include nfl_week in class creation")
        else:
            players = sql_grab_table(
                f"SELECT player_key FROM MenOfMadison.dbo.Players WHERE game_id = {self.game_id} AND league_id = {self.league_id}"
            )
            player_keys = list(players["player_key"])
            player_stats = pd.DataFrame()
            for k in player_keys:
                try:
                    r1 = self.yahoo_query.get_player_stats_by_week(k, nfl_week)
                    r2 = self.yahoo_query.get_player_percent_owned_by_week(k, nfl_week)

                except Exception as e:
                    if "token_expired" in str(e):
                        print(e)
                        self.yahoo_query._authenticate()
                    else:
                        print(
                            f"Error with player stats at player_key {k} and week {nfl_week}.\n{e}\nRetrying in 30 minutes."
                        )
                        time.sleep(1800)
                        self.yahoo_query._authenticate()

                    r1 = self.yahoo_query.get_player_stats_by_week(k, nfl_week)
                    r2 = self.yahoo_query.get_player_percent_owned_by_week(k, nfl_week)

                time.sleep(2.4)
                data = complex_json_handler(r1)
                player = pd.json_normalize(data)
                stats = data["player_stats"]["stats"]
                player_stat_week = pd.DataFrame()

                for r in stats:
                    stat = pd.json_normalize(complex_json_handler(r["stat"]))
                    stat["player_key"] = player["player_key"]
                    stat["week"] = player["player_stats.week"]
                    stat["total_points_week"] = player["player_points.total"]
                    player_stat_week = pd.concat([player_stat_week, stat])

                ownership = pd.json_normalize(complex_json_handler(r2))
                if "percent_owned.value" not in ownership.columns:
                    ownership["percent_owned.value"] = 0

                ownership = ownership[
                    ["player_key", "percent_owned.value", "percent_owned.delta"]
                ]

                player_stat_week = player_stat_week.merge(
                    ownership,
                    how="outer",
                    left_on="player_key",
                    right_on="player_key",
                )

            player_stats = pd.concat([player_stats, player_stat_week])

            player_stats["game_id"] = self.game_id
            player_stats["league_id"] = self.leauge_id
            player_stats.drop_duplicates(ignore_index=True, inplace=True)

            if str(first_time).upper() == "YES":
                sql_upload_table(
                    dataframe=player_stats,
                    table_name="PlayerStats",
                    data_schema="dbo",
                    chunksize=500,
                    if_exists="replace",
                    index=False,
                )

            elif str(first_time).upper() == "NO":
                sql_upload_table(
                    dataframe=player_stats,
                    table_name="PlayerStats",
                    data_schema="dbo",
                    chunksize=500,
                    if_exists="append",
                    index=False,
                )

            return player_stats

    def all_game_keys(self):
        response = unpack_data(self.yahoo_query.get_all_yahoo_fantasy_game_keys())
        league_keys = pd.read_csv("../assests/ID.csv")
        game_keys = pd.DataFrame()
        for r in response:
            row = pd.DataFrame(complex_json_handler(r["game"]), index=[0])
            game_keys = pd.concat([game_keys, row])

        game_keys.reset_index(drop=True, inplace=True)
        game_keys = game_keys[game_keys["season"] >= 2012]
        game_keys = game_keys.merge(
            league_keys,
            how="outer",
            left_on=["game_id", "season"],
            right_on=["game_id", "season"],
        )
        game_keys.drop_duplicates(ignore_index=True, inplace=True)

        sql_upload_table(
            dataframe=game_keys,
            table_name="GameKeys",
            data_schema="dbo",
            chunksize=500,
            if_exists="replace",
            index=False,
        )

        return game_keys

    def all_nfl_weeks(self):
        game_keys = sql_grab_table("GameKeys")
        game_id = list(game_keys["game_id"])
        weeks = pd.DataFrame()
        for g in game_id:
            response = self.yahoo_query.get_game_weeks_by_game_id(g)
            for r in response:
                row = pd.json_normalize(complex_json_handler(r["game_week"]))
                row["game_id"] = g
                weeks = pd.concat([weeks, row])

        weeks.rename(columns={"display_name": "week"}, inplace=True)
        weeks.drop_duplicates(ignore_index=True, inplace=True)

        sql_upload_table(
            dataframe=weeks,
            table_name="NFLWeeks",
            data_schema="dbo",
            chunksize=500,
            if_exists="replace",
            index=False,
        )

        return weeks
