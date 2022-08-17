from csv import excel
import pandas as pd
import numpy as np
import logging
import time
import yaml
from psycopg2 import sql
from pathlib import Path
from yfpy import YahooFantasySportsQuery
from yfpy.utils import complex_json_handler, unpack_data
from yfpy import get_logger

from scripts.db_psql_model import DatabaseCursor
from scripts.utils import data_upload, log_print

# from db_psql_model import DatabaseCursor
# from utils import data_upload, log_print

PATH = list(Path().cwd().parent.glob("**/private.yaml"))[0]
TEAMS_FILE = list(Path().cwd().parent.glob("**/teams.yaml"))[0]


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
        """
        Pull League Metadata
        """
        try:
            try:
                response = complex_json_handler(self.yahoo_query.get_league_metadata())
            except Exception as e:
                if "Invalid week" in str(e):
                    return
                elif "token_expired" in str(e):
                    self.yahoo_query._authenticate()
                else:
                    log_print(error=e, module_="yahoo_query.py", func="metadata", game_id=self.game_id, first_time=first_time, sleep="1 hour before retrying")
                    time.sleep(3600)
                    try:
                        self.yahoo_query._authenticate()
                    except Exception as e:
                        log_print(error=e, module_="yahoo_query.py", func="metadata", game_id=self.game_id, first_time=first_time, sleep="30 min before 2nd retry")
                        time.sleep(1800)
                        self.yahoo_query._authenticate()

                response = complex_json_handler(self.yahoo_query.get_league_metadata())

            league_metadata = pd.json_normalize(response)
            league_metadata["game_id"] = self.game_id
            league_metadata.drop_duplicates(ignore_index=True, inplace=True)
            league_metadata = league_metadata[
                [
                    "game_id",
                    "league_id",
                    "name",
                    "num_teams",
                    "season",
                    "start_date",
                    "start_week",
                    "end_date",
                    "end_week",
                ]
            ]

            league_metadata["game_id"] = league_metadata["game_id"].astype(int)
            league_metadata["league_id"] = league_metadata["league_id"].astype(int)
            league_metadata["name"] = league_metadata["name"].astype(str)
            league_metadata["num_teams"] = league_metadata["num_teams"].astype(int)
            league_metadata["season"] = league_metadata["season"].astype(int)
            league_metadata["start_date"] = league_metadata["start_date"].astype("datetime64[D]")
            league_metadata["start_week"] = league_metadata["start_week"].astype(int)
            league_metadata["end_date"] = league_metadata["end_date"].astype("datetime64[D]")
            league_metadata["end_week"] = league_metadata["end_week"].astype(int)

            query = (
                'SELECT "game_id"'
                ', "league_id"'
                ', "name"'
                ', "num_teams"'
                ', "season"'
                ', "start_date"'
                ', "start_week"'
                ', "end_date"'
                ', "end_week" '
                "FROM raw.league_metadata "
                "WHERE (game_id <> '"+str(self.game_id)+"') "
                'GROUP BY "game_id"'
                ', "league_id"'
                ', "name"'
                ', "num_teams"'
                ', "season"'
                ', "start_date"'
                ', "start_week"'
                ', "end_date"'
                ', "end_week" '
                'ORDER BY "game_id"'
                ', "league_id" '
            )

            data_upload(
                df=league_metadata,
                first_time=first_time,
                table_name="league_metadata",
                query=query,
                path=PATH,
                option_schema='raw',
            )

            return league_metadata

        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="metadata", game_id=self.game_id, first_time=first_time)

    def set_roster_pos_stat_cat(self, first_time="no"):
        """
        Get Roster Positions, Stat Categories, and League Settigns
        """
        try:
            try:
                response = complex_json_handler(self.yahoo_query.get_league_settings())
            except Exception as e:
                if "Invalid week" in str(e):
                    return
                elif "token_expired" in str(e):
                    self.yahoo_query._authenticate()
                else:
                    log_print(error=e, module_="yahoo_query.py", func="set_roster_pos_stat_cat", game_id=self.game_id, first_time=first_time, sleep="1 hour before retrying")
                    time.sleep(3600)
                    try:
                        self.yahoo_query._authenticate()
                    except Exception as e:
                        log_print(error=e, module_="yahoo_query.py", func="set_roster_pos_stat_cat", game_id=self.game_id, first_time=first_time, sleep="30 min before 2nd retry")
                        time.sleep(1800)
                        self.yahoo_query._authenticate()

                response = complex_json_handler(self.yahoo_query.get_league_settings())

            league_settings = pd.json_normalize(response)
            league_settings.drop(
                ["roster_positions", "stat_categories.stats", "stat_modifiers.stats"],
                axis=1,
                inplace=True,
            )

            league_settings["game_id"] = self.game_id
            league_settings["league_id"] = self.league_id
            league_settings["has_playoff_consolation_games"].fillna(0, inplace=True)
            league_settings["has_multiweek_championship"].fillna(0, inplace=True)
            league_settings.drop_duplicates(ignore_index=True, inplace=True)
            league_settings = league_settings[
                [
                    "game_id",
                    "league_id",
                    "has_multiweek_championship",
                    "max_teams",
                    "num_playoff_teams",
                    "has_playoff_consolation_games",
                    "num_playoff_consolation_teams",
                    "playoff_start_week",
                    "trade_end_date",
                ]
            ]

            league_settings["game_id"] = league_settings["game_id"].astype(int)
            league_settings["league_id"] = league_settings["league_id"].astype(int)
            league_settings["has_multiweek_championship"] = league_settings["has_multiweek_championship"].astype(int)
            league_settings["max_teams"] = league_settings["max_teams"].astype(int)
            league_settings["num_playoff_teams"] = league_settings["num_playoff_teams"].astype(int)
            league_settings["has_playoff_consolation_games"] = league_settings["has_playoff_consolation_games"].astype(int)
            league_settings["num_playoff_consolation_teams"] = league_settings["num_playoff_consolation_teams"].astype(int)
            league_settings["playoff_start_week"] = league_settings["playoff_start_week"].astype(int)
            league_settings["trade_end_date"] = league_settings["trade_end_date"].astype("datetime64[D]")

            query_1 = (
                'SELECT "game_id" '
                ', "league_id" '
                ', "has_multiweek_championship" '
                ', "max_teams"'
                ', "num_playoff_teams" '
                ', "has_playoff_consolation_games" '
                ', "num_playoff_consolation_teams" '
                ', "playoff_start_week" '
                ', "trade_end_date" '
                "FROM raw.league_settings "
                "WHERE (game_id <> '"+str(self.game_id)+"') "
                'GROUP BY "game_id"'
                ', "league_id"'
                ', "has_multiweek_championship"'
                ', "max_teams"'
                ', "num_playoff_teams"'
                ', "has_playoff_consolation_games"'
                ', "num_playoff_consolation_teams"'
                ', "playoff_start_week"'
                ', "trade_end_date" '
                'ORDER BY "game_id" '
                ', "league_id" '
            )

            data_upload(
                df=league_settings,
                first_time=first_time,
                table_name="league_settings",
                query=query_1,
                path=PATH,
                option_schema='raw',
            )

            roster_positions = pd.DataFrame()
            for r in response["roster_positions"]:
                row = pd.json_normalize(complex_json_handler(r["roster_position"]))
                roster_positions = pd.concat([roster_positions, row])

            roster_positions["game_id"] = self.game_id
            roster_positions["league_id"] = self.league_id
            roster_positions.drop_duplicates(ignore_index=True, inplace=True)
            roster_positions = roster_positions[
                ["game_id", "league_id", "position_type", "position", "count"]
            ]

            roster_positions["game_id"] = roster_positions["game_id"].astype(int)
            roster_positions["league_id"] = roster_positions["league_id"].astype(int)
            roster_positions["position_type"] = roster_positions["position_type"].astype(str)
            roster_positions["position"] = roster_positions["position"].astype(str)
            roster_positions["count"] = roster_positions["count"].astype(int)

            query_2 = (
                'SELECT "game_id"'
                ', "league_id" '
                ', "position_type" '
                ', "position" '
                ', "count" '
                "FROM raw.roster_positions "
                "WHERE (game_id <> '"+str(self.game_id)+"') "
                'GROUP BY "game_id"'
                ', "league_id"'
                ', "position_type"'
                ', "position" '
                ', "count" '
                'ORDER BY "game_id"'
                ', "league_id:'
            )

            data_upload(
                df=roster_positions,
                first_time=first_time,
                table_name="roster_positions",
                query=query_2,
                path=PATH,
                option_schema='raw',
            )

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
            stat_categories = stat_categories[
                [
                    "game_id",
                    "league_id",
                    "stat_id",
                    "name",
                    "display_name",
                    "is_only_display_stat",
                    "position_type",
                    "stat_modifier",
                ]
            ]

            stat_categories["game_id"] = stat_categories["game_id"].astype(int)
            stat_categories["league_id"] = stat_categories["league_id"].astype(int)
            stat_categories["stat_id"] = stat_categories["stat_id"].astype(int)
            stat_categories["name"] = stat_categories["name"].astype(str)
            stat_categories["display_name"] = stat_categories["display_name"].astype(str)
            stat_categories["is_only_display_stat"] = stat_categories["is_only_display_stat"].astype(int)
            stat_categories["position_type"] = stat_categories["position_type"].astype(str)
            stat_categories["stat_modifier"] = stat_categories["stat_modifier"].astype(float).round(decimals=2)

            query_3 = (
                'SELECT "game_id" '
                ', "league_id"'
                ', "stat_id"'
                ', "name"'
                ', "display_name"'
                ', "is_only_display_stat"'
                ', "position_type"'
                ', "stat_modifier"'
                "FROM raw.stat_categories "
                "WHERE (game_id <> '"+str(self.game_id)+"') "
                'GROUP BY "game_id"'
                ', "league_id"'
                ', "stat_id"'
                ', "name"'
                ', "display_name"'
                ', "is_only_display_stat"'
                ', "position_type"'
                ', "stat_modifier" '
                'ORDER BY "game_id"'
                ', "league_id"'
            )

            data_upload(
                df=stat_categories,
                first_time=first_time,
                table_name="stat_categories",
                query=query_3,
                path=PATH,
                option_schema='raw',
            )

            return league_settings, roster_positions, stat_categories
        
        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="set_roster_pos_stat_cat", game_id=self.game_id, first_time=first_time)
            # print(f"\n----ERROR yahoo_query.py: set_roster_pos_stat_cat\n----{self.game_id}--{self.league_id}\n----{e}\n")

    def players_list(self, first_time="no"):
        """
        
        """
        try:
            players = pd.DataFrame()
            try:
                response = self.yahoo_query.get_league_players()
            except Exception as e:
                if "token_expired" in str(e):
                    self.yahoo_query._authenticate()
                else:
                    log_print(error=e, module_="yahoo_query.py", func="players_list", game_id=self.game_id, first_time=first_time, sleep="1 hour before retrying")
                    time.sleep(3600)
                    try:
                        self.yahoo_query._authenticate()
                    except Exception as e:
                        log_print(error=e, module_="yahoo_query.py", func="players_list", game_id=self.game_id, first_time=first_time, sleep="30 min before 2nd retry")
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
                        log_print(error=e, module_="yahoo_query.py", func="draft_results --> player draft analysis", game_id=self.game_id, first_time=first_time, sleep="1 hour before retrying")
                        time.sleep(3600)
                        try:
                            self.yahoo_query._authenticate()
                        except Exception as e:
                            log_print(error=e, module_="yahoo_query.py", func="draft_results --> player draft analysis", game_id=self.game_id, first_time=first_time, sleep="30 min before 2nd retry")
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

            players = players[
                [
                    "game_id",
                    "league_id",
                    "player_id",
                    "player_key",
                    "position_type",
                    "display_position",
                    "eligible_positions",
                    "name.ascii_first",
                    "name.ascii_last",
                    "name.first",
                    "name.last",
                    "name.full",
                    "uniform_number",
                    "bye_weeks.week",
                    "draft_analysis.average_round",
                    "draft_analysis.average_pick",
                    "draft_analysis.average_cost",
                    "draft_analysis.percent_drafted",
                    "editorial_team_key",
                    "editorial_team_full_name",
                    "editorial_team_abbr",
                ]
            ]

            players["game_id"] = players["game_id"].astype(int)
            players["league_id"] = players["league_id"].astype(int)
            players["player_id"] = players["player_id"].astype(int)
            players["player_key"] = players["player_key"].astype(str)
            players["position_type"] = players["position_type"].astype(str)
            players["display_position"] = players["display_position"].astype(str)
            players["eligible_positions"] = players["eligible_positions"].astype(str)
            players["name.ascii_first"] = players["name.ascii_first"].astype(str)
            players["name.ascii_last"] = players["name.ascii_last"].astype(str)
            players["name.first"] = players["name.first"].astype(str)
            players["name.last"] = players["name.last"].astype(str)
            players["name.full"] = players["name.full"].astype(str)
            players["uniform_number"] = players["uniform_number"].astype(int)
            players["bye_weeks.week"] = players["bye_weeks.week"].astype(int)
            players["draft_analysis.average_round"] = players["draft_analysis.average_round"].astype(float).round(decimals=2)
            players["draft_analysis.average_pick"] =players["draft_analysis.average_pick"].astype(float).round(decimals=2)
            players["draft_analysis.average_cost"] = players["draft_analysis.average_cost"].astype(float).round(decimals=2)
            players["draft_analysis.percent_drafted"] = players["draft_analysis.percent_drafted"].astype(float).round(decimals=4)
            players["editorial_team_key"] = players["editorial_team_key"].astype(str)
            players["editorial_team_full_name"] = players["editorial_team_full_name"].astype(str)
            players["editorial_team_abbr"] = players["editorial_team_abbr"].astype(str)

            query = (
                'SELECT "game_id"'
                ',"league_id"'
                ',"player_id"'
                ',"player_key"'
                ',"position_type"'
                ',"display_position"'
                ',"eligible_positions"'
                ',"name.ascii_first"'
                ',"name.ascii_last"'
                ',"name.first"'
                ',"name.last"'
                ',"name.full"'
                ',"uniform_number"'
                ',"bye_weeks.week"'
                ',"draft_analysis.average_round"'
                ',"draft_analysis.average_pick"'
                ',"draft_analysis.average_cost"'
                ',"draft_analysis.percent_drafted"'
                ',"editorial_team_key"'
                ',"editorial_team_full_name"'
                ',"editorial_team_abbr" '
                "FROM raw.player_list "
                "WHERE (game_id <> '"+str(self.game_id)+"') "
                'GROUP BY "game_id"'
                ',"league_id"'
                ',"player_id"'
                ',"player_key"'
                ',"position_type"'
                ',"display_position"'
                ',"eligible_positions"'
                ',"name.ascii_first"'
                ',"name.ascii_last"'
                ',"name.first"'
                ',"name.last"'
                ',"name.full"'
                ',"uniform_number"'
                ',"bye_weeks.week"'
                ',"draft_analysis.average_round"'
                ',"draft_analysis.average_pick"'
                ',"draft_analysis.average_cost"'
                ',"draft_analysis.percent_drafted"'
                ',"editorial_team_key"'
                ',"editorial_team_full_name"'
                ',"editorial_team_abbr" '
                'ORDER BY "game_id"'
                ', "league_id"'
                ', "player_id"'
                ', "editorial_team_key"'
            )

            data_upload(
                df=players,
                first_time=first_time,
                table_name="player_list",
                query=query,
                path=PATH,
                option_schema='raw',
            )

            return players

        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="players_list", game_id=self.game_id, first_time=first_time)
            # print(f"\n----ERROR yahoo_query.py: players_list.\n----{self.game_id}--{self.league_id}\n----{e}\n")

    def draft_results(self, first_time="no"):
        """
        
        """
        try:
            try:
                response = self.yahoo_query.get_league_draft_results()
            except Exception as e:
                if "Invalid week" in str(e):
                    return
                elif "token_expired" in str(e):
                    self.yahoo_query._authenticate()
                else:
                    log_print(error=e, module_="yahoo_query.py", func="draft_results", game_id=self.game_id, first_time=first_time, sleep="1 hour before retrying")
                    time.sleep(3600)
                    try:
                        self.yahoo_query._authenticate()
                    except Exception as e:
                        log_print(error=e, module_="yahoo_query.py", func="draft_results", game_id=self.game_id, first_time=first_time, sleep="30 min before 2nd retry")
                        time.sleep(1800)
                        self.yahoo_query._authenticate()

                response = self.yahoo_query.get_league_draft_results()

            draft_results = pd.DataFrame()
            for r in response:
                row = pd.json_normalize(complex_json_handler(r["draft_result"]))
                draft_results = pd.concat([draft_results, row])

            draft_results["game_id"] = self.game_id
            draft_results["league_id"] = self.league_id
            draft_results.drop_duplicates(ignore_index=True, inplace=True)
            draft_results = draft_results[
                ["game_id", "league_id", "round", "pick", "player_key", "team_key"]
            ]

            draft_results["game_id"] = draft_results["game_id"].astype(int)
            draft_results["league_id"] = draft_results["league_id"].astype(int)
            draft_results["round"] = draft_results["round"].astype(int)
            draft_results["pick"] = draft_results["pick"].astype(int)
            draft_results["player_key"] = draft_results["player_key"].astype(str)
            draft_results["team_key"] = draft_results["team_key"].astype(str)

            query = (
                'SELECT "game_id"'
                ', "league_id"'
                ', "round"'
                ', "pick"'
                ', "player_key"'
                ', "team_key" '
                "FROM raw.draft_results "
                "WHERE (game_id <> '"+str(self.game_id)+"') "
                'GROUP BY "game_id"'
                ', "league_id"'
                ', "round"'
                ', "pick"'
                ', "player_key"'
                ', "team_key" '
                'ORDER BY "game_id"'
                ', "league_id"'
                ', "pick"'
            )

            data_upload(
                df=draft_results,
                first_time=first_time,
                table_name="draft_results",
                query=query,
                path=PATH,
                option_schema='raw',
            )

            return draft_results

        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="draft_results", game_id=self.game_id, first_time=first_time)
            # print(f"\n----ERROR yahoo_query.py: draft_results.\n----{self.game_id}--{self.league_id}\n----{e}\n")

    def matchups_by_week(self, first_time="no", nfl_week=None):
        """
        
        """
        try:
            if nfl_week == None:
                print("\n----ERROR yahoo_query.py: matchups_by_week\n----Please include nfl_week in class creation\n")

            else:
                m = []
                team_a = pd.DataFrame()
                team_b = pd.DataFrame()

                try:
                    response = self.yahoo_query.get_league_matchups_by_week(nfl_week)
                except Exception as e:
                    if "Invalid week" in str(e):
                        return
                    elif "scoreboard" in str(e):
                        return
                    elif "token_expired" in str(e):
                        self.yahoo_query._authenticate()
                    else:
                        log_print(error=e, module_="yahoo_query.py", func="matchups_by_week", game_id=self.game_id, nfl_week=nfl_week, first_time=first_time, sleep="1 hour before retrying")
                        time.sleep(3600)
                        try:
                            self.yahoo_query._authenticate()
                        except Exception as e:
                            log_print(error=e, module_="yahoo_query.py", func="matchups_by_week", game_id=self.game_id, nfl_week=nfl_week, first_time=first_time, sleep="30 min before 2nd retry")
                            time.sleep(1800)
                            self.yahoo_query._authenticate()

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
                        team_a["grade"] = ""
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
                        team_b["grade"] = ""
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
                matchups["is_playoffs"].fillna(0, inplace=True)
                matchups["is_consolation"].fillna(0, inplace=True)
                matchups["is_tied"].fillna(0, inplace=True)

            matchups = matchups[
                [
                    "game_id",
                    "is_consolation",
                    "is_playoffs",
                    "is_tied",
                    "league_id",
                    "team_a_grade",
                    "team_a_points",
                    "team_a_projected_points",
                    "team_a_team_key",
                    "team_b_grade",
                    "team_b_points",
                    "team_b_projected_points",
                    "team_b_team_key",
                    "week",
                    "week_start",
                    "week_end",
                    "winner_team_key",
                ]
            ]

            matchups["game_id"] = matchups["game_id"].astype(int)
            matchups["league_id"] = matchups["league_id"].astype(int)
            matchups["week"] = matchups["week"].astype(int)
            matchups["week_start"] = matchups["week_start"].astype("datetime64[D]")
            matchups["week_end"] = matchups["week_end"].astype("datetime64[D]")
            matchups["is_playoffs"] = matchups["is_playoffs"].astype(int)
            matchups["is_consolation"] = matchups["is_consolation"].astype(int)
            matchups["is_tied"] = matchups["is_tied"].astype(int)
            matchups["team_a_team_key"] = matchups["team_a_team_key"].astype(str)
            matchups["team_a_points"] = matchups["team_a_points"].astype(float).round(decimals=2)
            matchups["team_a_projected_points"] = matchups["team_a_projected_points"].astype(float).round(decimals=2)
            matchups["team_b_team_key"] = matchups["team_b_team_key"].astype(str)
            matchups["team_b_points"] = matchups["team_b_points"].astype(float).round(decimals=2)
            matchups["team_b_projected_points"] = matchups["team_b_projected_points"].astype(float).round(decimals=2)
            matchups["winner_team_key"] = matchups["winner_team_key"].astype(str)
            matchups["team_a_grade"] = matchups["team_a_grade"].astype(str)
            matchups["team_b_grade"] = matchups["team_b_grade"].astype(str)

            query = (
                'SELECT "game_id"'
                ', "league_id"'
                ', "week"'
                ', "week_start"'
                ', "week_end"'
                ', "is_playoffs"'
                ', "is_consolation"'
                ', "is_tied"'
                ', "team_a_team_key"'
                ', "team_a_points"'
                ', "team_a_projected_points"'
                ', "team_b_team_key"'
                ', "team_b_points"'
                ', "team_b_projected_points"'
                ', "winner_team_key"'
                ', "team_a_grade"'
                ', "team_b_grade"'
                "FROM raw.weekly_matchups "
                "WHERE (game_id <> '"+str(self.game_id)+"' AND week <> '"+str(nfl_week)+"') "
                'GROUP BY "game_id"'
                ', "league_id"'
                ', "week"'
                ', "week_start"'
                ', "week_end"'
                ', "is_playoffs"'
                ', "is_consolation"'
                ', "is_tied"'
                ', "team_a_team_key"'
                ', "team_a_points"'
                ', "team_a_projected_points"'
                ', "team_b_team_key"'
                ', "team_b_points"'
                ', "team_b_projected_points"'
                ', "winner_team_key"'
                ', "team_a_grade"'
                ', "team_b_grade" '
                'ORDER BY "game_id"'
                ', "league_id"'
                ', "week"'
                ', "team_a_team_key"'
            )

            data_upload(
                df=matchups,
                first_time=first_time,
                table_name=f"weekly_matchups",
                query=query,
                path=PATH,
                option_schema='raw',
            )

            print(self.game_id, nfl_week)

            return matchups

        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="matchups_by_week", game_id=self.game_id, nfl_week=nfl_week, first_time=first_time)
            # print(f"\n----ERROR yahoo_query.py: matchups_by_week.\n----{nfl_week}--{self.game_id}--{self.league_id}\n----{e}\n")

    def teams_and_standings(self, first_time="no"):
        """
        
        """
        try:
            try:
                response = self.yahoo_query.get_league_standings()
            except Exception as e:
                if "Invalid week" in str(e):
                    return
                elif "token_expired" in str(e):
                    self.yahoo_query._authenticate()
                else:
                    log_print(error=e, module_="yahoo_query.py", func="teams_and_standigns", game_id=self.game_id, first_time=first_time, sleep="1 hour before retrying")
                    time.sleep(3600)
                    try:
                        self.yahoo_query._authenticate()
                    except Exception as e:
                        log_print(error=e, module_="yahoo_query.py", func="teams_and_standings", game_id=self.game_id, first_time=first_time, sleep="30 min before 2nd retry")
                        time.sleep(1800)
                        self.yahoo_query._authenticate()
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

            if "draft_grade" not in teams_standings.columns:
                teams_standings["draft_grade"] = ""

            if "faab_balance" not in teams_standings.columns:
                teams_standings["faab_balance"] = 0

            teams_standings["game_id"] = self.game_id
            teams_standings["league_id"] = self.league_id
            teams_standings["clinched_playoffs"].fillna(0, inplace=True)

            with open(TEAMS_FILE, "r") as file:
                c_teams = yaml.load(file, Loader=yaml.SafeLoader)

            corrected_teams = pd.DataFrame()

            for team in c_teams["name"]:
                team = pd.DataFrame(team)
                corrected_teams = pd.concat([corrected_teams, team])

            correct_teams = pd.melt(corrected_teams, var_name="nickname", value_name="name")
            correct_teams = correct_teams[~correct_teams["name"].isna()]
            teams_standings = teams_standings.merge(
                correct_teams,
                how="outer",
                left_on="name",
                right_on="name",
                suffixes=("_drop", ""),
            )

            teams_standings["nickname"] = teams_standings["nickname"].fillna(
                teams_standings["nickname_drop"]
            )

            teams_standings["nickname"] = np.where(
                teams_standings["nickname"] == "--hidden--",
                teams_standings["team_key"],
                teams_standings["nickname"],
            )

            teams_standings = teams_standings[
                teams_standings.columns.drop(list(teams_standings.filter(regex="_drop")))
            ]

            teams_standings.dropna(
                subset=["game_id", "league_id", "manager_id", "team_key"], inplace=True
            )

            teams_standings = teams_standings[
                [
                    "game_id",
                    "league_id",
                    "team_id",
                    "team_key",
                    "manager_id",
                    "clinched_playoffs",
                    "draft_grade",
                    "faab_balance",
                    "name",
                    "nickname",
                    "number_of_moves",
                    "number_of_trades",
                    "team_standings.playoff_seed",
                    "team_standings.rank",
                    "team_standings.outcome_totals.wins",
                    "team_standings.outcome_totals.losses",
                    "team_standings.outcome_totals.ties",
                    "team_standings.outcome_totals.percentage",
                    "team_standings.points_for",
                    "team_standings.points_against",
                ]
            ]

            teams_standings["game_id"] = teams_standings["game_id"].astype(int)
            teams_standings["league_id"] = teams_standings["league_id"].astype(int)
            teams_standings["team_id"] = teams_standings["team_id"].astype(int)
            teams_standings["team_key"] = teams_standings["team_key"].astype(str)
            teams_standings["manager_id"] = teams_standings["manager_id"].astype(int)
            teams_standings["clinched_playoffs"] = teams_standings["clinched_playoffs"].astype(int)
            teams_standings["draft_grade"] = teams_standings["draft_grade"].astype(str)
            teams_standings["faab_balance"] = teams_standings["faab_balance"].astype(int)
            teams_standings["name"] = teams_standings["name"].astype(str)
            teams_standings["number_of_moves"] = teams_standings["number_of_moves"].astype(int)
            teams_standings["number_of_trades"] = teams_standings["number_of_trades"].astype(int)
            teams_standings["team_standings.playoff_seed"] = teams_standings["team_standings.playoff_seed"].astype(int)
            teams_standings["team_standings.rank"] = teams_standings["team_standings.rank"].astype(int)
            teams_standings["team_standings.outcome_totals.wins"] = teams_standings["team_standings.outcome_totals.wins"].astype(float).round(decimals=2)
            teams_standings["team_standings.outcome_totals.losses"] = teams_standings["team_standings.outcome_totals.losses"].astype(int)
            teams_standings["team_standings.outcome_totals.ties"] = teams_standings["team_standings.outcome_totals.ties"].astype(int)
            teams_standings["team_standings.outcome_totals.percentage"] = teams_standings["team_standings.outcome_totals.percentage"].astype(float).round(decimals=4)
            teams_standings["team_standings.points_for"] = teams_standings["team_standings.points_for"].astype(float).round(decimals=2)
            teams_standings["team_standings.points_against"] = teams_standings["team_standings.points_against"].astype(float).round(decimals=2)

            query = (
                'SELECT "game_id"'
                ',"league_id"'
                ',"team_id"'
                ',"team_key"'
                ',"manager_id"'
                ',"clinched_playoffs"'
                ',"draft_grade"'
                ',"faab_balance"'
                ',"name"'
                ',"nickname"'
                ',"number_of_moves"'
                ',"number_of_trades"'
                ',"team_standings.playoff_seed"'
                ',"team_standings.rank"'
                ',"team_standings.outcome_totals.wins"'
                ',"team_standings.outcome_totals.losses"'
                ',"team_standings.outcome_totals.ties"'
                ',"team_standings.outcome_totals.percentage"'
                ',"team_standings.points_for"'
                ',"team_standings.points_against" '
                "FROM raw.league_teams "
                "WHERE (game_id <> '"+str(self.game_id)+"') "
                'GROUP BY "game_id"'
                ',"league_id"'
                ',"team_id"'
                ',"team_key"'
                ',"manager_id"'
                ',"clinched_playoffs"'
                ',"draft_grade"'
                ',"faab_balance"'
                ',"name"'
                ',"nickname"'
                ',"number_of_moves"'
                ',"number_of_trades"'
                ',"team_standings.playoff_seed"'
                ',"team_standings.rank"'
                ',"team_standings.outcome_totals.wins"'
                ',"team_standings.outcome_totals.losses"'
                ',"team_standings.outcome_totals.ties"'
                ',"team_standings.outcome_totals.percentage"'
                ',"team_standings.points_for"'
                ',"team_standings.points_against" '
                'ORDER BY "game_id"'
                ', "league_id"'
                ', "team_id"'
            )

            data_upload(
                df=teams_standings,
                first_time=first_time,
                table_name="league_teams",
                query=query,
                path=PATH,
                option_schema='raw',
            )

            return teams_standings
        
        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="teams_and_standings", game_id=self.game_id, first_time=first_time)
            # print(f"\n----ERROR yahoo_query.py: teams_and_standings\n----{self.game_id}--{self.league_id}\n----{e}\n")

    def team_roster_by_week(self, first_time="no", nfl_week=None):
        """
        
        """
        try:
            sql_query = f"SELECT max_teams FROM raw.league_settings WHERE game_id = '{self.game_id}'"
            teams = DatabaseCursor(PATH, option_schema='raw').copy_data_from_postgres(
                sql_query
            )
            teams = teams["max_teams"].values[0]

            team_week_rosters = pd.DataFrame()

            for team in range(1, teams + 1):
                try:
                    response = complex_json_handler(
                        self.yahoo_query.get_team_roster_by_week(str(team), nfl_week)
                    )

                except Exception as e:
                    if "Invalid week" in str(e):
                        return
                    elif "token_expired" in str(e):
                        self.yahoo_query._authenticate()
                    else:
                        log_print(error=e, module_="yahoo_query.py", func="team_roster_by_week", game_id=self.game_id, nfl_week=nfl_week, first_time=first_time, sleep="1 hour before retrying")
                        time.sleep(3600)
                        try:
                            self.yahoo_query._authenticate()
                        except Exception as e:
                            log_print(error=e, module_="yahoo_query.py", func="team_roster_by_week", game_id=self.game_id, nfl_week=nfl_week, first_time=first_time, sleep="30 min before 2nd retry")
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

            team_week_rosters = team_week_rosters[
                [
                    "game_id",
                    "league_id",
                    "week",
                    "team_id",
                    "selected_position.position",
                    "player_id",
                    "player_key",
                    "display_position",
                    "eligible_positions",
                    "position_type",
                ]
            ]

            team_week_rosters["game_id"] = team_week_rosters["game_id"].astype(int)
            team_week_rosters["league_id"] = team_week_rosters["league_id"].astype(int)
            team_week_rosters["week"] = team_week_rosters["week"].astype(int)
            team_week_rosters["team_id"] = team_week_rosters["team_id"].astype(int)
            team_week_rosters["selected_position.position"] = team_week_rosters["selected_position.position"].astype(str)
            team_week_rosters["player_id"] = team_week_rosters["player_id"].astype(int)
            team_week_rosters["player_key"] = team_week_rosters["player_key"].astype(str)
            team_week_rosters["display_position"] = team_week_rosters["display_position"].astype(str)
            team_week_rosters["eligible_positions"] = team_week_rosters["eligible_positions"].astype(str)
            team_week_rosters["position_type"] = team_week_rosters["position_type"].astype(str)

            query = (
                'SELECT "game_id"'
                ',"league_id"'
                ',"week"'
                ',"team_id"'
                ',"selected_position.position"'
                ',"player_id"'
                ',"player_key"'
                ',"display_position"'
                ',"eligible_positions"'
                ',"position_type" '
                "FROM raw.weekly_team_roster "
                "WHERE (game_id <> '"+str(self.game_id)+"' AND week <> '"+str(nfl_week)+"') "
                'GROUP BY "game_id"'
                ',"league_id"'
                ',"week"'
                ',"team_id"'
                ',"selected_position.position"'
                ',"player_id"'
                ',"player_key"'
                ',"display_position"'
                ',"eligible_positions"'
                ',"position_type" '
                'ORDER BY "game_id"'
                ', "league_id"'
                ', "week"'
                ', "team_id"'
                ', "selected_position.position"'
            )

            data_upload(
                df=team_week_rosters,
                first_time=first_time,
                table_name="weekly_team_roster",
                query=query,
                path=PATH,
                option_schema='raw',
            )

            print(self.game_id, nfl_week)

            return team_week_rosters

        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="team_roster_by_week", game_id=self.game_id, nfl_week=nfl_week, first_time=first_time)
            # print(f"\n----ERROR yahoo_query.py: team_roster_by_week\n----{nfl_week}--{self.game_id}--{self.league_id}\n----{e}\n")

    def team_points_by_week(self, first_time="no", nfl_week=None):
        """
        
        """
        try:
            sql_query = f"SELECT max_teams FROM raw.league_settings WHERE game_id = '{self.game_id}'"
            teams = DatabaseCursor(PATH, option_schema='raw').copy_data_from_postgres(
                sql_query
            )
            teams = teams["max_teams"].values[0]

            team_points_weekly = pd.DataFrame()
            for team in range(1, teams + 1):
                try:
                    response = self.yahoo_query.get_team_stats_by_week(str(team), nfl_week)

                except Exception as e:
                    if "Invalid week" in str(e):
                        return
                    elif "token_expired" in str(e):
                        self.yahoo_query._authenticate()
                    else:
                        log_print(error=e, module_="yahoo_query.py", func="team_points_by_week", game_id=self.game_id, nfl_week=nfl_week, first_time=first_time, sleep="1 hour before retrying")
                        time.sleep(3600)
                        try:
                            self.yahoo_query._authenticate()
                        except Exception as e:
                            log_print(error=e, module_="yahoo_query.py", func="team_points_by_week", game_id=self.game_id, nfl_week=nfl_week, first_time=first_time, sleep="30 min before 2nd retry")
                            time.sleep(1800)
                            self.yahoo_query._authenticate()
                    try:
                        response = complex_json_handler(
                            self.yahoo_query.get_team_stats_by_week(str(team), nfl_week)
                        )
                    except:
                        response = self.yahoo_query.get_team_stats_by_week(
                            str(team), nfl_week
                        )

                time.sleep(1)

                team_pts = pd.DataFrame()
                try:
                    ttl_pts = pd.json_normalize(
                        complex_json_handler(response["team_points"])
                    )
                except:
                    ttl_pts = pd.json_normalize(response["team_points"])
                ttl_pts = ttl_pts[["total", "week"]]
                ttl_pts.rename(columns={"total": "final_points"}, inplace=True)

                try:
                    pro_pts = pd.json_normalize(
                        complex_json_handler(response["team_projected_points"])
                    )
                except:
                    pro_pts = pd.json_normalize(response["team_projected_points"])

                pro_pts = pro_pts[["total"]]
                pro_pts.rename(columns={"total": "projected_points"}, inplace=True)
                team_pts = pd.concat([ttl_pts, pro_pts], axis=1)
                team_pts["team_id"] = team

                team_points_weekly = pd.concat([team_points_weekly, team_pts])

            team_points_weekly["game_id"] = self.game_id
            team_points_weekly["league_id"] = self.league_id
            team_points_weekly["team_key"] = (
                team_points_weekly["game_id"].astype(str)
                + ".l."
                + team_points_weekly["league_id"].astype(str)
                + ".t."
                + team_points_weekly["team_id"].astype(str)
            )

            team_points_weekly = team_points_weekly[
                [
                    "game_id",
                    "league_id",
                    "team_id",
                    "team_key",
                    "week",
                    "final_points",
                    "projected_points",
                ]
            ]

            team_points_weekly["game_id"] = team_points_weekly["game_id"].astype(int)
            team_points_weekly["league_id"] = team_points_weekly["league_id"].astype(int)
            team_points_weekly["team_id"] = team_points_weekly["team_id"].astype(int)
            team_points_weekly["team_key"] = team_points_weekly["team_key"].astype(str)
            team_points_weekly["week"] = team_points_weekly["week"].astype(int)
            team_points_weekly["final_points"] = team_points_weekly["final_points"].astype(float).round(decimals=2)
            team_points_weekly["projected_points"] = team_points_weekly["projected_points"].astype(float).round(decimals=2)

            query = (
                'SELECT "game_id"'
                ',"league_id"'
                ',"team_id"'
                ',"team_key"'
                ',"week"'
                ',"final_points"'
                ',"projected_points" '
                "FROM raw.weekly_team_pts "
                "WHERE (game_id <> '"+str(self.game_id)+"' AND week <> '"+str(nfl_week)+"') "
                'GROUP BY "game_id"'
                ',"league_id"'
                ',"team_id"'
                ',"team_key"'
                ',"week"'
                ',"final_points"'
                ',"projected_points" '
                'ORDER BY "game_id"'
                ', "league_id"'
                ', "week"'
                ', "team_id"'
            )

            data_upload(
                df=team_points_weekly,
                first_time=first_time,
                table_name="weekly_team_pts",
                query=query,
                path=PATH,
                option_schema='raw',
            )

            print(self.game_id, nfl_week)

            return team_points_weekly

        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="team_points_by_week", game_id=self.game_id, nfl_week=nfl_week, first_time=first_time)
            # print(f"\n----ERROR yahoo_query.py: team_points_by_week\n----{nfl_week}--{self.game_id}--{self.league_id}\n----{e}\n")

    def all_game_keys(self):
        """
        
        """
        try:
            response = unpack_data(self.yahoo_query.get_all_yahoo_fantasy_game_keys())
            try:
                league_keys = pd.read_csv(PATH.parent / "assests" / "game_keys.csv")
            except:
                league_keys = pd.DataFrame({"game_id": np.nan, "season": np.nan})

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
            game_keys = game_keys[
                [
                    "game_id",
                    "league_ID",
                    "season",
                    "is_game_over",
                    "is_offseason"
                ]
            ]
            game_keys.drop_duplicates(ignore_index=True, inplace=True)
            DatabaseCursor(PATH, option_schema='dev').copy_table_to_postgres_new(
                game_keys, "game_keys", first_time="yes"
            )

            return game_keys
        
        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="all_game_keys", game_id=self.game_id)
            # print(f"\n----ERROR yahoo_query.py: all_game_keys\n----{self.game_id}--{self.league_id}\n----{e}")

    def all_nfl_weeks(self):
        """
        
        """
        try:
            game_keys = DatabaseCursor(
                PATH, option_schema='dev'
            ).copy_data_from_postgres("SELECT game_id FROM dev.game_keys")
            game_id = list(game_keys["game_id"])
            weeks = pd.DataFrame()
            for g in game_id:
                response = self.yahoo_query.get_game_weeks_by_game_id(g)
                for r in response:
                    row = pd.json_normalize(complex_json_handler(r["game_week"]))
                    row["game_id"] = g
                    weeks = pd.concat([weeks, row])

            weeks.rename(columns={"display_name": "week"}, inplace=True)
            weeks = weeks[["week", "start", "end", "game_id"]]
            weeks = weeks.iloc[:, 1:]
            weeks["start"] = weeks["start"].astype("datetime64[D]")
            weeks["end"] = weeks["end"].astype("datetime64[D]")
            weeks.drop_duplicates(ignore_index=True, inplace=True)

            DatabaseCursor(PATH, option_schema='dev').copy_table_to_postgres_new(
                weeks, "nfl_weeks", first_time="yes"
            )

            return weeks
        
        except Exception as e:
            log_print(error=e, module_="yahoo_query.py", func="all_nfl_weeks", game_id=self.game_id)
            # print(f"\n----ERROR yahoo_query.py: all_nfl_weeks\n----{self.game_id}--{self.league_id}\n----{e}")
