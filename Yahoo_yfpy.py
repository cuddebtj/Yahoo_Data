__author__ = "Tim Cuddeback"

import yfpy, json
from yfpy.query import YahooFantasySportsQuery
import pandas as pd
pd.set_option('display.max_colwidth', None)

df_id = pd.read_csv("ID.csv", dtype="str")
season = "2012"
# season = "2013"
# season = "2014"
# season = "2015"
# season = "2016"
# season = "2017"
# season = "2018"
# season = "2019"
# season = "2020"
# season = "2021"

league_id = df_id[df_id["season"] == season]["league_ID"].values[0] # put your real league id here
game_id = df_id[df_id["season"] == season]["game_ID"].values[0] # put the game id here (game id's reflect the type of sport and the year)
game_code = "nfl" # put the game code here
auth_dir = '' # put the location where you are storing the client_id/secret
working_dir = '' # put the location where you want excel file to be saved

# Query yfpy to get data
yahoo_query = YahooFantasySportsQuery(auth_dir, league_id, game_id=game_id, game_code=game_code, offline=False)
# game = yahoo_query.get_game_key_by_season(season)
# game

p = []
x = 0
while True:
    try:
        data = yahoo_query.get_league_players(x)
        p.append(data)
        x += 25
    except:
        break

# d = yahoo_query.get_league_draft_results()
# draft = []
# for i in range(0,len(d)):
#     draft.append(d[i]['draft_result'])
# df_draft = pd.DataFrame.from_dict(draft)
# df_draft['Year'] = season
# df_draft
# df_draft.to_csv(f'data/league-draft-{season}.csv',index=False)

# t = yahoo_query.get_league_standings()
# t = json.loads(str(t))
# teams = []
# for i in range(0,len(t["teams"])):
#     teams.append(t["teams"][i]["team"])
# rows = []
# for i in range(0, len(teams)):
#     row = {}
#     try:
#         row["Manager"] = teams[i]["managers"]["manager"]["nickname"]
#     except:
#         row["Manager"] = teams[i]["managers"][0]["manager"]["nickname"]
#         row["Manager_2"] = teams[i]["managers"][1]["manager"]["nickname"]

#     try:
#         row["Points_For"] = teams[i]["team_standings"]["points_for"]
#         row["Points_Against"] = teams[i]["team_standings"]["points_against"]
#         row["Rank"] = teams[i]["team_standings"]["rank"]
#         row["Team_Key"] = teams[i]["team_key"]
#         row["Trades"] = teams[i]["number_of_trades"]
#         row["Transactions"] = teams[i]["number_of_moves"]
#         row["Team"] = teams[i]["name"]
#         row["Draft_Position"] = teams[i]["draft_position"]
#     except:
#         row["Points_For"] = ""
#         row["Points_Against"] = ""
#         row["Rank"] = ""
#         row["Team_Key"] = ""
#         row["Trades"] = ""
#         row["Transactions"] = ""
#         row["Team"] = "" 
#         row["Draft_Position"] = ""

#     try:
#         row["Draft_Grade"] = teams[i]["draft_grade"]
#     except:
#         row["Draft_Grade"] = ""

#     try:
#         row["Playoff_Seed"] = teams[i]["team_standings"]["playoff_seed"]
#     except:
#         row["Playoff_Seed"] = ""

#     rows.append(row)        
# df_teams = pd.DataFrame(rows)
# df_teams['Year'] = season
# df_teams.to_csv(f'data/league-teams-{season}.csv',index=False)

# m = []
# i = 0
# while True:
#     try:
#         data = yahoo_query.get_league_matchups_by_week(i)
#         m.append(data)
#         i += 1
#     except:
#         break
# matchups = []
# for j in range(0,len(data)):
#     for i in range(0,len(m[j])):
#         matchups.append(json.loads(str(m[j][i]['matchup'])))
# df_matchups = pd.DataFrame()
# for x in range(0,len(matchups)):
#     home_side = {}
#     away_side = {}
#     try:
#         home_side["week"] = matchups[x]['week']
#         home_side["week_start"] = matchups[x]['week_start']
#         home_side["team_1"] = matchups[x]['teams'][0]['team']['name']
#         home_side["points_1"] = matchups[x]['teams'][0]['team']['team_points']['total']
#         home_side["team_2"] = matchups[x]['teams'][1]['team']['name']
#         home_side["points_2"] = matchups[x]['teams'][1]['team']['team_points']['total']

#         away_side["week"] = matchups[x]['week']
#         away_side["week_start"] = matchups[x]['week_start']
#         away_side["team_1"] = matchups[x]['teams'][1]['team']['name']
#         away_side["points_1"] = matchups[x]['teams'][1]['team']['team_points']['total']
#         away_side["team_2"] = matchups[x]['teams'][0]['team']['name']
#         away_side["points_2"] = matchups[x]['teams'][0]['team']['team_points']['total']
#     except:
#         home_side["week"] = ""
#         home_side["week_start"] = ""
#         home_side["team_1"] = ""
#         home_side["points_1"] = ""
#         home_side["team_2"] = ""
#         home_side["points_2"] = ""

#         away_side["week"] = ""
#         away_side["week_start"] = ""
#         away_side["team_1"] = ""
#         away_side["points_1"] = ""
#         away_side["team_2"] = ""
#         away_side["points_2"] = ""

#     try:
#         home_side["team_id1"] = matchups[x]['teams'][0]['team']['managers']['manager']['manager_id']
#     except:
#         home_side["team_id1"] = matchups[x]['teams'][0]['team']['managers'][0]['manager']['manager_id']
#     try:
#         home_side["owner1"] = matchups[x]['teams'][0]['team']['managers']['manager']['nickname']
#     except:
#         home_side["owner1"] = matchups[x]['teams'][0]['team']['managers'][0]['manager']['nickname']
#     try:
#         home_side["team_id2"] = matchups[x]['teams'][1]['team']['managers']['manager']['manager_id']
#     except:
#         home_side["team_id2"] = matchups[x]['teams'][1]['team']['managers'][0]['manager']['manager_id']
#     try:
#         home_side["owner2"] = matchups[x]['teams'][1]['team']['managers']['manager']['nickname']
#     except:
#         home_side["owner2"] = matchups[x]['teams'][1]['team']['managers'][0]['manager']['nickname']

#     try:
#         away_side["team_id1"] = matchups[x]['teams'][1]['team']['managers']['manager']['manager_id']
#     except:
#         away_side["team_id1"] = matchups[x]['teams'][1]['team']['managers'][0]['manager']['manager_id']
#     try:
#         away_side["owner1"] = matchups[x]['teams'][1]['team']['managers']['manager']['nickname']
#     except:
#         away_side["owner1"] = matchups[x]['teams'][1]['team']['managers'][0]['manager']['nickname']
#     try:
#         away_side["team_id2"] = matchups[x]['teams'][0]['team']['managers']['manager']['manager_id']
#     except:
#         away_side["team_id2"] = matchups[x]['teams'][0]['team']['managers'][0]['manager']['manager_id']
#     try:
#         away_side["owner2"] = matchups[x]['teams'][0]['team']['managers']['manager']['nickname']
#     except:
#         away_side["owner2"] = matchups[x]['teams'][0]['team']['managers'][0]['manager']['nickname']

#     df = pd.DataFrame.from_dict([home_side, away_side])
#     df["Season"] = season
#     df_matchups = pd.concat([df_matchups, df])
# df_matchups.to_csv(f'data/league-matchups-{season}.csv', index=False)