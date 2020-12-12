
import glob
import pandas as pd
import os
from itertools import groupby

season = "2019-20"

gw_files = glob.glob(f"C:\\Users\\sande\\Desktop\\Kool\\courses\\machine learning\\Fantasy-Premier-League\\data\\{season}\\players\\*\\gw.csv")

fixture_file = glob.glob(f"C:\\Users\\sande\\Desktop\\Kool\\courses\\machine learning\\Fantasy-Premier-League\\data\\{season}\\fixtures.csv")[0]

understat_files = glob.glob(f"C:\\Users\\sande\\Desktop\\Kool\\courses\\machine learning\\Fantasy-Premier-League\\data\\{season}\\understat\\*")

# get understat files for all teams
def get_understat_dfs():
    understat_dfs = []
    for f in understat_files:
        # ignore unnecessary files
        if "understat_player.csv" in f or "understat_team.csv" in f:
            continue
        understat_dfs.append(pd.read_csv(f, engine='python'))
    return understat_dfs

fixture_df = pd.read_csv(fixture_file)

understat_dfs = get_understat_dfs()

# get player name from filename
def get_player_name(path):
    namepart = path.split('\\')[-2]
    name = " ".join(namepart.split('_')[:-1])
    return name

# get player id from filename
def get_player_id(path):
    namepart = path.split('\\')[-2]
    player_id = namepart.split('_')[-1]
    return player_id

# get player team id from fixture (doesn't exist in player gameweek data)
def get_player_team_id(fixture_id, was_home):
    fixture = fixture_df.loc[fixture_df['id'] == fixture_id]
    if was_home:
        return fixture['team_h']
    else:
        return fixture['team_a']

# add player team column to player gameweek dataframe (doesn't exist in player gameweek data)
def add_player_team_to_df(df):
    player_team_row = []
    for index, row in df.iterrows():
        fixture = row["fixture"]
        was_home = row["was_home"]
        player_team_id = get_player_team_id(fixture, was_home)
        player_team_row.append(player_team_id)
    df[["player_team"]] = player_team_row
    return df

# add averages to team understat round data by using expanding and rolling dataframe functions
def add_averages_to_understat(df):
    # team strength (average points this season)
    df['avg_xpts'] = df[["xpts"]].expanding().mean().shift().fillna(value=0, axis=1)
    # team form (average points from last 5 fixtures)
    df['last5_xpts'] = df[["xpts"]].rolling(window=5, min_periods=1).mean().shift().fillna(value=0, axis=1)
    return df

for df in understat_dfs:
    df = add_averages_to_understat(df)


# add player points averages and form to player gameweek data
def add_averages_to_df(df):
    # average of all previous gameweeks
    df['avg_total_points'] = df[["total_points"]].expanding().mean().shift().fillna(value=0, axis=1)
    # average of last 5 gameweeks
    df['last5_total_points'] = df[["total_points"]].rolling(window=5, min_periods=1).mean().shift().fillna(value=0, axis=1)
    return df

# fetch the team statistics at specific round
def get_understat_team_round_row(id_, round_):
    team_df = understat_dfs[id_ - 1]
    row = team_df.iloc[[round_]]
    return row

# add player and opponent team strength and form to player's gameweek data
def add_team_columns_to_df(df):
    team_avg_xpts = []
    team_last5_xpts = []
    opp_avg_xpts = []
    opp_last5_xpts = []
    for index, row in df.iterrows():
        team_id = row["player_team"]
        opp_id = row["opponent_team"]
        team_row = get_understat_team_round_row(team_id, index)
        opp_row = get_understat_team_round_row(opp_id, index)
        team_avg_xpts.append(team_row["avg_xpts"])
        team_last5_xpts.append(team_row["last5_xpts"])
        opp_avg_xpts.append(opp_row["avg_xpts"])
        opp_last5_xpts.append(opp_row["last5_xpts"])
    df[["team_avg_xpts"]]= team_avg_xpts
    df[["team_last5_xpts"]]= team_last5_xpts
    df[["opp_avg_xpts"]]= opp_avg_xpts
    df[["opp_last5_xpts"]]= team_avg_xpts
    return df

result = []
for f in gw_files:
    # since I use indices for determining the round (round numbers are messed up in the dataset), 
    # there is no way to determine the correct rounds for someone who hasn't played the whole season 
    df = pd.read_csv(f)
    if len(df.index) != 38:
        continue
    player_name = get_player_name(f)
    df["name"] = player_name
    player_id = get_player_id(f)
    df["id"] = player_id
    df = add_player_team_to_df(df)
    df = add_averages_to_df(df)
    df = add_team_columns_to_df(df)
    result.append(df)

# merging all the dataframes together
df = pd.concat(result)

df.to_csv("data.csv")
