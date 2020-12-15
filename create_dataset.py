
import glob
import pandas as pd
import os

season = "2019-20"

gw_files = glob.glob(f"C:\\Users\\sande\\Desktop\\Kool\\courses\\machine learning\\Fantasy-Premier-League\\data\\{season}\\players\\*\\gw.csv")

fixture_file = glob.glob(f"C:\\Users\\sande\\Desktop\\Kool\\courses\\machine learning\\Fantasy-Premier-League\\data\\{season}\\fixtures.csv")[0]

understat_files = glob.glob(f"C:\\Users\\sande\\Desktop\\Kool\\courses\\machine learning\\Fantasy-Premier-League\\data\\{season}\\understat\\*")

player_columns_avg_last3 = ['total_points', 'ict_index', 'threat', 'creativity', 'influence', 'minutes', 'assists', 'bonus', 'clean_sheets', 'goals_scored', 'goals_conceded', 'own_goals', 'penalties_missed', 'penalties_saved', 'saves', 'red_cards', 'yellow_cards']
# get understat files for all teams
def get_understat_dfs():
    understat_dfs = []
    for f in understat_files:
        # ignore unnecessary files
        if "understat_player.csv" in f or "understat_team.csv" in f:
            continue
        # relegated teams have files in 20-21 season folder for some reason
        if ("understat_Watford.csv" in f or "understat_Bournemouth.csv" in f) and season == '2020-21':
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

# get the round number (based on how many games has the team played before)
def get_team_round(date, team_id):
    filter1 = fixture_df["team_h"] == team_id
    filter2 = fixture_df["team_a"] == team_id
    filter3 = fixture_df["kickoff_time"] < date
    rows = fixture_df.where((filter1 | filter2) & filter3).dropna()
    return len(rows.index)

# add round numbers to player's and opponents team (used later for attaching team statistics at these rounds)
def add_team_rounds_to_df(df):
    player_team_rounds = []
    opp_team_rounds = []
    for index, row in df.iterrows():
        p_team_id = row["player_team"]
        o_team_id = row["opponent_team"]
        kickoff_time = row["kickoff_time"]
        player_team_round = get_team_round(kickoff_time, p_team_id)
        opp_team_round = get_team_round(kickoff_time, o_team_id)
        player_team_rounds.append(player_team_round)
        opp_team_rounds.append(opp_team_round)
    df[["player_team_round"]] = player_team_rounds
    df[["opponent_team_round"]] = opp_team_rounds
    return df

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
    df['last3_xpts'] = df[["xpts"]].rolling(window=3, min_periods=1).mean().shift().fillna(value=0, axis=1)
    return df

# add player points averages and form to player gameweek data
def add_averages_to_df(df):
    for col in player_columns_avg_last3:
        # average of all previous gameweeks
        df[f'avg_{col}'] = df[[col]].expanding().mean().shift().fillna(value=0, axis=1)
        # average of last 3 gameweeks
        df[f'last3_{col}'] = df[[col]].rolling(window=3, min_periods=1).mean().shift().fillna(value=0, axis=1)
        # no need to keep the original in the dataset
        df.drop(col, axis=1)
    return df

# fetch the team statistics at specific round
def get_understat_team_round_row(id_, round_):
    team_df = understat_dfs[id_ - 1]
    row = team_df.iloc[[round_]]
    return row

# add player and opponent team strength and form to player's gameweek data
def add_team_columns_to_df(df):
    team_avg_xpts = []
    team_last3_xpts = []
    opp_avg_xpts = []
    opp_last3_xpts = []
    for index, row in df.iterrows():
        team_id = row["player_team"]
        opp_id = row["opponent_team"]
        team_row = get_understat_team_round_row(team_id, row["player_team_round"])
        opp_row = get_understat_team_round_row(opp_id, row["opponent_team_round"])
        team_avg_xpts.append(team_row["avg_xpts"])
        team_last3_xpts.append(team_row["last3_xpts"])
        opp_avg_xpts.append(opp_row["avg_xpts"])
        opp_last3_xpts.append(opp_row["last3_xpts"])
    df[["team_avg_xpts"]]= team_avg_xpts
    df[["team_last3_xpts"]]= team_last3_xpts
    df[["opp_avg_xpts"]]= opp_avg_xpts
    df[["opp_last3_xpts"]]= team_avg_xpts
    return df

for df in understat_dfs:
    df = add_averages_to_understat(df)

result = []
for f in gw_files:
    df = pd.read_csv(f)
    player_name = get_player_name(f)
    df["name"] = player_name
    df["id"] = df[["element"]]
    df.drop("element", axis=1)
    df = add_player_team_to_df(df)
    df = add_averages_to_df(df)
    df = add_team_rounds_to_df(df)
    df = add_team_columns_to_df(df)
    result.append(df)

# merging all the dataframes together
df = pd.concat(result)

df.to_csv(f"data{season}.csv")
