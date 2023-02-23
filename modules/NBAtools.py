
import pandas as pd
import datetime as dt
import numpy as np
from . import DailyScrape as ds
import os
import pickle
from tqdm import tqdm
from time import sleep
from sklearn.linear_model import LogisticRegression


def team_features(team, team_df):

    df = team_df.copy()
    df['team'] = team
    df['opp'] = 0
    df['team_pts'] = 0
    df['opp_pts'] = 0
    df['win'] = 0
    df['home'] = 0
    df['gp'] = 0
    g = 0

    for i in range(len(df)):

        if df.loc[i, 'away_team'] == team:
            df.loc[i, 'opp'] = df.loc[i, 'home_team']
            df.loc[i, 'team_pts'] = df.loc[i, 'away_pts']
            df.loc[i, 'opp_pts'] = df.loc[i, 'home_pts']
            df.loc[i, 'home'] = 0

        else:
            df.loc[i, 'opp'] = df.loc[i, 'away_team']
            df.loc[i, 'team_pts'] = df.loc[i, 'home_pts']
            df.loc[i, 'opp_pts'] = df.loc[i, 'away_pts']
            df.loc[i, 'home'] = 1

        if df.loc[i, 'team_pts'] > df.loc[i, 'opp_pts']:
            df.loc[i, 'win'] = 1

        df.loc[i, 'gp'] = g
        g += 1

    cols = ['season', 'date', 'team', 'team_pts', 'opp', 'opp_pts', 'win', 'home', 'gp', 'szn_type']
    df = df[cols]

    teamDayOff = [0]
    for i in range(1, len(df)):
        days_off = (df.iloc[i, 1] - df.iloc[(i - 1), 1]).days
        teamDayOff.append(days_off)

    df['teamDayOff'] = teamDayOff

    df['lastFive'] = (df['win'].rolling(min_periods=1, window=5).sum().shift().bfill()) / 5
    df['lastTen'] = (df['win'].rolling(min_periods=1, window=10).sum().shift().bfill()) / 10
    df['winp'] = df['win'].expanding(1).sum().shift()

    for i in range(1, 5):
        df.loc[i, 'lastFive'] = (df.loc[i, 'lastFive'] * 5) / i

    for i in range(1, 10):
        df.loc[i, 'lastTen'] = (df.loc[i, 'lastTen'] * 10) / i

    for i in range(len(df)):
        df.loc[i, 'winp'] = df.loc[i, 'winp'] / i

    df.iloc[0, 11:] = 0

    df = df.replace(np.nan, 0)

    return df


def top_players(team, lst, d, szn):
    df = d.copy()
    col = ['TEAM', 'PLAYER', 'MPG', 'AGE', 'raptor_total', 'raptor_offense', 'raptor_defense']
    df = df[col]
    df = df[(df['TEAM'] == team) & (df['PLAYER'].isin(lst))]
    df = df.sort_values(by=['MPG'], ascending=False).reset_index(drop=True)
    top = df[0:8]
    # top = top.sort_values(by=['raptor_total'], ascending=False).reset_index(drop=True)
    return top[['PLAYER', 'raptor_offense', 'raptor_defense', 'AGE']]


def retrain_model():
    """
    If NewTrain.csv dataset is found, this method determines the actual outcomes of yesterday's games
    and appends this dataset to the original training data, then trains a new logistic regression model
    on the updated training dataset

    Args:
        None

    Returns:
        None: if NewTrain.csv dataset not found in PREDICT_NBA directory
        retrained_log_model: logistic regression model fitted to existing data + yesterday's games
    """

    today = dt.date.today()
    pred_dir_path = str(os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop/PREDICT_NBA'))

    if not os.path.exists(pred_dir_path):
        print('You need to make predictions first')
        return None

    train = pd.read_csv(f'{pred_dir_path}/TrainingData.csv')
    NT_path = pred_dir_path + '/NewTrainingData.csv'

    if not os.path.exists(NT_path):
        print('You need to make predictions first')
        return None

    new_train = pd.read_csv(f'{pred_dir_path}/NewTrainingData.csv')
    if (new_train.date.max() == str(today)) | (train.date.max() == str(today - dt.timedelta(days=1))):
        print('Previously predicted games have yet to be played. Cannot update training data with correct outcomes.')
        return None

    print('\nUpdating Dataset With Previous Game Outcomes...')
    for i in tqdm(range(len(new_train))):
        sleep(3)
        date = new_train.loc[i, 'date']
        team = new_train.loc[i, 'team']
        season = new_train.loc[i, 'season']
        szn_type = new_train.loc[i, 'szn_type']
        new_train.loc[i, 'win'], new_train.loc[i, 'team_pts'], new_train.loc[i, 'opp_pts'] = ds.daily_training_update(date, team, season, szn_type)

    train = pd.read_csv(f'{pred_dir_path}/TrainingData.csv')
    df = pd.concat([train, new_train], ignore_index=True)
    df.to_csv(f'{pred_dir_path}/TrainingData.csv', index=False)
    os.remove(f'{pred_dir_path}/NewTrainingData.csv')

    df = df[['win', 'home', 'dRest', 'dAge', 'dWin%', 'dLast5', 'dLast10', 'teamElo', 'oppElo', 'dElo',
             'dORtg', 'dDRtg', 'dPACE', 'deFG', 'dTOV', 'dORB', 'dDRB', 'RVTo', 'RVTd', 'oRVTo', 'oRVTd']]

    targetName = "win"
    X = df.loc[:, df.columns[np.where(df.columns != targetName)]]
    y = df.loc[:, targetName]

    retrained_log_model = LogisticRegression(penalty='l2', tol=0.0001, max_iter=340, solver='lbfgs').fit(X, y)
    pickle.dump(retrained_log_model, open(f"{pred_dir_path}/MODELS/logreg_model.sav", 'wb'))

    return print('Training data has been updated and logistic regression model is now retrained and ready for use.')
