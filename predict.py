
import os
import pickle
import pandas as pd
import datetime as dt
from requests import get
from .modules import DatasetCompilers as dc
from .modules import DailyScrape as ds
import warnings
warnings.filterwarnings("ignore")


class DailyReport(object):

    """
    This is the primary class object used to gather new data and predict game
    outcomes, intended to be run from the terminal or within a notebook.
    """

    def __init__(self):
        """
        Initilizes DailyReport class object, generating necessary datasets
        for prediction

        Args:
            self

        Returns:
            self.log: Logistic Regression predictive model
            self.mlp: Multilayer Perceptron predictive model
            self.rf: Random Forest predictive model
            self.prediction_features: Features used to predict game outcomes
            self.today: Today's date
            self.player_stats: Statistical player stats
            self.team_stats: Statistical team stats
            self.raptor: 538's up to date RAPTOR ratings for each player in today's games
            self.elo: Up to date Elo ratings for each team
            self.games: Schedule for today's games & recent performance features
            self.stats: Player statistics merged with raptor ratings for each team's roster
            self.mp: Projected minute totals for players on each team
        """

        self.pred_dir_path = str(os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop/PREDICT_NBA'))

        if not os.path.exists(self.pred_dir_path):
            os.makedirs(self.pred_dir_path)
            os.makedirs(f'{self.pred_dir_path}/MODELS')

            td_url = 'https://raw.githubusercontent.com/nathanthomasrose/nbapredictdaily/main/data/TrainingData.csv'
            log_url = 'https://raw.githubusercontent.com/nathanthomasrose/nbapredictdaily/main/data/logreg_model.sav'
            mlp_url = 'https://raw.githubusercontent.com/nathanthomasrose/nbapredictdaily/main/data/mlp_model.sav'
            rf_url = 'https://raw.githubusercontent.com/nathanthomasrose/nbapredictdaily/main/data/rf_model.sav'

            url_dict = {'TrainingData.csv': td_url, 'logreg_model.sav': log_url, 'mlp_model.sav': mlp_url, 'rf_model.sav': rf_url}

            for key in url_dict.keys():
                if key == 'TrainingData.csv':
                    FILE_TO_SAVE_AS = f'{self.pred_dir_path}/{key}'
                else:
                    FILE_TO_SAVE_AS = f'{self.pred_dir_path}/MODELS/{key}'
                resp = get(url_dict[key])
                with open(FILE_TO_SAVE_AS, "wb") as f:
                    f.write(resp.content)

        self.log = pickle.load(open(f'{self.pred_dir_path}/MODELS/logreg_model.sav', 'rb'))
        self.mlp = pickle.load(open(f'{self.pred_dir_path}/MODELS/mlp_model.sav', 'rb'))
        self.rf = pickle.load(open(f'{self.pred_dir_path}/MODELS/rf_model.sav', 'rb'))

        self.prediction_features = ['home', 'dRest', 'dAge', 'dWin%', 'dLast5', 'dLast10', 'teamElo', 'oppElo', 'dElo', 'dORtg', 'dDRtg',
                                    'dPACE', 'deFG', 'dTOV', 'dORB', 'dDRB', 'RVTo', 'RVTd', 'oRVTo', 'oRVTd']

        self.today = dt.date.today()
        print(f'\nGenerating NBA Game Schedule for {self.today.month}/{self.today.day}/{self.today.year}...')

        self.games = dc.build_game_data()
        if self.games is None:
            print('\nno NBA games today')
            return

        print("\nCompiling Statistical Data...")
        self.player_stats, self.team_stats = ds.daily_stats()
        self.raptor = ds.daily_raptor()
        self.stats = dc.build_full_stats(self.raptor, self.player_stats, daily=True)
        self.mp = ds.get_projected_minutes(names=list(self.stats.PLAYER.unique()))[['DATE', 'TEAM', 'PLAYER', 'MP', 'SEASON']]
        self.elo = ds.daily_elo()

    def get_predictions(self):
        """
        Predicts the outcomes of today's NBA games, logging their results in a full prediction dataset called Predictions.csv
        and to a more readable dataset containing only today's predictions

        Args:
            self

        Returns:
            today_pred: Reader-friendly DataFrame containing all of today's NBA game outcome predictions
        """

        if self.games is None:
            print('\nno games to predict')
            return

        print("\nAdding Player Stats to Game Data...")
        gamedata = dc.build_games_plus_roster(self.games, self.mp, self.stats)

        print("\nBuilding Game Prediction Dataset...")
        df = dc.build_prediction_data(gamedata, self.team_stats, self.elo)
        
        df.to_csv(f'{self.pred_dir_path}/NewTrainingData.csv', mode='a', header=not os.path.exists(f'{self.pred_dir_path}/NewTrainingData.csv'), index=False)
        df = pd.read_csv(f'{self.pred_dir_path}/NewTrainingData.csv')
        df.drop_duplicates(subset=['date', 'team', 'opp'], keep='last', inplace=True)
        df.to_csv(f'{self.pred_dir_path}/NewTrainingData.csv', index=False)
        
        print("\nPredicting Game Outcomes...")

        team = list(df['team'])
        opp = list(df['opp'])
        date = list(df['date'])

        df = df[self.prediction_features]

        log_prediction = self.log.predict_proba(df)
        log_prob = [round(i[1] * 100, 1) for i in log_prediction]

        mlp_prediction = self.mlp.predict_proba(df)
        mlp_prob = [round(i[1] * 100, 1) for i in mlp_prediction]

        rf_prediction = self.rf.predict_proba(df)
        rf_prob = [round(i[1] * 100, 1) for i in rf_prediction]

        binary_prediction = list(self.log.predict(df))

        preds_df = pd.DataFrame(list(zip(date, team, opp, binary_prediction, log_prob, mlp_prob, rf_prob)),
                                columns=['DATE', 'TEAM', 'OPP', 'WIN', 'LOG_PROB_%', 'MLP_PROB_%', 'RF_PROB_%'])

        print('\nPredictions Complete!')

        preds_df.to_csv(f'{self.pred_dir_path}/Predictions.csv', mode='a', header=not os.path.exists(f'{self.pred_dir_path}/Predictions.csv'), index=False)
        p = pd.read_csv(f'{self.pred_dir_path}/Predictions.csv')
        p.drop_duplicates(subset=['DATE', 'TEAM', 'OPP'], keep='last', inplace=True)
        p.to_csv(f'{self.pred_dir_path}/Predictions.csv', index=False)

        today_pred = preds_df.copy()
        today_pred = today_pred[today_pred.WIN == 1]
        cols = ['DATE', 'TEAM', 'OPP', 'LOG_PROB_%', 'MLP_PROB_%', 'RF_PROB_%']
        today_pred = today_pred[cols]
        today_pred.columns = ['Date', 'Predicted Winner', 'Predicted Loser', 'Probability (LR)', 'Probability (MLP)', 'Probability (RF)']
        today_pred['Probability (LR)'] = today_pred['Probability (LR)'].astype('str') + '%'
        today_pred['Probability (MLP)'] = today_pred['Probability (MLP)'].astype('str') + '%'
        today_pred['Probability (RF)'] = today_pred['Probability (RF)'].astype('str') + '%'
        today_pred.to_csv(f'{self.pred_dir_path}/TodayPred.csv', index=False)

        return print(today_pred)


if __name__ == "__main__":

    nba = DailyReport()
    nba.get_predictions()
