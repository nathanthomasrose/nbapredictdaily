# nbapredictdaily
### Predict NBA games using nbapredictdaily
This package is an extension of my master's dissertation project that has been specifically tailored for generating daily reports of predicted NBA game outcomes using ML classification models. The idea is to utilize the trained predictive models developed in this project to allow for users to quickly and easily generate highly accurate predictions on NBA games on a daily basis. This is accomplished by redesigning the webscrapers used in the project to gather, clean, and compile a prediction dataset exclusively representing games taking place on the day that the program is run. Once predictions are made, the results are stored in csv files in a directory created on the user's Desktop called "PREDICT_NBA". 

At the moment, this package is only availabe on MacOS.

### Background
The main project involved using a dataset with >20,000 rows representing each NBA game played from the 2013-2014 NBA season to the beginning of the 2022-2023 NBA season, containing information regarding recent team performance, player skill levels, and, crucially, minutes played by players on both teams in a given matchup. This dataset was used to train a logistic regression model, an MLP classifier, and a randon forest classifier. The best performing model, the logistic regression model, a mean testing accuracy of 69.7% across 20 different train-test-split random states. This model outperforms most game prediction models created by students and hobbyists that can be found online and rivals the predictive power of some professional sports betting sites' models. However, to be completely clear, this is not a tool intended to inform betting decisions.

### Installation
To install, use the following terminal command:

```sh
pip install nbapredictdaily
```

### Generate Predictions
To use the primary prediction functionality, use the following lines of Python code in a notebook or python file:

```sh
from nbapredict_daily.predict import DailyReport
daily_report = DailyReport()
daily_report.get_predictions()
```

### Output
When run for the the first time, the program will create the "NBA_PREDICT" folder on the user's desktop, into which it will log all results and store the necessary documents. It will also display a Pandas DataFrame in the console. Therefore, the output includes:
- A Pandas DataFrame containing today's prediction results with the following columns
    - `['Date', 'Predicted Winner', 'Predicted Loser', 'Probability (LR)', 'Probability (MLP)', 'Probability (RF)']`
- `TodayGames.csv` - Today's slate of NBA games
- `TodayPred.csv` - Predictions for today's games
- `Predictions.csv` - All predictions made by the user, appended with each run
- `TrainingData.csv` - The data used to train the models. This will be appended with the new training data upon use of the `retrain_model` function
- `NewTrainingData.csv` - Data for today's games that can be appended to the model's training data once the game outcomes are known
- `MODELS` - A subfolder containing the trained models' .sav files for easy access on future runs

### Retrain the Logistic Regression Model
After some predictions have been made and the true outcomes can be known (generally the following day), you can use the `retrain_model` function. This function will update the `NewTrainingData.csv` file with the correct game outcomes, append it to the main `TrainingData.csv` dataset, and retrain the logistic regression model, keeping it up to date and, ideally, enhancing its predictive capability.

To do this, run the following lines of code in a notebook or python file:

```sh
from nbapredict_daily.modules import NBAtools
NBAtools.retrain_model()
```

### Documentation
Check out the source code [here](https://github.com/nathanthomasrose/nbapredictdaily)