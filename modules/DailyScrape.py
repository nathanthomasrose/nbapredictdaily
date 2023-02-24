
import os
import io
import difflib
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup, Comment
from time import sleep
from requests import get
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By


def get_projected_minutes(names):
    """
    Purpose:
        Generates projected minutes played totals for players in today NBA game slate
    
    Args:
        names: A list of names used to update player names to match basketball-reference naming style
    Returns:
        df: DataFrame containing the relevant players projected minute totals
    """
    
    today = dt.date.today()
    if today.month > 7 & today.month <= 12:
        season = today.year + 1
    else:
        season = today.year
    
    # Use selenium webdriver to get projected minutes from Lineups.com
    driver = webdriver.Firefox()
    driver.get(f'https://www.lineups.com/nba/nba-player-minutes-per-game')
    
    # Use driver.click() function to convert the webpage table to CSV form and maximize player output
    driver.find_element(By.XPATH, '/html/body/app-root/div[3]/app-minutes-router/app-minutes/div/div/div[4]/div/div[2]/div[2]/div/div/button').click()
    driver.find_element(By.XPATH, '/html/body/app-root/div[3]/app-minutes-router/app-minutes/div/div/div[4]/div/div[2]/div[1]/div/app-dropdown/div/button/span').click()
    sleep(1)
    driver.find_element(By.XPATH, '/html/body/app-root/div[3]/app-minutes-router/app-minutes/div/div/div[4]/div/div[2]/div[1]/div/app-dropdown/div/div/div[4]').click()
    sleep(2)
    text = driver.find_element(By.XPATH, "/html/body/app-root/div[3]/app-minutes-router/app-minutes/div/div/div[5]/div/p").text
    driver.quit()
    
    # create dataframe from mp table text
    mp_df = pd.read_csv(io.StringIO(text), header=0)
    mp_df['DATE'] = str(today)
    mp_df['SEASON'] = season
    mp_df['SZN_TYPE'] = 'reg'
    mp_df = mp_df.rename(columns={'Name':'PLAYER', 'Team':'TEAM', 'Projected Minutes':'MP'})
    mp_df['MP'] = mp_df['MP'].astype('int')
    mp_df = mp_df[mp_df.MP != 0].sort_values(by=['TEAM'])
    mp_df = mp_df[['DATE', 'TEAM', 'PLAYER', 'MP', 'SEASON', 'SZN_TYPE']]
    mp_df = mp_df.replace({'BKN': 'BRK', 'CHA': 'CHO', 'NO': 'NOP', 'NY': 'NYK', 'SA': 'SAS', 'GS': 'GSW'})

    for i in range(len(mp_df)):
        name = mp_df.loc[i, 'PLAYER']
        try:
            new_name = difflib.get_close_matches(name, names, 1)[0]
        except Exception:
            continue
        mp_df.loc[i, 'PLAYER'] = new_name

    return mp_df


def daily_stats():
    """
    Purpose:
        Generates player and team stats for each team in today NBA game slate by scraping from basketball-reference.com
    
    Args:
        None
    Returns:
        ps_df: Player stats dataframe
        ts_df: Team stats dataframe
    """
    
    pred_folder = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop/PREDICT_NBA')
    sched = pd.read_csv(f'{pred_folder}/TodayGames.csv')
    teams = list(sched.team.unique())

    stats = ['per_game', 'advanced']
    today = dt.date.today()

    if today.month > 7 & today.month <= 12:
        season = today.year + 1
    else:
        season = today.year

    player_stats_dfs = []
    team_stats_dfs = [] 

    for team in tqdm(teams):
        sleep(3)
        resp = get(f'https://www.basketball-reference.com/teams/{team}/{season}.html')
        soup = BeautifulSoup(resp.content, 'html.parser')

        d_list = []
        for stat in stats:
            d = None
            table = soup.find('table', {"id": stat})
            d = pd.read_html(str(table))[0]
            d['SEASON'] = season
            d['TEAM'] = team
            d = d.rename(columns={'Player': 'PLAYER', 'Age': 'AGE'})
            if stat == 'per_game':
                d = d.rename(columns={'MP': 'MPG'})
            else:
                d = d[['Rk', 'SEASON', 'TEAM', 'PLAYER', 'MP', 'PER', 'TS%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%',
                       'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%', 'OWS', 'DWS', 'WS', 'WS/48', 'OBPM', 'DBPM', 'BPM', 'VORP']]
            drop = [col for col in d.columns if 'Unnamed' in col]
            d = d.sort_values(by='PLAYER', ascending=True).reset_index().drop(drop + ['Rk', 'index'], axis=1)
            d_list.append(d)

        player_stats = pd.merge(d_list[0], d_list[1], on=['SEASON', 'TEAM', 'PLAYER'], how='inner', validate='1:1')
        player_stats_dfs.append(player_stats)
        
        team_soup = BeautifulSoup("\n".join(soup.find_all(text=Comment)), "lxml")
        team_stats = pd.read_html(str(team_soup.select_one("table#team_misc")))[0]
        team_stats.columns = team_stats.columns.droplevel()
        team_stats.rename(columns={'Unnamed: 0_level_1': 'TEAM'}, inplace=True)
        team_stats = team_stats[team_stats.TEAM != 'Lg Rank']
        team_stats['TEAM'] = team
        team_stats['SEASON'] = season
        team_stats.columns = ['TEAM', 'W', 'L', 'PW', 'PL', 'MOV', 'SOS', 'SRS', 'ORtg', 'DRtg',
                              'PACE', 'FTr', '3PAr', 'eFG%', 'TOV%', 'ORB%', 'FT/FGA', 'DeFG%',
                              'DTOV%', 'DRB%', 'DFT/FGA', 'Arena', 'Attendance', 'SEASON']

        team_stats = team_stats[['TEAM', 'SEASON', 'ORtg', 'DRtg', 'PACE', 'eFG%', 'TOV%', 'ORB%', 'DRB%']]
        team_stats_dfs.append(team_stats)

    ps_df = pd.concat(player_stats_dfs, ignore_index=True)
    ps_df = ps_df.sort_values(by=['TEAM', 'PLAYER']).reset_index(drop=True).fillna(0)

    ps_df = ps_df[['SEASON', 'TEAM', 'PLAYER', 'AGE', 'G', 'GS', 'MPG', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%', '2P',
                   '2PA', '2P%', 'eFG%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS',
                   'PER', 'TS%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%', 'OWS', 'DWS',
                   'WS', 'WS/48', 'OBPM', 'DBPM', 'BPM', 'VORP']]

    ts_df = pd.concat(team_stats_dfs, ignore_index=True)

    return ps_df, ts_df


def daily_raptor():
    """
    Purpose:
        Generates up-to-date RAPTOR ratings from 538's data repository
    
    Args:
        None
    Returns:
        rap_df: Dataframe containing RAPTOR ratings for each player in the league
    """
    
    today = dt.date.today()

    if today.month > 7 & today.month <= 12:
        season = today.year + 1
    else:
        season = today.year

    by = ['team', 'player']
    df_list = []
    for i in by:
        url = f"https://projects.fivethirtyeight.com/nba-model/{season}/latest_RAPTOR_by_{i}.csv"
        download = get(url).content
        d = pd.read_csv(io.StringIO(download.decode('utf-8')))
        d = d.rename(columns={'season': 'SEASON', 'player_name': 'PLAYER', 'mp': 'MP', 'team': 'TEAM'})
        if i == 'team':
            d = d[['PLAYER', 'TEAM']]
        df_list.append(d)

    rap_df = pd.merge(df_list[0], df_list[1], on='PLAYER', how='left')
    rap_df = rap_df.sort_values(by=['PLAYER']).reset_index(drop=True)
    rap_df = rap_df.replace({'CHA': 'CHO'})

    return rap_df


def daily_elo():
    """
    Purpose:
        Generates up-to-date ELO ratings from 538's data repository
    
    Args:
        None
    Returns:
        elo_df: Dataframe containing ELO ratings for each team in the league
    """

    url = "https://projects.fivethirtyeight.com/nba-model/nba_elo_latest.csv"
    download = get(url).content
    df = pd.read_csv(io.StringIO(download.decode('utf-8')))
    df = df[df.date == str(dt.date.today())]
    cols = ['date', 'team1', 'team2', 'elo1_pre', 'elo2_pre']
    df = df[cols]
    df.replace('CHA', 'CHO', inplace=True)
    df.reset_index(drop=True, inplace=True)
    elodup = df.copy()
    elodup = elodup.rename(columns={'team1': 'team2', 'team2': 'team1', 'elo1_pre': 'elo2_pre', 'elo2_pre': 'elo1_pre'})
    elo_df = pd.concat([df, elodup], ignore_index=True)
    elo_df = elo_df.sort_values(by='date')
    elo_df.reset_index(drop=True, inplace=True)
    elo_df = elo_df.rename(columns={'team1': 'team', 'team2': 'opp', 'elo1_pre': 'teamElo', 'elo2_pre': 'oppElo'})

    return elo_df


def daily_training_update(date, team, season, szn_type):
    """
    Purpose:
        Generates game outcome and score from basketball-reference.com

        Used to update recently added training data from previous prediction runs
        with the actual game outcome in order to provide an accurate training dataset
        for retraining the logistic regression model
    
    Args:
        date: Date of the game
        team: primary team listed in training dataset
        season: NBA season
        szn_type: Regular Season or Playoffs
    Returns:
        win: Binary game outcome indicator (1,0)
        Tm: Primary team points
        Opp: Opposing team points
    """

    r = get(f'https://www.basketball-reference.com/teams/{team}/{season}_games.html')

    if szn_type == 0:
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, 'html.parser')
            table = soup.find('table', attrs={'id': 'games'})
            if table:
                df = pd.read_html(str(table))[0]

        else:
            print(r.status_code)

    elif szn_type == 1:
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, 'html.parser')
            table = soup.find('table', attrs={'id': 'games_playoffs'})
            if table:
                df = pd.read_html(str(table))[0]

    df = df.rename(columns={'Unnamed: 7': 'win'})
    df = df[df.G != 'G']
    df['Date'] = df['Date'].apply(lambda x: pd.to_datetime(x))
    df['Team'] = team
    df = df[['Date', 'Team', 'win', 'Tm', 'Opp']]
    df.loc[df.win == 'W', 'win'] = 1
    df.loc[df.win == 'L', 'win'] = 0
    df = df[df.Date == date]
    df = df.reset_index(drop=True)
    win = df.iloc[0, 2]
    tm = df.iloc[0, 3]
    opp = df.iloc[0, 4]
    return win, tm, opp
