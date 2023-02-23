
import io
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup
from requests import get
from time import sleep
from tqdm import tqdm
from selenium import webdriver


def get_szn_schedule(season):

    months = ['october', 'november', 'december', 'january', 'february', 'march', 'april', 'may', 'june', 'july']

    if season == 2020:
        months[0] = 'october-2019'
        months_2020 = ['august', 'september', 'october-2020']
        months.extend(months_2020)

    df_list = []
    for month in tqdm(months):
        sleep(3)
        r = get(f'https://www.basketball-reference.com/leagues/NBA_{season}_games-{month}.html')
        if r.status_code==200:
            soup = BeautifulSoup(r.content, 'html.parser')
            table = soup.find('table', attrs={'id': 'schedule'})
            if table:
                month_df = pd.read_html(str(table))[0]
                df_list.append(month_df)

    df = pd.concat(df_list, ignore_index=True)
    df['Season'] = season
    cols = ['Season', 'Date','Visitor/Neutral','PTS','Home/Neutral','PTS.1']
    df=df[cols]
    df.columns = ['season', 'date', 'away_team', 'away_pts', 'home_team', 'home_pts']
    df = df[df.date != 'Playoffs']
    df['date'] = df['date'].apply(lambda x: pd.to_datetime(x))
    
    df.loc[df.date == str(dt.date.today()), ['away_pts', 'home_pts']] = 0
    df = df[~df.isna().any(axis=1)].reset_index(drop=True)

    return df


def get_box_scores(gamestr, team1, team2, season, date, szn_type):
    
    df_list = []
    teams = [team1,team2]
    resp = get(f'https://www.basketball-reference.com/boxscores/{gamestr}.html')
    
    if resp.status_code==200:
        soup = BeautifulSoup(resp.content, 'html.parser')
        for team in teams:
            table = soup.find('table', id=f"box-{team}-game-basic")
            df = pd.read_html(str(table))[0]
            df.columns = df.columns.droplevel()
            df['TEAM'] = team
            df['SEASON'] = season
            df['DATE'] = date
            df['SZN_TYPE'] = szn_type
            df.rename(columns = {'Starters': 'PLAYER'}, inplace=True)
            df = df[(df.PLAYER != 'Reserves') & (df.PLAYER != 'Team Totals')]
            df.replace({'Did Not Play':'0', 'Did Not Dress':'0', 'Not With Team':'0', 'Player Suspended':'0', ':':'.'},inplace=True, regex=True)
            df['MP'] = df['MP'].astype('float').astype('int')
            cols = ['SEASON', 'TEAM', 'DATE', 'PLAYER', 'SZN_TYPE', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%', 'FT', 'FTA',
                    'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', '+/-']
            df = df[cols]
            df_list.append(df)

        dfs = pd.concat(df_list, ignore_index=True)
        return dfs
    
    else:
        return 'they blocked ya, chief'


def get_roster_stats(team = 'PHO', year= 2023, stat='per_game'):    

    resp = get(f'https://www.basketball-reference.com/teams/{team}/{year}.html')
    df = None
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.content, 'html.parser')
        table = soup.find('table', {"id":stat} )
        df = pd.read_html(str(table))[0]
        df['SEASON'] = year
        df['TEAM'] = team
        df.rename(columns={'Unnamed: 1':'PLAYER', 'Age':'AGE', 'Tm':'TEAM', 'Pos':'POS'}, inplace=True)
        drop = [col for col in df.columns if 'Unnamed' in col]
        df = df.reset_index().drop(drop+['Rk', 'index'], axis=1)
        return df

def get_team_stats(team, season):
    
    try:
        driver = webdriver.Firefox()
        driver.get(f'https://www.basketball-reference.com/teams/{team}/{season}.html')
        soup = BeautifulSoup(driver.page_source, features='html.parser')
        table = soup.find('table', id='team_misc')
        df = pd.read_html(str(table))[0]
        driver.quit()
    except:
        driver.quit()
        raise ValueError("Invalid Input")

    df.columns = df.columns.droplevel()
    df.rename(columns={'Unnamed: 0_level_1': 'TEAM'}, inplace=True)
    df = df[df.TEAM != 'Lg Rank']
    df['TEAM'] = team
    df['SEASON'] = season
    df.columns = ['TEAM', 'W', 'L', 'PW', 'PL', 'MOV', 'SOS', 'SRS', 'ORtg', 'DRtg',
                    'PACE', 'FTr', '3PAr', 'eFG%', 'TOV%', 'ORB%', 'FT/FGA', 'DeFG%', 
                    'DTOV%', 'DRB%', 'DFT/FGA', 'Arena', 'Attendance', 'SEASON']

    cols = ['TEAM', 'SEASON', 'ORtg', 'DRtg', 'PACE', 'eFG%', 'TOV%', 'ORB%', 'DRB%']

    df = df[cols]

    return df


def get_raptor():

    url1 = f"https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-raptor/modern_RAPTOR_by_team.csv" 
    download1 = get(url1).content
    df1 = pd.read_csv(io.StringIO(download1.decode('utf-8')))

    url2 = f"https://projects.fivethirtyeight.com/nba-model/2023/latest_RAPTOR_by_team.csv" 
    download2 = get(url2).content
    df2 = pd.read_csv(io.StringIO(download2.decode('utf-8')))

    df = pd.concat([df1,df2], ignore_index=True)
    df = df[df.season_type == 'RS']
    df = df.rename(columns = {'season':'SEASON', 'player_name':'PLAYER', 'mp':'MP', 'team':'TEAM'})
    df = df.sort_values(by=['PLAYER']).reset_index(drop=True)
    df = df.replace({'BKN':'BRK'})
    df.SEASON = df.SEASON.astype('int')
    df.loc[(df.TEAM == 'CHA') & (df.SEASON > 2014), 'TEAM'] = 'CHO'

    df = df.sort_values(by=['SEASON','PLAYER']).reset_index(drop=True)
    df
    
    return df



def get_elo():
    
    url = "https://projects.fivethirtyeight.com/nba-model/nba_elo.csv" 
    download = get(url).content
    elo = pd.read_csv(io.StringIO(download.decode('utf-8')))
    elo=elo[['date','season','playoff','team1','team2','elo1_pre','elo2_pre']]
    elo.fillna(0, inplace=True)
    elo = elo[(elo.playoff==0) | (elo.playoff =='p')]
    elo = elo[(elo.date < str(dt.date.today())) & (elo.season > 2013)]
    elo.drop(columns='playoff', inplace=True)
    elo.replace({'BKN':'BRK'},inplace=True,regex=True)
    elo.season = elo.season.astype('int')
    elo.loc[(elo.team1 == 'CHA') & (elo.season > 2014), 'team1'] = 'CHO'
    elo.loc[(elo.team2 == 'CHA') & (elo.season > 2014), 'team2'] = 'CHO'
    elo.reset_index(drop=True, inplace=True)
    cols = ['date','team1','team2','elo1_pre','elo2_pre']
    elo=elo[cols]
    
    elodup = elo.copy()
    elodup = elodup.rename(columns={'team1':'team2','team2':'team1','elo1_pre':'elo2_pre','elo2_pre':'elo1_pre'})
    elodup=elodup[cols]
    
    full_elo = pd.concat([elo, elodup], ignore_index=True)
    full_elo = full_elo.sort_values(by='date')
    full_elo.reset_index(drop=True, inplace=True)
    full_elo = full_elo.rename(columns={'team1':'team','team2':'opp','elo1_pre':'teamElo', 'elo2_pre':'oppElo'})
    
    return full_elo

# In[ ]: