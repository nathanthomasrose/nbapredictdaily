
import os
import difflib
import pandas as pd
import numpy as np
import datetime as dt
from . import Scrapers as scrape
from . import NBAtools as tl
from tqdm import tqdm
from time import sleep


def build_game_data(season=None):
    
    pred_folder = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop/PREDICT_NBA')
    today = dt.date.today()
    
    if season is None:
        if today.month > 7 & today.month <= 12:
            season = today.year + 1
        else:
            season = today.year
        time_span = 'Today'
    
    else:
        time_span = str(season)

    sched = scrape.get_szn_schedule(season)
    
    sched['szn_type'] = 'reg'

    abv_dict = {'Atlanta Hawks' : 'ATL',
                'Boston Celtics' : 'BOS',
                'Brooklyn Nets' : 'BRK',
                'Chicago Bulls' : 'CHI',
                'Charlotte Hornets' : 'CHO',
                'Charlotte Bobcats' : 'CHA',
                'Cleveland Cavaliers' : 'CLE',
                'Dallas Mavericks' : 'DAL',
                'Denver Nuggets' : 'DEN',
                'Detroit Pistons' : 'DET',
                'Golden State Warriors' : 'GSW',
                'Houston Rockets' : 'HOU',
                'Indiana Pacers' : 'IND',
                'Los Angeles Clippers' : 'LAC',
                'Los Angeles Lakers' : 'LAL',
                'Memphis Grizzlies' : 'MEM',
                'Miami Heat' : 'MIA',
                'Milwaukee Bucks' : 'MIL',
                'Minnesota Timberwolves' : 'MIN',
                'New Orleans Pelicans' : 'NOP',
                'New Orleans Hornets' : 'NOH',
                'New York Knicks' : 'NYK',
                'Oklahoma City Thunder' : 'OKC',
                'Orlando Magic' : 'ORL',
                'Philadelphia 76ers' : 'PHI',
                'Phoenix Suns' : 'PHO',
                "Portland Trail Blazers" : "POR",
                "Sacramento Kings" : "SAC",
                "San Antonio Spurs" : "SAS",
                "Toronto Raptors" : "TOR",
                "Utah Jazz" : "UTA",
                "Washington Wizards" : 'WAS'}

    for i in range(len(sched)):
        v = sched.iloc[i,2]
        h = sched.iloc[i,4]
        sched.iloc[i,2] = abv_dict[v]
        sched.iloc[i,4] = abv_dict[h]

    sched = sched.astype({"away_pts":"int","home_pts":"int"})

    df_list = []
    teams = sched['away_team'].unique()

    for team in teams:
        teamdf = sched[(sched.home_team == team) | (sched.away_team == team)]
        teamdf.reset_index(drop=True, inplace=True)
        d = tl.team_features(team,teamdf)
        df_list.append(d)

    df = pd.concat(df_list, ignore_index=True)
    
    maindf = pd.merge(df, df, left_on=['date', 'opp'], right_on=['date', 'team'], how='outer', suffixes=["","_opp"])
    maindf = maindf.drop(columns=['season_opp', 'team_opp', 'team_pts_opp', 'opp_opp', 
                                  'opp_pts_opp', 'win_opp', 'home_opp', 'gp_opp', 'szn_type_opp']).sort_values(by=['date']).reset_index(drop=True)
    
    maindf.rename(columns={'teamDayOff_opp':'oppDayOff', 'lastFive_opp':'oppLastFive', 'lastTen_opp':'oppLastTen', 'winp_opp':'oppWinp'}, inplace=True)
    
    if season == 2022:
        maindf.loc[maindf['date'] > '2022-04-15', 'szn_type'] = 'post'
    elif season == 2021:
        maindf.loc[maindf['date'] > '2021-05-21', 'szn_type'] = 'post'
    elif season == 2020:
        maindf.loc[maindf['date'] > '2020-08-15', 'szn_type'] = 'post'
    else:
        maindf.loc[maindf.gp > 81, 'szn_type'] = 'post'

    cols = ['season', 'date', 'team', 'team_pts', 'opp', 'opp_pts', 'home', 'szn_type', 'gp', 'lastFive', 'lastTen', 'winp',
            'teamDayOff', 'oppLastFive', 'oppLastTen', 'oppWinp', 'oppDayOff', 'win']
    
    maindf = maindf[cols]
    
    if time_span == 'Today':
        maindf = maindf[maindf.date == str(today)]
        if len(maindf) == 0:
            return None
    else:
        maindf = maindf[maindf.date != str(today)]
        
    maindf.reset_index(drop=True, inplace=True)
    
    maindf.to_csv(f'{pred_folder}/{time_span}Games.csv', index=False)
    
    return maindf


def build_player_minutes(games):

    games = games[~games[['team', 'opp', 'date']].apply(frozenset, axis=1).duplicated()]

    dflist = []
    for i in tqdm(range(len(games))):
        sleep(2.8)
        date = str(games.loc[i,'date'])
        team1 = games.loc[i,'team']
        team2 = games.loc[i,'opp']
        home = games.loc[i,'home']
        season = games.loc[i,'season']
        szn_type = games.loc[i,'szn_type']

        if home == 1:
            h = team1
        else:
            h = team2

        d = date.replace('-','')

        gamestr = d + '0' + h

        try:
            d = scrape.get_box_scores(gamestr, team1, team2, season, date, szn_type)
            dflist.append(d)
        except Exception:
            raise ValueError('no good fam')

    main_df = pd.concat(dflist, ignore_index=True)
    main_df.sort_values(by=['DATE', 'TEAM'], ascending=True, inplace=True)
    main_df.reset_index(drop=True, inplace=True)

    return main_df


def build_player_stats(teams, years):

    df_list = []
    for team in tqdm(teams):
        for year in years:
            sleep(3)
            try:
                pg = scrape.get_roster_stats(team, year, 'per_game').sort_values(by='PLAYER', ascending=True).rename(columns={"MP": "MPG"})
                sleep(3)
                adv = scrape.get_roster_stats(team, year, stat='advanced').sort_values(by='PLAYER', ascending=True)
                adv = adv[['SEASON', 'TEAM', 'PLAYER', 'MP', 'PER', 'TS%', '3PAr', 'FTr', 'ORB%', 'DRB%',
                           'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%', 'OWS', 'DWS',
                            'WS', 'WS/48', 'OBPM', 'DBPM', 'BPM', 'VORP']]

                df = pg.merge(adv, on = ['SEASON','TEAM','PLAYER'], how = 'inner', validate='1:1')
                df_list.append(df)

            except Exception:
                continue

    maindf = pd.concat(df_list, ignore_index=True)
    maindf = maindf.drop_duplicates().sort_values(by=['SEASON', 'TEAM'], ascending=True).reset_index(drop=True).fillna(0)

    cols = ['SEASON', 'TEAM', 'PLAYER', 'AGE', 'G', 'GS', 'MPG', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%', '2P',
            '2PA', '2P%', 'eFG%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS/G',
            'PER', 'TS%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%', 'OWS', 'DWS',
            'WS', 'WS/48', 'OBPM', 'DBPM', 'BPM', 'VORP']

    maindf = maindf[cols]

    return maindf


def build_full_stats(rap = None, stat = None, daily = False):

    stat.SEASON = stat.SEASON.astype('int')
    teams = list(stat.TEAM.unique())
    rap = rap[rap.TEAM.isin(teams)].reset_index(drop=True)
    correct_names = list(stat.PLAYER.unique())

    for i in range(len(rap)):
        name = rap.loc[i,'PLAYER']
        try:
            new_name = difflib.get_close_matches(name, correct_names, 1)[0]
            rap.loc[i,'PLAYER'] = new_name
        except:
            continue

    if daily is False:
        df = pd.merge(stat, rap, how='left', on=['PLAYER', 'SEASON', 'TEAM'], suffixes=['','no']).reset_index(drop=True)
    else:
        df = pd.merge(stat, rap, how='left', on=['PLAYER'], suffixes=['','no']).reset_index(drop=True)
        
    cols = ['SEASON', 'TEAM', 'PLAYER', 'AGE', 'G', 'GS', 'MPG', 'MP', 'raptor_offense', 'raptor_defense', 'raptor_total']

    df=df[cols]
    
    return df


def build_games_plus_roster(games, mp, stats):
    
    for i in range(1,9):
        games[f'P{i}Or'] = 0
        games[f'P{i}Dr'] = 0
        games[f'P{i}name'] = 0
        games[f'P{i}MP'] = 0
        games['age'] = 0

    for i in range(1,9):
        games[f'oppP{i}Or'] = 0
        games[f'oppP{i}Dr'] = 0
        games[f'oppP{i}name'] = 0
        games[f'oppP{i}MP'] = 0
        games['oppage'] = 0


    for i,r in tqdm(games.iterrows()):
        date = r['date']
        team = r['team']
        opp = r['opp']
        szn = int(r['season'])
        # po = r['szn_type']

        teammp = mp[mp.TEAM == team]
        oppmp = mp[mp.TEAM == opp]

        # Use Get Top Players Function To Return Roster and Player Info
        top = tl.top_players(team, teammp.PLAYER.unique(), stats, szn)
        otop = tl.top_players(opp, oppmp.PLAYER.unique(), stats, szn)

        roster = top.merge(teammp, on='PLAYER', how='left')
        oroster = otop.merge(oppmp, on='PLAYER', how='left')

        roster.drop(columns=['DATE','TEAM','SEASON'], inplace=True)
        oroster.drop(columns=['DATE','TEAM','SEASON'], inplace=True)

        names = list(roster.iloc[0:7,0].values)
        Or = list(roster.iloc[0:7,1].values)
        Dr = list(roster.iloc[0:7,2].values)
        minutes = list(roster.iloc[0:7,4].values)
        age = round(np.sum(roster.iloc[0:7,3].values)/8,1)
        games.loc[i,'age'] = age


        onames = list(oroster.iloc[0:7,0].values)
        oOr = list(oroster.iloc[0:7,1].values)
        oDr = list(oroster.iloc[0:7,2].values)
        ominutes = list(oroster.iloc[0:7,4].values)
        oage = round(np.sum(oroster.iloc[0:7,3].values)/8,1)
        games.loc[i,'oppage'] = oage

        for j,name in enumerate(names):
            O = Or[j]
            D = Dr[j]
            minu = minutes[j]
            games.loc[i,f'P{j+1}Or'] = O
            games.loc[i,f'P{j+1}Dr'] = D
            games.loc[i,f'P{j+1}MP'] = minu
            games.loc[i,f'P{j+1}name'] = name
        for x,oname in enumerate(onames):
            oO = oOr[x]
            oD = oDr[x]
            ominu = ominutes[x]
            games.loc[i,f'oppP{x+1}Or'] = oO
            games.loc[i,f'oppP{x+1}Dr'] = oD
            games.loc[i,f'oppP{x+1}MP'] = ominu
            games.loc[i,f'oppP{x+1}name'] = oname

    return games


def build_prediction_data(games_base, team_stats, elo):

    opp_ts = team_stats.copy()
    team_stats.columns = ['team', 'season', 'ORtg', 'DRtg', 'PACE', 'eFG%', 'TOV%', 'ORB%', 'DRB%']
    opp_ts.columns = ['opp', 'season', 'oppORtg', 'oppDRtg', 'oppPACE', 'oppeFG%', 'oppTOV%', 'oppORB%', 'oppDRB%']
    
    g_ts = pd.merge(games_base, team_stats, on=['team','season'], how='left')
    games = pd.merge(g_ts, opp_ts, on=['opp','season'], how='left')

    elo['date'] = elo['date'].astype('str')
    games['date'] = games['date'].astype('str')

    df = games.merge(elo, on=['date','team','opp'], how='outer').reset_index(drop=True)

    featlist = []
    ofeatlist = []

    for i in range(1,9):
        df[f'P{i}RVo'] = round(df[f'P{i}Or'] * df[f'P{i}MP'],2)
        df[f'P{i}RVd'] = round(df[f'P{i}Dr'] * df[f'P{i}MP'],2)
        df[f'oP{i}RVo'] = round(df[f'oppP{i}Or'] * df[f'oppP{i}MP'],2)
        df[f'oP{i}RVd'] = round(df[f'oppP{i}Dr'] * df[f'oppP{i}MP'],2)
        l = [f'P{i}RVo', f'P{i}RVd']
        ol = [f'oP{i}RVo', f'oP{i}RVd']
        featlist.extend(l)
        ofeatlist.extend(ol)

    featlist_off = [i for i in featlist if 'RVo' in i]
    featlist_def = [i for i in featlist if 'RVd' in i]
    ofeatlist_off = [i for i in ofeatlist if 'RVo' in i]
    ofeatlist_def = [i for i in ofeatlist if 'RVd' in i]

    df['RVTo'] = round(df[featlist_off].sum(axis=1),2)
    df['RVTd'] = round(df[featlist_def].sum(axis=1),2)
    df['oRVTo'] = round(df[ofeatlist_off].sum(axis=1),2)
    df['oRVTd'] = round(df[ofeatlist_def].sum(axis=1),2)

    df['dWin%'] = round((df['winp'] - df['oppWinp'])*100,2)
    df['dLast5'] = df['lastFive'] - df['oppLastFive']
    df['dLast10'] = df['lastTen'] - df['oppLastTen']

    df['tmElo'] = round(df['teamElo'], 1)
    df['oppElo'] = round(df['oppElo'], 1)
    df['dORtg'] = round(df['ORtg'] - df['oppORtg'],4)
    df['dDRtg'] = round(df['DRtg'] - df['oppDRtg'],4)
    df['dPACE'] = round(df['PACE'] - df['oppPACE'],4)
    df['deFG'] = round(df['eFG%'] - df['oppeFG%'],4)
    df['dTOV'] = round(df['TOV%'] - df['oppTOV%'],4)
    df['dORB'] = round(df['ORB%'] - df['oppORB%'],4)
    df['dDRB'] = round(df['DRB%'] - df['oppDRB%'],4)
    df['dElo'] = round(df['teamElo'] - df['oppElo'], 1)
    df['dAge'] = round(df['age'] - df['oppage'], 1)
    df['dRest'] = round(df['oppDayOff'] - df['teamDayOff'], 1)

    df.loc[df.home == 0, 'home'] = -1
    df.loc[df.szn_type == 'reg', 'szn_type'] = 0
    df.loc[df.szn_type == 'post', 'szn_type'] = 1
    df['dAge'] = round((df['dAge']),2)
    df['dLast5'] = round((df['dLast5'])*100,2)
    df['dLast10'] = round((df['dLast10'])*100,2)

    df.reset_index(drop=True, inplace=True)
    
    return df
