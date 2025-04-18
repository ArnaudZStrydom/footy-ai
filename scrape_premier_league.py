import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import csv
from collections import OrderedDict
import time

def get_team_players(url):
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    table = soup.find('table')

    links = []
    for tr in table.findAll("tr"):
        for each in tr.findAll("td"):
            if not each.text.startswith('Matches'): continue
            try:
                link = each.find('a')['href']
                links.append(link)
            except:
                pass 

    df = pd.read_html(url, header=1)[0]
    df = df[(df.Player != 'Opponent Total') & (df.Player != 'Squad Total')]
    assert len(links) == len(df)
    df['Matches'] = links
    return df

def get_league_tables(url):
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    table = soup.find('table')

    # Check if table is found
    if table is None:
        print(f"Error: No table found on the page: {url}")
        return None  # or raise an exception if you want to stop the script

    links = []
    for tr in table.findAll("tr"):
        for each in tr.findAll("th", {'data-stat':'squad'}):
            try:
                link = each.find('a')['href']
                links.append(link)
            except:
                pass 

    squad = pd.read_html(url, header=1)[0]
    opponent = pd.read_html(url, header=1)[1]

    oppNames = OrderedDict()
    for x in opponent.columns.to_list():
        oppNames[x] = 'Opp ' + x
    opponent.rename(columns=oppNames, inplace=True)

    squad['page'] = links
    df = pd.concat([squad, opponent], axis=1)
    return df


def get_teamInfoPerLeague(url):
    df = pd.read_html(url)[1]
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    table = soup.findAll('table')[1]

    links = []
    for tr in table.findAll("tr"):
        for each in tr.findAll("td", {'data-stat':'match_report'}):
            try:
                link = each.find('a')['href']
                links.append(link)
            except:
                links.append('')

    df['Team'] = url.split('/')[-1].split('-')[0]
    df['Match Report'] = links
    return df

def access_perMatchInfo_tables(url, ind):
    df = pd.read_html(url)[ind]
    dfColumns = df.columns.map('{0[0]}|{0[1]}'.format)
    dfColumns = [x.split('|')[1] if x.startswith('Unnamed') else x for x in dfColumns]
    df = df.droplevel(level=0, axis=1)
    df.columns = dfColumns
    return df

def access_perMatchInfo_summary(url, ind):
    simpleTable = pd.read_html(url)[ind]
    tmpTeamName = simpleTable.columns.to_list()[0]
    simpleTable['Team'] = np.array([tmpTeamName.split(' ')[0]] * len(simpleTable))
    simpleTable['Formation'] = np.array([tmpTeamName.split(' ')[1].replace('(', '').replace(')', '')] * len(simpleTable))
    simpleTable.rename(columns={simpleTable.columns.to_list()[0]: 'Shirt', simpleTable.columns.to_list()[1]: 'Player'}, inplace=True)
    return simpleTable

def get_perMatchInfo_players(url, players='all'):
    tmpColumns = ['Player', '#', 'Nation', 'Pos', 'Age', 'Min']
    simpleTableHome = access_perMatchInfo_summary(url, 0)
    simpleTableAway = access_perMatchInfo_summary(url, 1)

    if players.startswith('goalkeepers'):
        gkHome = pd.merge(simpleTableHome, access_perMatchInfo_tables(url, 9), how='inner', on='Player')
        gkAway = pd.merge(simpleTableAway, access_perMatchInfo_tables(url, 16), how='inner', on='Player')
        finalTable = pd.concat([gkHome, gkAway], ignore_index=True)

    else:
        finalTableHome = pd.merge(simpleTableHome, access_perMatchInfo_tables(url, 3), on='Player')
        for itab in range(4, 9):
            finalTableHome = pd.merge(finalTableHome, access_perMatchInfo_tables(url, itab), on=tmpColumns)

        finalTableAway = pd.merge(simpleTableAway, access_perMatchInfo_tables(url, 10), on='Player')
        for itab in range(11, 16):
            finalTableAway = pd.merge(finalTableAway, access_perMatchInfo_tables(url, itab), on=tmpColumns)

        finalTable = pd.concat([finalTableHome, finalTableAway], ignore_index=True)

    return finalTable

def get_perMatchInfo_shots(url):
    df = access_perMatchInfo_tables(url, 17)
    df.dropna(how='all', inplace=True)
    return df

def createDataframes(url, label):
    season = get_league_tables(url)
    season.to_csv(f'datasetTable{label}.csv', index=False)

    teamInfoSeason = []
    for teamPage in season['page'].to_list():
        teamInfoSeason.append(get_teamInfoPerLeague('https://fbref.com/' + teamPage))

    dfTeamInfoSeason = pd.concat(teamInfoSeason, ignore_index=True)
    dfTeamInfoSeason.to_csv(f'datasetTeamInfo{label}.csv', index=False)

    matchInfoSeason = []
    matchGkInfoSeason = []
    matchShotsInfoSeason = []
    for matchPage in dfTeamInfoSeason['Match Report'].to_list():
        if label in matchPage:
            matchInfoSeason.append(get_perMatchInfo_players('https://fbref.com/' + matchPage))
            matchGkInfoSeason.append(get_perMatchInfo_players('https://fbref.com/' + matchPage, players='goalkeepers'))
            matchShotsInfoSeason.append(get_perMatchInfo_shots('https://fbref.com/' + matchPage))

    dfMatchInfoSeason = pd.concat(matchInfoSeason, ignore_index=True)
    dfMatchInfoSeason.to_csv(f'datasetMatchInfo{label}.csv', index=False)
    dfMatchGkInfoSeason = pd.concat(matchGkInfoSeason, ignore_index=True)
    dfMatchGkInfoSeason.to_csv(f'datasetMatchGkInfo{label}.csv', index=False)
    dfMatchShotsInfoSeason = pd.concat(matchShotsInfoSeason, ignore_index=True)
    dfMatchShotsInfoSeason.to_csv(f'datasetMatchShotsInfo{label}.csv', index=False)

# Loop through seasons from 2004 to 2025
for year in range(2004, 2026):
    url = f'https://fbref.com/en/comps/9/English-Premier-League-Stats/{year}-{year+1}'
    createDataframes(url, f'{year}-PL')
    time.sleep(5)  # Wait for 5 seconds between requests
