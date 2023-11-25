from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests
import pandas as pd
import json
import re
import os

load_dotenv() # take environment variables from .env.

API_AUTH_URL         = os.getenv('API_AUTH_URL')     # API endpoint for authentication
API_PLAYER_STATS_URL = os.getenv('PLAYER_STATS_URL') # API endpoint for players stats
PLAYERS_URL          = os.getenv('PLAYERS_URL')      # API endpoint for players 

EMAIL    = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')


def get_auth_token(): 
    # data to be sent in JSON format
    data_to_send = { "email": EMAIL, "password": PASSWORD}
    # convert data to JSON format
    data_json = json.dumps(data_to_send)
    # set headers to indicate JSON content
    headers = {"Content-Type": "application/json"}
    # make a POST request to the authentication API
    response = requests.post(API_AUTH_URL, data=data_json, headers=headers)
    # check the response status code 
    if response.status_code == 200:         
        token = response.json()['access'] # Extract and return the access token from the response
        return token
    else:        
        raise Exception(f"Error en la solicitud POST a la API local. Código de respuesta: {response.status_code}")

  
# Define headers at the module level to avoid repetition
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

def get_config_scraping(url): 
    """
    Perform the HTTP request and return the BeautifulSoup object.
    """
    res  = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(res.content, 'html.parser')
    return soup


def get_player_stats(player, update=False):
    """
    Get player statistics and call the web_scraping function.
    """
    soup = get_config_scraping(player["url"])
    
    web_scraping_player_data(player,soup)

    if update: 
        # Get information for the current season
        year   = soup.find('select').find_all('option')[1].get('value')
        season = soup.find('select').find_all('option')[1].text
        web_scraping_stats(player, year, season)
    else:  
        # Get information for all seasons
        lists = soup.find('select').find_all('option')[1:]
        for item in lists[1:]:
            year, season = item.get('value'), item.text
            web_scraping_stats(player, year, season)
            
            
def web_scraping_player_data(player,soup): 
    URL = f'{PLAYERS_URL}{player["id"]}/'       
    
    response_get = requests.get(URL, {"Content-Type": "application/json"}).json()    
    
    # get the current age 
    age  = soup.find('li',class_='data-header__label').find('span',class_='data-header__content').text
    # remove white spaces
    age  = age.strip()
    # use a regular expression to extract the number within parentheses
    age  = re.search(r'\((\d+)\)', age).group(1)
    # get the current club 
    current_club = soup.find('span',class_='data-header__club').text
    
    # player data to be sent in JSON format
    data  = {
        "name":response_get["name"], 
        "age": age,
        'date_of_birth': response_get["date_of_birth"], 
        'nationality': response_get["nationality"], 
        'position': response_get["position"],
        "current_club" : current_club,
    } 
        
    # get the authentication token
    token = get_auth_token()
    # convert data to JSON format
    data_json = json.dumps(data)
    # set headers to indicate JSON content and include the authentication token
    headers = { "Content-Type": "application/json", "Authorization": f"Bearer {token}"}    
    # Make a POST request to add new data
    response_put = requests.put(URL, data=data_json, headers=headers)
    
    if response_put.status_code == 200: 
        print(f"Data updated successfully for player: {response_put.json()}")
    else:
        print(f"Failed to update data for player: {response_put.json()}") ,
    
            
def web_scraping_stats(player, year, season):
    """
    Perform web scraping of statistics and display general statistics.
    """
    url = f"{player['url']}?saison={year}"
    soup = get_config_scraping(url)

    dfs = []

    # Find and process each statistics box on the page
    boxes = soup.find('div',class_='large-8 columns').find_all('div',class_='box')       
    
    for box in boxes[2:]:
        competition = box.find('div',class_='table-header img-vat').find('a').text.strip() 
        # get on which team the player plays 
        team   = box.find_all('td',class_='zentriert')[3].find('a').get('title')            
        stats  = box.find_all('table')
        
        # Process each statistics table
        for stat in stats:
            table = pd.read_html(str(stat))[0]
            table['competition'] = competition
            table['team'] = team
            table['season'] = season
            table = clean_data(table)             
            dfs.append(table)
    
    if dfs: 
        # Concatenate all collected DataFrames
        df = pd.concat(dfs, ignore_index=True)      
        
        # Add additional columns
        df['victory'] = df.apply(lambda row: 1 if get_match_result(row) == 'victory' else 0, axis=1)
        df['draw'] = df.apply(lambda row: 1 if get_match_result(row) == 'draw' else 0, axis=1)
        df['defeat'] = df.apply(lambda row: 1 if get_match_result(row) == 'defeat' else 0, axis=1)
        
        # Calculate general statistics and display the result
        general_stats = df.groupby(['competition', 'team', 'season']).sum()
        games = df.groupby(['competition', 'team', 'season']).count().reset_index()['result']
        general_stats['games'] = games.tolist() 
        
        save_player_stats(player,general_stats)


def clean_data(table):    
    """
    Perform cleaning and transformations on the statistics DataFrame.
    """    
    # delete the row 'total'
    table = table[table['Date'].str.contains('Squad') == False ] 
    # Drop unnecessary columns
    columns_to_drop = ['Matchday','Date', 'For','For.1', 'Opponent','Unnamed: 11','Unnamed: 12','Unnamed: 13','Unnamed: 15','Unnamed: 16','Unnamed: 17']
    table = table.drop(columns=columns_to_drop, errors='ignore')
    # rename columns        
    table = table.rename(columns={'Venue':'venue','Opponent.1':'opponent','Result':'result','Unnamed: 9':'goals','Unnamed: 10':'assists','Unnamed: 14':'minutes played'})  
    # normalize name teams: from 'team(.1)' to 'team'
    table['opponent'] = table['opponent'].str.replace(r'\(\d+\.\)', '').str.strip()
    table['team'] = table['team'].str.replace(r'\(\d+\.\)', '').str.strip()       
    # replace NaN values with zeros
    table['goals'], table['assists'] = table['goals'].fillna(0), table['assists'].fillna(0) 
    # remove matches he has not played
    table = table.drop(table[table['goals'].astype(str).str.match('.*[a-zA-Z].*')].index)      
    table['minutes played'] = table['minutes played'].str.replace("'", '').astype(int)    
    # reformat the dates in the 'season' column
    table['season'] = table['season'].str.replace(r'(\d{2})/(\d{2})', r'20\1-20\2')
    # convert strings to integers
    table['goals']   = table['goals'].astype(int) 
    table['assists'] = table['assists'].astype(int) 
    
    return table  

def get_match_result(row):
    """
    Determine the match result (victory, draw, or defeat) for a given row.
    """
    # Function to determine victory, draw, or defeat
    venue  = row['venue']    
    result = row['result']
        
    try:               
        if 'on pens' in result or 'AET' in result: return 'draw'        
        elif venue == "H": # the team that the player belongs to played at their home  

            home_goals, away_goals = map(int, result.split(':'))  

            if home_goals > away_goals:    return 'victory'
            elif home_goals == away_goals: return 'draw'
            else: return 'defeat'

        elif venue == "A": # the player's team played as an away team. 
            
            home_goals, away_goals = map(int, result.split(':')) 
            
            if away_goals > home_goals:    return 'victory'
            elif away_goals == home_goals: return 'draw'
            else: return 'defeat' 
            
        else: return None
        
    except: None         


def save_player_stats(player, df):
    # get the authentication token
    token = get_auth_token()
    # set headers to indicate JSON content and include the authentication token
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    for record, index in zip(df.to_dict('records'), df.index.to_list()):
        # Prepare data for the POST request
        data = {
            "player": player['id'],
            "team": index[1],
            "competition": index[0],
            "season": index[2],
            "goals": record['goals'],
            "assists": record['assists'],
            "games": record['games'],
            "wins": record['victory'],
            "draws": record['draw'],
            "defeats": record['defeat'],
            "team_goals": 0,
            "minutes_played": record['minutes played'],
        }

        # Convert data to JSON format
        data_json = json.dumps(data)
        # Make a POST request to add new data
        response_post = requests.post(API_PLAYER_STATS_URL, data=data_json, headers=headers)

        # Check the response status code for the POST request
        if response_post.status_code == 201:
            print(f"Data added successfully for player {player['id']}, team {index[1]}, competition {index[0]}, season {index[2]}")        
        else:
            print(f"Failed to add data for player {player['id']}, team {index[1]}, competition {index[0]}, season {index[2]} {response_post.json()}")


PLAYER_STATS=[{"id": 1, "name":"Lionel Messi", "url":'https://www.transfermarkt.com/lionel-messi/leistungsdaten/spieler/28003/plus/'},{"id": 2, "name":"Kylian Mbappe", "url":'https://www.transfermarkt.com/kylian-mbappe/leistungsdaten/spieler/342229/plus/'},{"id": 3, "name":"Erling Haaland", "url":'https://www.transfermarkt.com/erling-haaland/leistungsdaten/spieler/418560/plus/'},{"id": 4, "name":"Vinicius Junior", "url":'https://www.transfermarkt.com/vinicius-junior/leistungsdaten/spieler/371998/plus/'},{"id": 5, "name":"Robert Lewandowski", "url":'https://www.transfermarkt.com/robert-lewandowski/leistungsdaten/spieler/38253/plus/'},{"id": 6, "name":"Kevin De Bruyne", "url":'https://www.transfermarkt.com/kevin-de-bruyne/leistungsdaten/spieler/88755/plus/'},{"id": 7, "name":"Neymar", "url":'https://www.transfermarkt.com/neymar/leistungsdaten/spieler/68290/plus/'},{"id": 8, "name":"Harry Kane", "url":'https://www.transfermarkt.com/harry-kane/leistungsdaten/spieler/132098/plus/'},{"id": 9, "name":"Victor Osimhen", "url":'https://www.transfermarkt.com/victor-osimhen/leistungsdaten/spieler/401923/plus/'},{"id": 10, "name":"Lautaro Martinez", "url":'https://www.transfermarkt.com/lautaro-martinez/leistungsdaten/spieler/406625/plus/'},{"id": 11, "name":"Antoine Griezmann", "url":'https://www.transfermarkt.com/antoine-griezmann/leistungsdaten/spieler/125781/plus/'}]


for player in PLAYER_STATS: get_player_stats(player,update=True)  




