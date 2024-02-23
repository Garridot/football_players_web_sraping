from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests
import pandas as pd
import json
import re
import os

from datetime import datetime

load_dotenv() # take environment variables from .env.

API_AUTH_URL         = os.getenv('API_AUTH_URL')     # API endpoint for authentication
API_PLAYER_STATS_URL = os.getenv('API_PLAYER_STATS_URL') # API endpoint for players stats
API_PLAYERS_URL      = os.getenv('API_PLAYERS_URL')      # API endpoint for players 
API_STATS_BY_POSITION_URL = os.getenv('API_STATS_BY_POSITION_URL' ) # API endpoint for players stats by position
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

def config_scraper(url): 
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
    soup = config_scraper(player["url"])
    
    scraper_player_data(player,soup)

    if update: 
        # Get information for the current season
        year   = soup.find('select').find_all('option')[1].get('value')
        season = soup.find('select').find_all('option')[1].text        
        scraper_general_stats(player, year, season)
        scraper_stats_by_position(soup, player, year, season)
    else:  
        # Get information for all seasonss
        lists = soup.find('select').find_all('option')[1:]           
        for item in lists:            
            year, season = item.get('value'), item.text 
            scraper_general_stats(player, year, season)
            scraper_stats_by_position(soup, player, year, season)
            
            
def scraper_player_data(player,soup):

    # Get Player Details
    player_details = soup.find('div',class_='data-header__details').find_all('ul',class_='data-header__items')
    # Get the date_of_birth and current age 
    date_of_birth  = player_details[0].find_all('li',class_='data-header__label')[0].find('span',class_='data-header__content').text
    # Get Player Position
    position = player_details[1].find_all('li',class_='data-header__label')[1].find('span',class_='data-header__content').text    
    # Get Player nationality
    nationality = player_details[2].find_all('li',class_='data-header__label')[0].find('span',class_='data-header__content').text
    # Get the current club 
    current_club = soup.find('span',class_='data-header__club').text   

    ### CLEAN DATA  
       
    # Remove white spacess
    date_of_birth = date_of_birth.strip()   
    position      = position.strip()
    nationality   = nationality.strip()  
    current_club  = current_club.strip() 
    # Get the current age; Use a regular expression to extract the number within parentheses   
    age  = re.search(r'\((\d+)\)', date_of_birth).group(1) 
    # Get birth; Remove unnecessary string content to convert to a datetime object
    birth = date_of_birth[0:-5] 
    # Convert the string to a datetime object
    date_object = datetime.strptime(birth, "%b %d, %Y")
    # Format the datetime object as a string in "YYYY-MM-DD" format
    formatted_birth = date_object.strftime("%Y-%m-%d")        
    
    # Player data to be sent in JSON format
    data  = {
        "name"          :player["name"], 
        "age"           : age,      
        "current_club"  : current_club,     
        'date_of_birth' : formatted_birth, 
        'nationality'   : nationality,
        'position'      : position         
    } 
        
    # Get the authentication token
    token = get_auth_token()
    # Convert data to JSON format
    data_json = json.dumps(data)
    # Set headers to indicate JSON content and include the authentication token
    headers = { "Content-Type": "application/json", "Authorization": f"Bearer {token}"} 

    URL = f'{API_PLAYERS_URL}{player["id"]}/'           
    response_get = requests.get(URL, {"Content-Type": "application/json"})     

    if response_get.status_code == 200:
        # Make a PUT request to add new data
        response_put = requests.put(URL, data=data_json, headers=headers)        
        if response_put.status_code == 200: print(f"Data updated successfully for player: {response_put.json()}")
        else: print(f"Failed to update data for player: {response_put.json()}")
    else:     
        # Make a POST request to add new data
        response_put = requests.post(API_PLAYERS_URL, data=data_json, headers=headers)        
        if response_put.status_code == 201: print(f"Data added successfully for player: {response_put.json()}")
        else: print(f"Failed to add data for player: {response_put.json()}")

                
def scraper_general_stats(player, year, season):
    """
    Perform web scraping of statistics and display general statistics.
    """
    url = f"{player['url']}?saison={year}"
    soup = config_scraper(url)

    dfs = []

    # Find and process each statistics box on the page
    table_stats = soup.find('div',class_='large-8 columns').find_all('div',class_='box')       
    
    for box in table_stats[2:]:
        competition = box.find('div').find('a').text.strip() 
        # get on which team the player plays 
        team   = box.find_all('td',class_='zentriert')[3].find('a').get('title')            
        stats  = box.find_all('table')
        
        # Process each statistics table
        for stat in stats:
            table = pd.read_html(str(stat))[0]
            table['competition'] = competition            
            table['team'] = team
            table['season'] = season            
            table = clean_general_stats(table)             
            dfs.append(table)
    
    if dfs: 
        # Concatenate all collected DataFrames
        df = pd.concat(dfs, ignore_index=True)                     
        
        # Add additional columns
        df['victory'] = df.apply(lambda row: 1 if get_match_result(row) == 'victory' else 0, axis=1)
        df['draw'] = df.apply(lambda row: 1 if get_match_result(row) == 'draw' else 0, axis=1)
        df['defeat'] = df.apply(lambda row: 1 if get_match_result(row) == 'defeat' else 0, axis=1)
        df['team goals'] = df.apply(lambda row: get_team_goals(row), axis=1)
                
        # Calculate general statistics and display the result
        general_stats = df.groupby(['competition', 'team', 'season']).sum()
        games = df.groupby(['competition', 'team', 'season']).count().reset_index()['result']
        general_stats['games'] = games.tolist()  

        save_general_stats(player,general_stats)

def clean_general_stats(table):    
    """
    Perform cleaning and transformations on the statistics DataFrame.
    """    
    # delete the row 'total'
    table = table[table['Date'].str.contains('Squad') == False ] 
    # Drop unnecessary columns
    columns_to_drop = ['Matchday','For','For.1', 'Opponent','Unnamed: 11','Unnamed: 12','Unnamed: 13','Unnamed: 15','Unnamed: 16','Unnamed: 17']
    table = table.drop(columns=columns_to_drop, errors='ignore')
    # rename columns        
    table = table.rename(columns={'Date':'date','Venue':'venue','Opponent.1':'opponent','Result':'result','Unnamed: 9':'goals','Unnamed: 10':'assists','Unnamed: 14':'minutes played'})  
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

    # convert the "date" column as a datetime type for proper sorting
    table['date'] = pd.to_datetime(table['date'])

    # sorting by the "date" column
    table = table.sort_values(by='date')       
    
    return table  

def get_match_result(row):
    """
    Determine the match result (victory, draw, or defeat) for a given row.
    """
    # Function to determine victory, draw, or defeat
    venue = row.get('venue', '')  # Use get to handle the case where 'venue' is not present
    result = row.get('result', '')        
    try:               
        if 'on pens' in result or 'AET' in result: 
            team_goals = 0
            return 'draw', team_goals        
        elif venue == "H": # the team that the player belongs to played at their home  

            home_goals, away_goals = map(int, result.split(':'))  

            team_goals = home_goals                              

            if home_goals > away_goals:    return 'victory'
            elif home_goals == away_goals: return 'draw'
            else: return 'defeat', team_goals

        elif venue == "A": # the player's team played as an away team. 
            
            home_goals, away_goals = map(int, result.split(':')) 

            team_goals = away_goals
            
            if away_goals > home_goals:    return 'victory'
            elif away_goals == home_goals: return 'draw'
            else: return 'defeat' , team_goals
            
        else: return None
        
    except: None    

def get_team_goals(row):
    """
    Determine how much goals the player team scored in the match.
    """
    # Function to determine victory, draw, or defeat
    venue = row.get('venue', '')  # Use get to handle the case where 'venue' is not present
    result = row.get('result', '')        
    try:               
        if 'on pens' in result or 'AET' in result:  return 0       
        elif venue == "H": # the team that the player belongs to played at their home  

            home_goals, away_goals = map(int, result.split(':')) 

            return home_goals 

        elif venue == "A": # the player's team played as an away team. 
            
            home_goals, away_goals = map(int, result.split(':')) 

            return away_goals
            
        else: return 0
        
    except: None             

def save_general_stats(player, df):
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
            "team_goals": record['team goals'],
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



def scraper_stats_by_position(soup, player, year, season):
    url  = f"{player['url']}?saison={year}"
    soup = config_scraper(url)

    tables = soup.find_all('table')

    df = None

    for table in tables:
        if table.find('th', text='Played as...'):
            df = pd.read_html(str(table))[0]           
            break

    if df is not None:       

        # rename columns   
        df = df.rename(columns={'Unnamed: 1':'games','Unnamed: 2':'goals', 'Unnamed: 3':'assists'})  
        
        # replace NaN values with zeros
        df['goals']   = df['goals'].astype(str).str.replace("-","0")
        df['assists'] = df['assists'].astype(str).str.replace("-","0") 

        # convert strings to integers
        df['games']   = df['games'].astype(int)
        df['goals']   = df['goals'].astype(int) 
        df['assists'] = df['assists'].astype(int)   

        df["player"] = player["id"]
        df["season"] = season

        # reformat the dates in the 'season' column
        df['season'] = df['season'].str.replace(r'(\d{2})/(\d{2})', r'20\1-20\2')

        save_stats_by_position(player, df)      


def save_stats_by_position(player, df):
    # get the authentication token
    token = get_auth_token()
    # set headers to indicate JSON content and include the authentication token
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    for record in df.to_dict('records'):        
        data = {
            "player": record['player'], 
            "position": record['Played as...'],
            "games": record['games'],  
            "goals": record['goals'],
            "assists": record['assists'],              
            "season": record["season"],        
        } 
        
        # Convert data to JSON format
        data_json = json.dumps(data)
        # Make a POST request to add new data
        response_post = requests.post(API_STATS_BY_POSITION_URL, data=data_json, headers=headers)

        # Check the response status code for the POST request
        if response_post.status_code == 201:
            print(f"Data added successfully for player: {record['player']} - position: {record['Played as...']} - season: {record['season']}")        
        else:
            print(f"Failed to add data for player: {record['player']} - position: {record['Played as...']} - season: {record['season']}")
    


PLAYER_STATS=[
    {"id": 1, "name":"Lionel Messi", "url":'https://www.transfermarkt.com/lionel-messi/leistungsdaten/spieler/28003/plus/'},
    {"id": 2, "name":"Kylian Mbappe", "url":'https://www.transfermarkt.com/kylian-mbappe/leistungsdaten/spieler/342229/plus/'},
    {"id": 3, "name":"Erling Haaland", "url":'https://www.transfermarkt.com/erling-haaland/leistungsdaten/spieler/418560/plus/'},
    {"id": 4, "name":"Vinicius Junior", "url":'https://www.transfermarkt.com/vinicius-junior/leistungsdaten/spieler/371998/plus/'},
    {"id": 5, "name":"Robert Lewandowski", "url":'https://www.transfermarkt.com/robert-lewandowski/leistungsdaten/spieler/38253/plus/'},
    {"id": 6, "name":"Kevin De Bruyne", "url":'https://www.transfermarkt.com/kevin-de-bruyne/leistungsdaten/spieler/88755/plus/'},
    {"id": 7, "name":"Neymar", "url":'https://www.transfermarkt.com/neymar/leistungsdaten/spieler/68290/plus/'},
    {"id": 8, "name":"Harry Kane", "url":'https://www.transfermarkt.com/harry-kane/leistungsdaten/spieler/132098/plus/'},
    {"id": 9, "name":"Victor Osimhen", "url":'https://www.transfermarkt.com/victor-osimhen/leistungsdaten/spieler/401923/plus/'},
    {"id": 10, "name":"Lautaro Martinez", "url":'https://www.transfermarkt.com/lautaro-martinez/leistungsdaten/spieler/406625/plus/'},
    {"id": 11, "name":"Antoine Griezmann", "url":'https://www.transfermarkt.com/antoine-griezmann/leistungsdaten/spieler/125781/plus/'}
    ]


if __name__ == "__main__":
    for player in PLAYER_STATS: get_player_stats(player,update=True) 

   

