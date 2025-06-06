from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from logger_config import logger
from data_manager import *
from save_data import *
from config import *
import pandas as pd
import requests
import json
import time
  
def setup_scraping(url): 
    """
    Set up and make the HTTP request and return the BeautifulSoup object.
    """
    session = requests.Session()
    retries = Retry(
        total=5,  # Maximum number of retries
        backoff_factor=2,  # Backoff time
        status_forcelist=[500, 502, 503, 504],  # Retry on these status codes
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(url, headers=HEADERS, timeout=4)  # 10-second timeout
        response.raise_for_status()  # Throw exception if the request was not successful
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    except requests.RequestException as e:
        logger.error(f"Error accessing {url}: {e}")
        return None    

def scraper_stats(player, update=False):
    """
    Get player statistics and call the web_scraping function.
    """
    soup = setup_scraping(player["url"])
    
    get_player_personal_data(player,soup)

    if update: 
        # if the option "update" was chosen, get information for the current season
        year   = soup.find('select').find_all('option')[1].get('value') 
        season = soup.find('select').find_all('option')[1].text         
        get_stats(player, year, season)
        get_stats_by_position(soup, player, year, season)
    else:  
        # get information for all seasons
        lists = soup.find('select').find_all('option')[1:]           
        for item in lists:            
            year = item.get('value')  
            season = item.text
            get_stats(player, year, season)
            get_stats_by_position(soup, player, year, season) 

def get_player_personal_data(player,soup):

    # Get Player Details
    player_details = soup.find('div',class_='data-header__details').find_all('ul',class_='data-header__items') 

    if not player_details:
        logger.error(f"No details found for {player['name']}")
        return 

    data = {}  
    
    data["name"]          = player["name"] 
    data["date_of_birth"] = player_details[0].find_all('li',class_='data-header__label')[0].find('span',class_='data-header__content').text
    data["position"]      = player_details[1].find_all('li',class_='data-header__label')[1].find('span',class_='data-header__content').text  
    data["nationality"]   = player_details[2].find_all('li',class_='data-header__label')[0].find('span',class_='data-header__content').text    
    data["current_club"]  = soup.find('span',class_='data-header__club').text  

    data = clean_personal_data(player,data)
    # Convert the DataFrame into JSON format
    data_json = json.dumps(data)

    save_personal_data(player,data_json)

def get_stats(player, year, season):
    """
    Make the extraction of statistics 
    """
    url = f"{player['url']}?saison={year}" # configure the player URL
    
    soup = setup_scraping(url) # make the webscraping

    dfs = [] # create an empty list to add the stats record  

    # Each statistic is in divs called "box" on the page. Get all "boxes" except the first two divs.
    boxes = soup.find('div',class_='large-8 columns').find_all('div',class_='box')[2:]
    
    for box in boxes:
        # The name of the competition is in the first div. Find the hyperlink and remove the whitespace from the text. 
        competition = box.find('div').find('a').text.strip() 
        # The team where the player plays is in the third "td" element. Find the hyperlink and get the attribute "title". 
        team   = box.find_all('td',class_='zentriert')[3].find('a').get('title')      
        
        stats  = box.find_all('table')
        
        # Process each statistics table
        for stat in stats:                         
            record = pd.read_html(str(stat))[0] # convert the HTML element into a DataFrame object  
            # read_html function is used to parse HTML tables from a webpage and convert them into DataFrame objects
            # str(stat): This part converts the element object into a string.
            # [0]: This is indexing notation ([0]) applied to the result of read_html. 
            
            # Create the columns and add the data collected earlier.  
            record['competition'] = competition            
            record['team']        = team
            record['season']      = season

            record = clean_general_stats(record) # clean and remove unnecesary data. 

            dfs.append(record) # add the dataframe in the list "dfs" created earlier.      

    if dfs: 
        # Concatenate all collected DataFrames
        df = pd.concat(dfs, ignore_index=True) 
        
        get_general_stats(df,player)

def get_general_stats(df,player):                     

    # Add additional columns
    df['victory'] = df.apply(lambda row: 1 if get_match_result(row) == 'victory' else 0, axis=1)
    df['draw'] = df.apply(lambda row: 1 if get_match_result(row) == 'draw' else 0, axis=1)
    df['defeat'] = df.apply(lambda row: 1 if get_match_result(row) == 'defeat' else 0, axis=1)
    df['team goals'] = df.apply(lambda row: get_team_goals(row), axis=1)

    # Calculate general statistics and display the result
    general_stats = df.groupby(['competition', 'team', 'season']).sum()
    games = df.groupby(['competition', 'team', 'season']).count().reset_index()['result']
    general_stats['games'] = games.tolist() 

    data_list = [] 

    for record, index in zip(general_stats.to_dict('records'), general_stats.index.to_list()):
        
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

        data_list.append(data)

    save_stats_data(API_PLAYER_STATS_URL,data_list) 

def get_stats_by_position(soup, player, year, season):
    url  = f"{player['url']}?saison={year}"
    soup = setup_scraping(url)

    df = None

    tables = soup.find_all('table')
    for table in tables:
        if table.find('th', text='Played as...'):
            df = pd.read_html(str(table))[0]           
            break

    if df is not None: 

        # Add columns in the dataframe
        df["player"] = player["id"]
        df["season"] = season

        df = clean_stats_by_position(df)

        data_list = []

        for record in df.to_dict('records'):        
            data = {
                "player": record['player'], 
                "position": record['Played as...'],
                "games": record['games'],  
                "goals": record['goals'],
                "assists": record['assists'],              
                "season": record["season"],        
            } 

            data_list.append(data)
    else:

        data_list = [
            {
                "player": player["id"], 
                "position": "data not available",
                "games": 0,  
                "goals": 0,
                "assists": 0,              
                "season": expand_year_range(season),   
            }             
            ]        

    save_stats_data(API_STATS_BY_POSITION_URL,data_list) 

