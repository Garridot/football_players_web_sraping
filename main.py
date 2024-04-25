from bs4 import BeautifulSoup
import pandas as pd
import requests
import json
import re

from config import *
from data_processing import *
from auth import get_auth_token
from players_url import PLAYERS_URL


from datetime import datetime


token = get_auth_token()
  
def setup_scraping(url): 
    """
    Set up and make the HTTP request and return the BeautifulSoup object.
    """
    res  = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(res.content, 'html.parser')
    return soup


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
        get_player_stats(player, year, season)
#         scraper_stats_by_position(soup, player, year, season)
    else:  
        # get information for all seasons
        lists = soup.find('select').find_all('option')[1:]           
        for item in lists:            
            year = item.get('value')  
            season = item.text
            get_player_stats(player, year, season)
            scraper_stats_by_position(soup, player, year, season) 
            
            
def get_player_personal_data(player,soup):

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

    # Convert the DataFrame into JSON format
    data_json = json.dumps(data)

    # set headers to indicate JSON content and include the authentication token
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
        
    
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
               

def get_player_stats(player, year, season):
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
        
        get_general_stats(df)

def get_general_stats(df):                     

    # Add additional columns
    df['victory'] = df.apply(lambda row: 1 if get_match_result(row) == 'victory' else 0, axis=1)
    df['draw'] = df.apply(lambda row: 1 if get_match_result(row) == 'draw' else 0, axis=1)
    df['defeat'] = df.apply(lambda row: 1 if get_match_result(row) == 'defeat' else 0, axis=1)
    df['team goals'] = df.apply(lambda row: get_team_goals(row), axis=1)

    # Calculate general statistics and display the result
    general_stats = df.groupby(['competition', 'team', 'season']).sum()
    games = df.groupby(['competition', 'team', 'season']).count().reset_index()['result']
    general_stats['games'] = games.tolist()  

    for record, index in zip(general_stats.to_dict('records'), general_stats.index.to_list()):
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

        # Convert the DataFrame into JSON format
        data_json = json.dumps(data)

        save_data(API_PLAYER_STATS_URL,data_json)    

    

def scraper_stats_by_position(soup, player, year, season):
    url  = f"{player['url']}?saison={year}"
    soup = setup_scraping(url)

    tables = soup.find_all('table')

    df = None

    for table in tables:
        if table.find('th', text='Played as...'):
            df = pd.read_html(str(table))[0]           
            break

    if df is not None: 

        df = clean_stats_by_position(df)  

        df["player"] = player["id"]
        df["season"] = season

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

            save_data(API_STATS_BY_POSITION_URL,data_json)    

def save_data(URL,data_json): 

    # set headers to indicate JSON content and include the authentication token
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}    

    response_post = requests.post(URL, data=data_json, headers=headers)

    # Check the response status code for the POST request
    if response_post.status_code == 201:
        print(f"Data added successfully: {response_post.json()}")        
    else:
        print(f"Failed to add data: {response_post.json()}")  


if __name__ == "__main__":
    for player in PLAYERS_URL: scraper_stats(player,update=True) 

   

