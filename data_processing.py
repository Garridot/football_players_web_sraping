import pandas as pd 
import re
import datetime

def clean_general_stats(table):    
    """
    Perform cleaning and transformations on the statistics DataFrame.
    """    
    # delete the row 'total'
    table = table[table['Date'].str.contains('Squad') == False ] 
    # Drop unnecessary columns
    columns_to_drop =   ['Matchday','For','For.1', 'Opponent','Unnamed: 11','Unnamed: 12','Unnamed: 13','Unnamed: 15','Unnamed: 16','Unnamed: 17']
    table = table.drop(columns=columns_to_drop, errors='ignore')
    # rename columns        
    table = table.rename(
        columns={'Date':'date','Venue':'venue','Opponent.1':'opponent','Result':'result','Unnamed: 9':'goals','Unnamed: 10':'assists','Unnamed: 14':'minutes played'}
    )  
    # normalize name teams: from 'team(.1)' to 'team'
    table['opponent'] = table['opponent'].str.replace(r'\(\d+\.\)', '').str.strip()
    table['team'] = table['team'].str.replace(r'\(\d+\.\)', '').str.strip()       
    # replace NaN values with zeros
    table['goals'], table['assists'] = table['goals'].fillna(0), table['assists'].fillna(0) 
    # remove matches he has not played
    table = table.drop(table[table['goals'].astype(str).str.match('.*[a-zA-Z].*')].index)      
    table['minutes played'] = table['minutes played'].str.replace("'", '').astype(int)    
    # reformat the dates in the 'season' column: from "23/24" to "2023-2024" 
    table['season'] = table['season'].str.replace(r'(\d{2})/(\d{2})', r'20\1-20\2')
    # convert strings numbers to integers
    table['goals']   = table['goals'].astype(int) 
    table['assists'] = table['assists'].astype(int) 

    # convert the "date" column as a datetime type for proper sorting: from "3/8/24" to "2024-03-08"
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

def clean_stats_by_position(df):
    # rename columns   
    df = df.rename(columns={'Unnamed: 1':'games','Unnamed: 2':'goals', 'Unnamed: 3':'assists'})  
    
    # replace NaN values with zeros
    df['goals']   = df['goals'].astype(str).str.replace("-","0")
    df['assists'] = df['assists'].astype(str).str.replace("-","0") 

    # convert strings to integers
    df['games']   = df['games'].astype(int)
    df['goals']   = df['goals'].astype(int) 
    df['assists'] = df['assists'].astype(int) 
    
    # reformat the dates in the 'season' column
    df['season'] = df['season'].str.replace(r'(\d{2})/(\d{2})', r'20\1-20\2')

    return df

def clean_personal_data(player,data):
    # Remove white spacess
    data["date_of_birth"] = data["date_of_birth"].strip()   
    data["position"]      = data["position"].strip()
    data["nationality"]   = data["nationality"].strip()  
    data["current_club"]  = data["current_club"].strip()

    
    age  = re.search(r'\((\d+)\)', data["date_of_birth"]).group(1) # # get the current age; use a regular expression to extract the number within parentheses.  

    birth = data["date_of_birth"][0:-5] # get the birth; remove unnecessary string content to convert to a datetime object.     
    date_object = datetime.datetime.strptime(birth, "%b %d, %Y") # convert the string to a datetime object.    
    formatted_birth = date_object.strftime("%Y-%m-%d") # format the datetime object as a string in "YYYY-MM-DD" format. 
    
    data["age"] = age # add new element in the dict.
    data["date_of_birth"] = formatted_birth # update with a correct date format the element "date_of_birth".  

    return data      