import requests
import json
from logger_config import logger
from config import *


def save_stats_data(URL,data_list): 

    # set headers to indicate JSON content and include the authentication token
    token = get_token()
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}    

    for data in data_list:
        # Convert the DataFrame into JSON format
        data_json = json.dumps(data) 
        response_post = requests.post(URL, data=data_json, headers=headers)
        # Check the response status code for the POST request
        if response_post.status_code == 201:  
            logger.info(f"Data added successfully: {response_post.json()}")        
        else: 
            logger.error(f"Failed to add data: {response_post.json()}")  

def save_personal_data(player,data_json):

    # set headers to indicate JSON content and include the authentication token
    token = get_token()
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
            
    URL = f'{API_PLAYERS_URL}{player["id"]}/'           
    response_get = requests.get(URL, {"Content-Type": "application/json"})     

    if response_get.status_code == 200:
        # If the response returns a status "200", the player exists. Make a PUT request to update 
        response_put = requests.put(URL, data=data_json, headers=headers)   

        if response_put.status_code == 200: 
            logger.info(f"Data updated successfully for player: {response_put.json()}")
        else: 
            logger.error(f"Failed to update data for player: {response_put.json()}")

    else:     
        # If the response does not return the status "200", the player doesn't exist. Make a POST request to add the new player and their data
        response_put = requests.post(API_PLAYERS_URL, data=data_json, headers=headers) 

        if response_put.status_code == 201: 
            logger.info(f"Data added successfully for player: {response_put.json()}")
        else: 
            logger.error(f"Failed to add data for player: {response_put.json()}")
