import requests
import json

from config import *

async def get_auth_token(): 
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

