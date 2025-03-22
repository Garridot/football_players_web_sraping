from dotenv import load_dotenv
import os

load_dotenv() # take environment variables from .env.

API_AUTH_URL         = os.getenv('API_AUTH_URL')     # API endpoint for authentication
API_PLAYER_STATS_URL = os.getenv('API_PLAYER_STATS_URL') # API endpoint for players stats
API_PLAYERS_URL      = os.getenv('API_PLAYERS_URL')      # API endpoint for players 
API_STATS_BY_POSITION_URL = os.getenv('API_STATS_BY_POSITION_URL' ) # API endpoint for players stats by position
EMAIL    = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')
CLOUDAMQP_URL = os.getenv("CLOUDAMQP_URL")

# Define headers at the module level to avoid repetition
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.google.com/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'DNT': '1'
    }

token = None

def set_token(new_token):
    global token
    token = new_token

def get_token():
    return token