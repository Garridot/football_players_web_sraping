from dotenv import load_dotenv
import os

load_dotenv() # take environment variables from .env.

API_AUTH_URL         = os.getenv('API_AUTH_URL')     # API endpoint for authentication
API_PLAYER_STATS_URL = os.getenv('API_PLAYER_STATS_URL') # API endpoint for players stats
API_PLAYERS_URL      = os.getenv('API_PLAYERS_URL')      # API endpoint for players 
API_STATS_BY_POSITION_URL = os.getenv('API_STATS_BY_POSITION_URL' ) # API endpoint for players stats by position
EMAIL    = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

# Define headers at the module level to avoid repetition
HEADERS = {'User-Agent': os.getenv("USER_AGENT")}

