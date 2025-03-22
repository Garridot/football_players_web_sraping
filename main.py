from requests.exceptions import ConnectionError, RequestException
from auth import get_auth_token
from players_url import PLAYERS_URL
from logger_config import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from scrapper import scraper_stats
from config import set_token

# Obtain the token and store it in the config.py file.
token = get_auth_token()
set_token(token)

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=5) as executor: 
        # Send the tasks to the executor.
        future_to_player = {executor.submit(scraper_stats, player, True): player for player in PLAYERS_URL}

        # Gat the results as they are finished.
        for future in as_completed(future_to_player):
            player = future_to_player[future]
            try:
                future.result()  # Obtain the outcome (or exception) of the task.
            except ConnectionError as e:
                logger.error(f"Connection error processing {player['name']}: {e}")
            except RequestException as e:
                logger.error(f"Request error processing {player['name']}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error processing {player['name']}: {e}")



   

