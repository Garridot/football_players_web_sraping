import logging

logging.basicConfig(
    level=logging.INFO,  
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler() 
    ]
)

logger = logging.getLogger("save_stats_data")

