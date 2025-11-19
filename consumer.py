import pika
import json
from auth import get_auth_token
from scrapper import scraper_stats
from logger_config import logger
from config import CLOUDAMQP_URL,set_token
import threading

# Get the authentication token
token = get_auth_token()
set_token(token)  # Store the token in config.py

# Configure the connection to RabbitMQ
params = pika.URLParameters(CLOUDAMQP_URL + '?heartbeat=30') 
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare the queue
queue_name = 'scraping_tasks'
channel.queue_declare(queue=queue_name, durable=True)

def worker(ch, method, player, update):
    """Worker function to run the scraping task."""
    try:
        logger.info(f"STARTING scraping task for {player['name']} in a new thread. Update Data: {update}")
        scraper_stats(player, update)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"Finished scraping task for {player['name']} and ACK sent.")

    except Exception as e:
        logger.error(f"Error processing message for {player['name']}: {e}")
       
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

def callback(ch, method, properties, body):
    """Function to process messages."""
    try:
        message = json.loads(body)
        player = {
            'id': message['player_id'],
            'name': message['player_name'],
            'url': message['player_url']
        }
        update = message['update']

        thread = threading.Thread(target=worker, args=(ch, method, player, update))
        thread.start() # Exit script
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Reject the message and do not retry it
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

# Configure the consumer
channel.basic_qos(prefetch_count=1)  # Handle one message at a time.
channel.basic_consume(queue=queue_name, on_message_callback=callback)

logger.info("Waiting for scraping tasks. To exit press CTRL+C")
channel.start_consuming()
