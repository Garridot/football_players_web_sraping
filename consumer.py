import pika
import json
from auth import get_auth_token
from scrapper import scraper_stats
from logger_config import logger
from config import CLOUDAMQP_URL,set_token

# Get the authentication token
token = get_auth_token()
set_token(token)  # Store the token in config.py

# Configure the connection to RabbitMQ
connection = pika.BlockingConnection(pika.URLParameters(CLOUDAMQP_URL))
channel = connection.channel()

# Declare the queue
queue_name = 'scraping_tasks'
channel.queue_declare(queue=queue_name, durable=True)

def is_queue_empty(channel, queue_name):
    """Check if the queue is empty."""
    method_frame, _, _ = channel.basic_get(queue=queue_name)
    if method_frame:
        # If there is a message, reject it so it returns to the queue.
        channel.basic_reject(method_frame.delivery_tag, requeue=True)
        return False
    return True

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

        logger.info(f"Processing scraping task for {player['name']}")
        scraper_stats(player, update)

        # Confirm the message was processed
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Check if the queue is empty
        if is_queue_empty(channel, queue_name):
            logger.info("No more messages in the queue. Closing connection...")
            connection.close()
            exit(0)  # Exit script
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Reject the message and do not retry it
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

# Configure the consumer
channel.basic_qos(prefetch_count=1)  # Handle one message at a time.
channel.basic_consume(queue=queue_name, on_message_callback=callback)

logger.info("Waiting for scraping tasks. To exit press CTRL+C")
channel.start_consuming()
