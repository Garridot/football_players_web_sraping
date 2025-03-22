import pika
import json
from players_url import PLAYERS_URL
from logger_config import logger
from config import CLOUDAMQP_URL

try:
    # Connect to RabbitMQ using the CloudAMQP URL
    connection = pika.BlockingConnection(pika.URLParameters(CLOUDAMQP_URL))
    channel = connection.channel()
    logger.info("Connected to RabbitMQ")
except pika.exceptions.AMQPConnectionError as e:
    logger.error(f"Failed to connect to RabbitMQ: {e}")
    exit(1)  # Exit script if unable to connect

# Declare the queue
channel.queue_declare(queue='scraping_tasks', durable=True)

# Send messages to the queue
for player in PLAYERS_URL:
    message = {
        'player_id': player['id'],
        'player_name': player['name'],
        'player_url': player['url'],
        'update': True
    }
    channel.basic_publish(
        exchange='',
        routing_key='scraping_tasks',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make the message persistent
        ))
    logger.info(f"Sent scraping task for {player['name']}")

# Close connection
connection.close()
logger.info("Connection to RabbitMQ closed")