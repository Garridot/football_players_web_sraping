# purge_queue.py
import pika
from config import CLOUDAMQP_URL
from logger_config import logger

queue_name = 'scraping_tasks'

try:
    # 1. Conectar a RabbitMQ
    connection = pika.BlockingConnection(pika.URLParameters(CLOUDAMQP_URL))
    channel = connection.channel()
    logger.info("Connected to RabbitMQ")
    
    # 2. Purga la cola (elimina todos los mensajes pendientes)
    method_frame = channel.queue_purge(queue=queue_name)
    
    # 3. Muestra cuántos mensajes se eliminaron
    messages_purged = method_frame.method.message_count
    logger.info(f"Successfully purged {messages_purged} messages from '{queue_name}' queue.")
    
    # 4. Cierra la conexión
    connection.close()
    
except pika.exceptions.AMQPConnectionError as e:
    logger.error(f"Failed to connect to RabbitMQ: {e}")
    exit(1)
except Exception as e:
    logger.error(f"An error occurred during purge: {e}")