import os
import sys
import json
import time
import signal
from datetime import datetime

from loguru import logger as log
from dotenv import load_dotenv, dotenv_values

from utils.rabbitmq_handler import RabbitMQHandler
from utils.mongodb_handler import MongoDBHandler

#Debug flag to enable/disable debug logs
DEBUG = "-d" in sys.argv

class MessageProcessor:
    
    def __init__(self, user, password, host, port, queue_name, db_uri, db_name, collection_name,debug=False) -> None:
        """Message processor class to handle pulling and processing of messages from rabbitmq to mongodb

        Args:
            user (str): RabbitMQ username
            password (str): RabbitMQ password
            host (str): RabbitMQ server host
            port (str): RabbitMQ server port
            queue_name (str): RabbitMQ queue name
            db_uri (str): MongoDB URI
            db_name (str): MongoDB database name
            collection_name (str): MongoDB collection name
            debug (bool, optional): Print Debugging Messages. Defaults to False.
        """
        
        self.debug = debug
        self.rabbitmq_handler = RabbitMQHandler(user, password, host, port, queue_name, self.debug)
        self.mongodb_handler = MongoDBHandler(db_uri, db_name, collection_name, self.debug)
    
    def exit_handler(self, sign, frame):
        log.info("Exiting properly")
        
        if self.rabbitmq_handler.channel.is_open:
            self.rabbitmq_handler.channel.stop_consuming()  # Stop consuming messages
        if self.rabbitmq_handler.connection.is_open:
            self.rabbitmq_handler.connection.close()  # Close the connection
            
        if self.mongodb_handler.client is not None:
            self.mongodb_handler.client.close()
            
        log.success("Exit sequence complete")
        sys.exit(0)

    def fetch_message(self):
        
        rmq_message = self.rabbitmq_handler.pull_message()
        
        if self.debug:
            log.debug(f"Received message: {rmq_message['message']}")
        
        return rmq_message
    
    def push_to_mongodb(self, data):
        
        self.mongodb_handler.insert(data)
    
    def process_message(self, message):
        
        if self.debug:
            log.debug(f"Processing message: {message}")
            
        
        message_data = json.loads(message["message"])
        message_data["created_at"] = datetime.fromtimestamp(message["header"].timestamp)
        
        self.push_to_mongodb(message_data)
        
        return message_data
    
    def run(self):
        
        rmq_message = self.fetch_message()
        if rmq_message["message"] == "No Messages_available":
            
            log.debug("No messages available, exiting")    
            return False
        
        processed_message = self.process_message(rmq_message)
        return True
    
        
    def run_service(self):
        
        """ 
        This will run the processor as a service.
        It will keep running till there are messages in the queue, if its empty we will wait 10 seconds and check again
        """

        signal.signal(signal.SIGINT, self.exit_handler)
        signal.signal(signal.SIGTERM, self.exit_handler)
        signal.signal(signal.SIGQUIT, self.exit_handler)
        
        while True:
                
                status = self.run()
                
                if not status:
                    log.debug("No messages available, waiting for 10 seconds")
                    time.sleep(10)
                    
                # if self.debug:
                #     log.debug("Exiting")
                #     break
            
if __name__ == "__main__":
    
    env = dotenv_values()
    
    user = env.get("RABBIT_MQ_USER")
    password = env.get("RABBIT_MQ_PASSWORD")
    host = env.get("RABBIT_MQ_HOST")
    port = env.get("RABBIT_MQ_PORT", 5672)
    queue_name = env.get("RABBIT_MQ_QUEUE_NAME")
    db_url = env.get("MONGO_DB_URI")
    db_name = env.get("MONGO_DB_NAME")
    collection = env.get("MONGO_DB_COLLECTION_NAME")
    
    try:
        processor = MessageProcessor(user, password, host, port, queue_name, db_url, db_name, collection, DEBUG)
        
        if not processor.rabbitmq_handler.connection:
            log.error("Error initializing RabbitMQ Handler")
            exit(1)
        if not processor.mongodb_handler.client:
            log.error("Error initializing MongoDB Handler")
            exit(1)
            
        processor.run_service()
    
    except Exception as e:
        log.error(f"Error initializing Message Processor: {e}")
        exit(1)
    
    except KeyboardInterrupt:
        processor.exit_handler(None, None)
        exit(0)