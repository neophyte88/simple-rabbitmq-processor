import os
import sys
import json
import random
import sched, time
import signal
from datetime import datetime

from dotenv import load_dotenv, dotenv_values
from loguru import logger as log

from utils.rabbitmq_handler import RabbitMQHandler

#Debug flag to enable/disable debug logs
DEBUG = "-d" in sys.argv

class StatusBroadcaster:
    
    def __init__(self, user, password, host, port, queue_name, debug=False) -> None:
        """
        Broadcaster sends out status messages with a random status value (0-6) every second

        Args:
            user (str): RabbitMQ username
            password (str): RabbitMQ password
            host (str): RabbitMQ server host
            port (str): RabbitMQ server port
            queue_name (str): RabbitMQ queue name
            debug (bool, optional): _description_. Defaults to False.
        """
        
        log.info("Initializing Status Broadcaster")
        
        self.debug = debug
        self.rabbitmq_handler = RabbitMQHandler(user, password, host, port, queue_name, self.debug)
        self.scheduler = sched.scheduler(time.time, time.sleep)
        # self.env = load_dotenv(".env")
        
    def exit_handler(self, sign, frame):
        log.info("Exiting properly")
        
        if self.rabbitmq_handler.channel.is_open:
            self.rabbitmq_handler.channel.stop_consuming()  # Stop consuming messages
        if self.rabbitmq_handler.connection.is_open:
            self.rabbitmq_handler.connection.close()  # Close the connection
            
        log.success("Exit sequence complete")
        sys.exit(0)
        
    def push_status(self):
        
        self.rabbitmq_handler.send_message(json.dumps({"status": random.randint(0, 6)}))
        
        if self.debug:
            log.debug(f"Sent Message at {datetime.now()}")
        
        self.scheduler.enter(1, 1, self.push_status)
        
    def run(self):
        
        signal.signal(signal.SIGINT, self.exit_handler)
        signal.signal(signal.SIGTERM, self.exit_handler)
        signal.signal(signal.SIGQUIT, self.exit_handler)
        
        self.scheduler.enter(1, 1, self.push_status)
        self.scheduler.run()

if __name__ == "__main__":
    
    env = dotenv_values()
    
    # print(env)
    
    user = env.get("RABBIT_MQ_USER")
    password = env.get("RABBIT_MQ_PASSWORD")
    host = env.get("RABBIT_MQ_HOST")
    port = env.get("RABBIT_MQ_PORT", 5672)
    queue_name = env.get("RABBIT_MQ_QUEUE_NAME")
    
    
    try:
        status_broadcaster = StatusBroadcaster(user, password, host, port, queue_name, DEBUG)
    
        if not status_broadcaster.rabbitmq_handler.connection:
            log.error("Error initializing RabbitMQ Handler")
            exit(1)
    
        status_broadcaster.run()
    except KeyboardInterrupt:
        status_broadcaster.exit_handler(None, None)