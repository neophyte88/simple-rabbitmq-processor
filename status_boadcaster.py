import os
import sys
import json
import sched, time
from datetime import datetime
import random
from dotenv import load_dotenv, dotenv_values
from loguru import logger as log

from utils.rabbitmq_handler import RabbitMQHandler

DEBUG = "-d" in sys.argv

class StatusBroadcaster:
    
    def __init__(self, user, password, host, port, queue_name, debug=False) -> None:
        
        self.debug = debug
        self.rabbitmq_handler = RabbitMQHandler(user, password, host, port, queue_name, self.debug)
        self.scheduler = sched.scheduler(time.time, time.sleep)
        # self.env = load_dotenv(".env")
        
    def push_status(self):
        
        self.rabbitmq_handler.send_message(json.dumps({"status": random.randint(0, 6)}))
        
        if self.debug:
            log.debug(f"Sent Message at {datetime.now()}")
        
        self.scheduler.enter(1, 1, self.push_status)
        
    def run(self):
        
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
    
    status_broadcaster = StatusBroadcaster(user, password, host, port, queue_name, DEBUG)
    status_broadcaster.run()