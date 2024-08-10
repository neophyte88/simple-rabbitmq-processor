from datetime import datetime

import pika
from loguru import logger as log

class RabbitMQHandler:
    
    
    
    def __init__(self, user, password, host, port, queue_name, debug=False) -> None:
        """
        Handles the RabbitMQ Operations for sending and receiving messages

        Args:
            user (str): RabbitMQ username
            password (str): RabbitMQ password
            host (str): RabbitMQ server host
            port (str): RabbitMQ server port
            queue_name (str): RabbitMQ queue name
            debug (bool, optional): Enable/Disable debug messages. Defaults to False.
        """
        
        self.rmq_user = user
        self.rmq_host_name = host
        self.rmq_port = port
        self.rmq_queue_name = queue_name
        
        self.debug = debug
        
        # Connection setup here
        self.credential_object = pika.PlainCredentials(self.rmq_user, password)
        self.connection_params = pika.ConnectionParameters(host=self.rmq_host_name, port = port, credentials=self.credential_object)
        self.connection = pika.BlockingConnection(self.connection_params)
        self.channel = self.connection.channel()
        
        self.channel.queue_declare(queue=self.rmq_queue_name, durable=True)
        
    def send_message(self, message:str):
        
        
        message_timestamp = int(datetime.now().timestamp())
        properties = pika.BasicProperties(timestamp= message_timestamp)
        
        if self.debug:
            log.debug(f"Sending message: {message} | Timestamp: {message_timestamp}")
            
        self.channel.basic_publish(
            exchange='', 
            routing_key=self.rmq_queue_name, 
            body=message,
            properties=properties
        )
        
    def pull_message(self):
            
            method_frame, header_frame, body = self.channel.basic_get(queue=self.rmq_queue_name, auto_ack=False)
            
            print(f"Method Frame: {method_frame, header_frame, body}")
            
            if method_frame:
                if self.debug:
                    log.debug(f"Received message: {body}")
                
                self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)    
                return {"message": body, "header": header_frame}
            
            
            return {"message":"No Messages_available"}
        
    
        
        