import pika
from loguru import logger as log

class RabbitMQHandler:
    
    def __init__(self, user, password, host, port, queue_name, debug=False) -> None:
        
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
        
        if self.debug:
            log.debug(f"Sending message: {message}")
            
        self.channel.basic_publish(exchange='', routing_key=self.rmq_queue_name, body=message)
        
    
        
        