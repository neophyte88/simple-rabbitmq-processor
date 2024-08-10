# Simple Python Rabbitmq stream processor

This project was written as a submission for a coding task.

1. MQTT Messaging Integration:
        Implement MQTT messaging via RabbitMQ.
        Emit MQTT messages every second with a "status" field containing a random value between 0 and 6.
2. Message Processing:
        Develop a server script to process incoming MQTT messages.
        Store the processed messages in MongoDB.
3. Data Retrieval Endpoint:
        Create an endpoint to accept start and end times.
        Return the count of each status within the specified time range using MongoDB's aggregate pipeline.

## Setup Instructions

#### Requirements:

The following need to be in place before starting the setup

`Python : 3.10.12`
`RabbitMQ : 3.13.6`
`Erlang : 26.x`


#### Setup :

1. The python requirements can be installed using the requirements.txt file, eg:
    `pip install -r requirements.txt`

2. `.env` file needs to be created for the project to load variables, example schema can be found in .env.example

   
## Usage

The Project document called for 3 processes to be made
Here are the respective files

 1. Message Integration -> status_broadcaster.py
 - Command: ` python status_boadcaster.py -d `
  
 2. Message Processing  -> message_processor.py
 - Command: ` python message_processor.py -d `
    
 3. Data Retrieval Endpoint -> data_endpoint.py
 - Command: ` uvicorn data_endpoint:app --host 0.0.0.0 --port 8000 `

## Flow

1. The status broadcaster script when run will broadcast/publish one message with a random status value (0 to 6) 
per second to the Rabbit MQ Queue.
   eg. `{"status": 3}`
   
2. The message_processor script when executed, will start fetching and commiting messages from the queue to the MongoDB collection with the timestamp of the message and waits 10 seconds if the queue is exhausted.
   
3. The data endpoint is a FastAPI app that exposes a endpoint
   1. This endpoint accepts a post request with `start_time` and `end_time`
   2. Returned data looks as follows:
    ```json
      {
        "0": 1,
        "1": 4,
        "2": 3,
        "3": 1,
        "4": 3,
        "5": 3,
        "6": 2
      } 
    ```