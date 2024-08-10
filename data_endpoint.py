from datetime import datetime, time, timedelta
from typing import Annotated

from contextlib import asynccontextmanager

from dotenv import dotenv_values
from fastapi import Body, FastAPI

from utils.mongodb_handler import MongoDBHandler
from models.status import Status

DEBUG = True


@asynccontextmanager
async def lifespan(app: FastAPI):
    
    env = dotenv_values()
    
    db_url = env.get("MONGO_DB_URI")
    db_name = env.get("MONGO_DB_NAME")
    collection = env.get("MONGO_DB_COLLECTION_NAME")
    
    app.mongodb_handler = MongoDBHandler(
        uri=db_url,
        db_name=db_name,
        collection_name=collection,
        debug=DEBUG
    )
    
    yield
    
    app.mongodb_handler.client.close()



app = FastAPI(lifespan=lifespan)

@app.post("/get_status_counts")
async def get_status_counts(
    start_time: Annotated[datetime, Body()],
    end_time: Annotated[datetime, Body()],
    ):
    """Endpoint to get the status counts between the given time range

    Args:
        start_time (Annotated[datetime, Body): Start time for the query 
        end_time (Annotated[datetime, Body): End time for the query

    Returns:
        _type_: dict: Status counts between the given time range
    """
    aggregator_query = [{
        '$match': {
        'created_at': {
            '$gte': start_time,
            '$lte': end_time 
        }
        }
    },
    {
    '$group': {
        '_id': '$status', 
        'count': { '$sum': 1 }
    }
    }]
    
    query_object = app.mongodb_handler.collection.aggregate(aggregator_query)
    
    data = {}
    
    for result in query_object:
        data[result['_id']] = result['count']
    
    return data
