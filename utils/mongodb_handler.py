
from loguru import logger as log
from pymongo.mongo_client import MongoClient

class MongoDBHandler:
    
    def __init__(self, uri, db_name, collection_name, debug=False) -> None:
        
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.debug = debug
        
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        
    def insert(self, data):
        
        result = self.collection.insert_one(data)
        log.debug(f"Inserted data: {result.inserted_id}")
        
        return result
    
    def fetch_by_id(self, id):
        
        result = self.collection.find_one({"_id": id})
        log.debug(f"Fetched data: {result}")
        
        return result
    
    def test_connection(self):
        
        try:
            self.client.admin.command('ping')
            log.debug("Connection successfull")
            return True
        
        except Exception as e:
            log.error(f"Error connecting to the database: {e}")
            return False