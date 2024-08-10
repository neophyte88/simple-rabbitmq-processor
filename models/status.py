import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class Status(BaseModel):
    
    id : str = Field(default_factory=uuid.uuid4, alias="_id")
    status : str
    created_on: datetime = Field(default_factory=datetime.now)
    
    
    class Config:
        
        schema_extra = {
            "example": {
                "status": "1",
                "created_on": "2024-08-10T00:00:00"
            }
        }