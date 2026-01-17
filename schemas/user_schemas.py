from pydantic import BaseModel, Field, validator
from typing import List, Optional


class UserCheckRequest(BaseModel):
    user_id: str = Field(..., description="Single user ID or comma-separated user IDs")

    @validator('user_id')
    def validate_user_id(cls, v):
        if not v or not v.strip():
            raise ValueError("user_id cannot be empty")
        
        # Split by comma and validate each ID
        user_ids = [uid.strip() for uid in v.split(',')]
        for uid in user_ids:
            if not uid:
                raise ValueError("Empty user_id found in comma-separated list")
            try:
                int(uid)  # Validate that each ID is numeric
            except ValueError:
                raise ValueError(f"Invalid user_id: {uid}. Must be numeric.")
        
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "116585"
            }
        }


class UserExistenceResult(BaseModel):
    user_id: int
    exists: bool


class UserData(BaseModel):
    user_id: int
    consumer_token: Optional[str] = None
    platform: str
    device_id: str
    source: Optional[str] = None  # Add source field to track where data came from


class UserDataResponse(BaseModel):
    """Response for single or multiple user data requests"""
    users: List[UserData]
    total: int
    not_found: List[int] = []


class UserCheckResponse(BaseModel):
    results: List[UserExistenceResult]

    class Config:
        json_schema_extra = {
            "example": {
                "results": [
                    {"user_id": 116585, "exists": True}
                ]
            }
        }


class HealthResponse(BaseModel):
    status: str
    redis: str
    kafka: str
    trino: str