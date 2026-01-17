"""
Authentication schemas for request/response models
"""
from pydantic import BaseModel, EmailStr, Field


class UserRegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, description="Password must be at least 8 characters")

    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "password": "securePassword123"
            }
        }


class UserLoginRequest(BaseModel):
    email: EmailStr
    password: str

    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "password": "securePassword123"
            }
        }


class TokenResponse(BaseModel):
    token: str
    token_type: str = "cog-api-token"

    class Config:
        json_schema_extra = {
            "example": {
                "token": "your_generated_token_here",
                "token_type": "cog-api-token"
            }
        }


class MessageResponse(BaseModel):
    message: str
    email: str = None

    class Config:
        json_schema_extra = {
            "example": {
                "message": "User registered successfully",
                "email": "user@example.com"
            }
        }