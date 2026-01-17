import secrets
import time
from fastapi import HTTPException
from redis import asyncio as aioredis
from helpers.auth.password_handler import hash_password, verify_password
from pathlib import Path
from dotenv import load_dotenv
from core.redis_config import RedisConnectionConfig

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


class TokenStore:
    def __init__(self, redis_url=RedisConnectionConfig.REDIS_CONNECTION_HOST, prefix="api_token:", user_prefix="user:"):
        self.redis_url = redis_url
        self.prefix = prefix
        self.user_prefix = user_prefix
        self.redis = None

    async def connect(self):
        self.redis = aioredis.from_url(self.redis_url, decode_responses=True)

    async def register_user(self, email: str, password: str):
        await self.connect()
        key = f"{self.user_prefix}_{email.lower()}"
        exists = await self.redis.exists(key)
        if exists:
            raise HTTPException(
                status_code=400,
                detail="User already exists"
            )

        hashed = hash_password(password)
        await self.redis.hset(key, mapping={
            "email": email,
            "password": hashed
        })
        return {"email": email, "message": "User registered successfully"}

    async def generate_token(self, email: str, password: str):
        await self.connect()
        user_key = f"{self.user_prefix}_{email.lower()}"
        user_data = await self.redis.hgetall(user_key)
        if not user_data:
            raise ValueError("User not found")

        if not verify_password(password, user_data.get("password", "")):
            raise ValueError("Invalid credentials")

        token = secrets.token_urlsafe(32)
        token_key = f"{self.prefix}_{token}"

        await self.redis.hset(token_key, mapping={
            "email": email,
            "created_at": str(int(time.time()))
        })
        # await self.redis.expire(token_key, ttl_seconds)
        return token

    async def validate_token(self, token: str):
        await self.connect()
        key = f"{self.prefix}_{token}"
        data = await self.redis.hgetall(key)
        return data if data else None

    async def revoke_token(self, token: str):
        await self.redis.delete(f"{self.prefix}{token}")
