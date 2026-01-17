"""
Authentication routes for user registration and login
"""
from fastapi import APIRouter, HTTPException, Depends
from schemas.auth_schemas import (
    UserRegisterRequest,
    UserLoginRequest,
    TokenResponse,
    MessageResponse
)
from services.auth import TokenStore
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Authentication"])


def get_token_store():
    """Dependency to get TokenStore instance"""
    return TokenStore()


@router.post("/register", response_model=MessageResponse, status_code=201)
async def register_user(
    user_data: UserRegisterRequest,
    token_store: TokenStore = Depends(get_token_store)
):
    """
    Register a new user with email and password.
    
    - **email**: Valid email address
    - **password**: Must be at least 8 characters
    """
    try:
        result = await token_store.register_user(user_data.email, user_data.password)
        logger.info(f"User registered successfully: {user_data.email}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Registration error: {e}")
        raise HTTPException(status_code=500, detail="Registration failed")


@router.post("/login", response_model=TokenResponse)
async def login_user(
    credentials: UserLoginRequest,
    token_store: TokenStore = Depends(get_token_store)
):
    """
    Login with email and password to receive an API token.
    
    Returns a token that should be used in the Authorization header:
    `Authorization: cog-api-token <your_token>`
    """
    try:
        token = await token_store.generate_token(credentials.email, credentials.password)
        logger.info(f"User logged in successfully: {credentials.email}")
        return TokenResponse(token=token)
    except ValueError as e:
        logger.warning(f"Login failed for {credentials.email}: {e}")
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(status_code=500, detail="Login failed")


@router.post("/revoke-token", response_model=MessageResponse)
async def revoke_token(
    token: str,
    token_store: TokenStore = Depends(get_token_store)
):
    """
    Revoke/logout a token.
    
    - **token**: The token to revoke
    """
    try:
        await token_store.revoke_token(token)
        logger.info("Token revoked successfully")
        return MessageResponse(message="Token revoked successfully")
    except Exception as e:
        logger.error(f"Token revocation error: {e}")
        raise HTTPException(status_code=500, detail="Token revocation failed")