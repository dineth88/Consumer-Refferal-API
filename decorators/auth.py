from fastapi import Depends, Header, HTTPException, status
from services.auth import TokenStore


async def cog_auth_required(
    authorization: str = Header(..., alias="Authorization"),
    token_store: TokenStore = Depends(),
):
    # token validation
    parts = authorization.split(" ", 1)

    if len(parts) != 2:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing Authorization header",
        )
    scheme, token = parts[0], parts[1]

    if scheme != "cog-api-token":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    data = await token_store.validate_token(token)
    if not data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing cog-api-token",
        )
    return True
