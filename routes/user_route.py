"""
User routes with authentication protection
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Path, Depends
from schemas.user_schemas import (
    UserCheckRequest, 
    UserCheckResponse, 
    UserExistenceResult,
    HealthResponse,
    UserData,
    UserDataResponse
)
from services.redis_service import redis_service
from services.kafka_service import kafka_service
from services.trino_service import trino_service
from services.rds_service import rds_service
from services.datasource_service import datasource_service, DataSource
from services.sync_service import sync_service
from services.parallel_fetch_service import parallel_fetch_service
from decorators.auth import cog_auth_required
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/check-user/{user_ids}", 
    response_model=UserCheckResponse,
    dependencies=[Depends(cog_auth_required)]
)
async def check_user_exists_get(
    user_ids: str = Path(..., description="Single user ID or comma-separated user IDs (e.g., 116585 or 116585,123456,789012)")
):
    try:
        # Validate and parse user IDs
        if not user_ids or not user_ids.strip():
            raise HTTPException(status_code=400, detail="user_ids cannot be empty")
        
        # Split by comma and validate each ID
        user_id_list = [uid.strip() for uid in user_ids.split(',')]
        results = []
        
        for uid in user_id_list:
            if not uid:
                raise HTTPException(status_code=400, detail="Empty user_id found in comma-separated list")
            try:
                user_id_int = int(uid)
                exists = redis_service.user_exists(user_id_int)
                results.append(UserExistenceResult(user_id=user_id_int, exists=exists))
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid user_id: {uid}. Must be numeric.")
        
        return UserCheckResponse(results=results)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking user existence: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/user/{user_ids}", 
    response_model=UserDataResponse,
    dependencies=[Depends(cog_auth_required)]
)
async def get_user_data(
    user_ids: str = Path(..., description="Single user ID or comma-separated user IDs (e.g., 91061 or 91061,91062,91063)")
):
    """
    Retrieve complete user data for single or multiple users.
    
    - In LAKE mode: Gets data from Redis only
    - In RDS mode: Queries both Redis and RDS in parallel, returns fastest response
    
    Supports comma-separated user IDs for batch retrieval.
    
    **Requires Authentication**: Include `Authorization: cog-api-token <token>` header
    """
    try:
        # Parse and validate user IDs
        if not user_ids or not user_ids.strip():
            raise HTTPException(status_code=400, detail="user_ids cannot be empty")
        
        user_id_list = []
        for uid in user_ids.split(','):
            uid = uid.strip()
            if not uid:
                raise HTTPException(status_code=400, detail="Empty user_id in list")
            try:
                user_id_list.append(int(uid))
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid user_id: {uid}. Must be numeric.")
        
        # Check if we have too many IDs (limit to 100 for safety)
        # if len(user_id_list) > 100:
        #     raise HTTPException(status_code=400, detail="Too many user IDs. Maximum 100 allowed.")
        
        # Check current data source
        if datasource_service.is_lake_active():
            # LAKE MODE: Get from Redis only
            logger.info(f"Fetching {len(user_id_list)} user(s) from Redis (LAKE mode)")
            
            found_users = []
            not_found = []
            
            for user_id in user_id_list:
                user_data = redis_service.get_user_data(user_id)
                if user_data:
                    user_data['source'] = 'redis'
                    found_users.append(user_data)
                else:
                    not_found.append(user_id)
            
            return UserDataResponse(
                users=found_users,
                total=len(found_users),
                not_found=not_found
            )
        
        else:
            # RDS MODE: Parallel fetch from both Redis and RDS
            logger.info(f"Fetching {len(user_id_list)} user(s) in parallel from Redis and RDS")
            
            if len(user_id_list) == 1:
                # Single user - use parallel fetch
                user_data = await parallel_fetch_service.fetch_user_parallel(user_id_list[0])
                
                if not user_data:
                    raise HTTPException(
                        status_code=404,
                        detail=f"User {user_id_list[0]} not found in any source"
                    )
                
                return UserDataResponse(
                    users=[user_data],
                    total=1,
                    not_found=[]
                )
            
            else:
                # Multiple users - batch parallel fetch
                result = await parallel_fetch_service.fetch_multiple_users_parallel(user_id_list)
                
                return UserDataResponse(
                    users=result['users'],
                    total=result['total'],
                    not_found=result['not_found']
                )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving user data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post(
    "/switch-rds",
    dependencies=[Depends(cog_auth_required)]
)
async def switch_to_rds():
    """
    Switch data source to RDS.
    Stops Kafka consumer and uses RDS for all queries.
    Queries will automatically use parallel Redis+RDS fetching.
    
    **Requires Authentication**
    """
    try:
        # Stop Kafka consumer
        logger.info("Stopping Kafka consumer...")
        kafka_service.stop_consuming()
        
        # Connect to RDS if not already connected
        if not rds_service.health_check():
            logger.info("Connecting to RDS...")
            rds_service.connect()
        
        # Switch data source
        result = datasource_service.switch_to_rds()
        
        logger.info("Successfully switched to RDS data source (parallel mode enabled)")
        return result
        
    except Exception as e:
        logger.error(f"Error switching to RDS: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to switch to RDS: {str(e)}")


@router.post(
    "/switch-lake",
    dependencies=[Depends(cog_auth_required)]
)
async def switch_to_lake():
    """
    Switch data source back to Lake (Trino + Kafka).
    Performs initial sync from Trino and restarts Kafka consumer.
    
    **Requires Authentication**
    """
    try:
        # Ensure Trino is connected
        if not trino_service.health_check():
            logger.info("Connecting to Trino...")
            trino_service.connect()
        
        # Perform initial sync from Trino to Redis
        logger.info("Performing initial sync from Trino to Redis...")
        sync_service.initial_sync()
        
        # Ensure Kafka is connected
        if not kafka_service.health_check():
            logger.info("Connecting to Kafka...")
            kafka_service.connect()
        
        # Start Kafka consumer
        logger.info("Starting Kafka consumer...")
        kafka_service.start_consuming()
        
        # Switch data source
        result = datasource_service.switch_to_lake()
        
        logger.info("Successfully switched to Lake data source")
        return result
        
    except Exception as e:
        logger.error(f"Error switching to Lake: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to switch to Lake: {str(e)}")


@router.get(
    "/datasource-status",
    dependencies=[Depends(cog_auth_required)]
)
async def get_datasource_status():
    """
    Get current data source status and health of all components
    
    **Requires Authentication**
    """
    try:
        status = datasource_service.get_status()
        
        # Add health status of each service
        status["services"] = {
            "redis": "healthy" if redis_service.health_check() else "unhealthy",
            "kafka": "healthy" if kafka_service.health_check() else "unhealthy",
            "trino": "healthy" if trino_service.health_check() else "unhealthy",
            "rds": "healthy" if rds_service.health_check() else "unhealthy"
        }
        
        # Add parallel fetch info
        status["parallel_mode"] = datasource_service.is_rds_active()
        
        return status
        
    except Exception as e:
        logger.error(f"Error getting datasource status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint to verify all services are running.
    This endpoint does NOT require authentication.
    """
    redis_status = "healthy" if redis_service.health_check() else "unhealthy"
    kafka_status = "healthy" if kafka_service.health_check() else "unhealthy"
    trino_status = "healthy" if trino_service.health_check() else "unhealthy"
    
    overall_status = "healthy" if all([
        redis_status == "healthy",
        kafka_status == "healthy",
        trino_status == "healthy"
    ]) else "degraded"
    
    return HealthResponse(
        status=overall_status,
        redis=redis_status,
        kafka=kafka_status,
        trino=trino_status
    )