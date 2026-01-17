"""
Main FastAPI application with authentication
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from routes import auth_route, user_route
from services.redis_service import redis_service
from services.kafka_service import kafka_service
from services.trino_service import trino_service
from services.rds_service import rds_service
from services.sync_service import sync_service
from services.datasource_service import datasource_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events
    """
    # Startup
    logger.info("Starting application...")
    
    try:
        # Initialize Redis
        logger.info("Connecting to Redis...")
        redis_service.connect()
        
        # Initialize Trino
        logger.info("Connecting to Trino...")
        trino_service.connect()
        
        # Perform initial sync
        logger.info("Performing initial sync from Trino to Redis...")
        sync_service.initial_sync()
        
        # Initialize and start Kafka consumer
        logger.info("Starting Kafka consumer...")
        kafka_service.connect()
        kafka_service.start_consuming()
        
        # Initialize RDS (optional, connects on-demand)
        logger.info("RDS service initialized (will connect on-demand)")
        
        logger.info("Authentication system enabled")
        logger.info("All services started successfully!")
        logger.info("API documentation available at /docs")
        
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    try:
        # Stop Kafka consumer
        logger.info("Stopping Kafka consumer...")
        kafka_service.stop_consuming()
        
        # Close connections
        logger.info("Closing service connections...")
        if redis_service:
            redis_service.close()
        if trino_service:
            trino_service.close()
        if rds_service:
            rds_service.close()
        
        logger.info("Application shut down successfully")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


# Create FastAPI app with lifespan
app = FastAPI(
    title="User Management API",
    description="API for user data management with authentication, supporting both Lake (Trino+Kafka) and RDS data sources",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth_route.router, tags=["Authentication"])
app.include_router(user_route.router, prefix="/api", tags=["Users"])


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "User Management API",
        "version": "1.0.0",
        "docs": "/docs",
        "authentication": "Required for protected endpoints",
        "endpoints": {
            "auth": {
                "register": "/auth/register",
                "login": "/auth/login",
                "revoke": "/auth/revoke-token"
            },
            "users": {
                "check_user": "/api/check-user/{user_ids}",
                "get_user": "/api/user/{user_ids}",
                "switch_rds": "/api/switch-rds",
                "switch_lake": "/api/switch-lake",
                "status": "/api/datasource-status",
                "health": "/api/health"
            }
        }
    }


@app.get("/status", tags=["Root"])
async def api_status():
    """Get current API and data source status"""
    try:
        datasource_status = datasource_service.get_status()
        
        return {
            "api": "running",
            "current_datasource": datasource_status.get("current_source"),
            "services": {
                "redis": "healthy" if redis_service.health_check() else "unhealthy",
                "kafka": "healthy" if kafka_service.health_check() else "unhealthy",
                "trino": "healthy" if trino_service.health_check() else "unhealthy",
                "rds": "healthy" if rds_service.health_check() else "unhealthy"
            }
        }
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return {
            "api": "running",
            "error": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True, 
        log_level="info"
    )