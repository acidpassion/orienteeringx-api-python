from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.core.config import settings
from app.api.v1.api import api_router
from app.api.job_management import router as job_management_router
from app.db.mongodb import connect_to_mongo, close_mongo_connection
from app.jobs.scheduler import scheduler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    logger.info("Starting up...")
    await connect_to_mongo()
    await scheduler.start()
    logger.info("Application started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    await scheduler.stop()
    await close_mongo_connection()
    logger.info("Application shutdown complete")


def create_application() -> FastAPI:
    """Create FastAPI application"""
    
    app = FastAPI(
        title=settings.project_name,
        version=settings.project_version,
        description="Enterprise-level FastAPI application with MongoDB and batch job system",
        openapi_url=f"{settings.api_v1_str}/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan
    )

    # Set all CORS enabled origins
    if settings.backend_cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[str(origin) for origin in settings.backend_cors_origins],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Include API router
    app.include_router(api_router, prefix=settings.api_v1_str)
    app.include_router(job_management_router)

    return app


app = create_application()