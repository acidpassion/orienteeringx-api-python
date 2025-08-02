from fastapi import APIRouter, Depends
from datetime import datetime
from app.core.config import settings
from app.models.models import HealthCheck
from app.db.mongodb import get_database

router = APIRouter()


@router.get("/health", response_model=HealthCheck)
async def health_check(db=Depends(get_database)):
    """Health check endpoint"""
    
    # Check database connection
    try:
        await db.command('ping')
        database_status = "healthy"
    except Exception:
        database_status = "unhealthy"
    
    return HealthCheck(
        status="healthy" if database_status == "healthy" else "unhealthy",
        timestamp=datetime.utcnow(),
        version=settings.project_version,
        database_status=database_status
    )


@router.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": f"Welcome to {settings.project_name}",
        "version": settings.project_version,
        "docs_url": "/docs",
        "redoc_url": "/redoc"
    }