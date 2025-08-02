from fastapi import APIRouter
from app.api.v1.endpoints import auth, users, batch_jobs, health

api_router = APIRouter()

api_router.include_router(health.router, tags=["health"])
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(batch_jobs.router, prefix="/batch-jobs", tags=["batch-jobs"])