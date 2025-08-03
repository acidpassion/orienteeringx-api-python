from pydantic_settings import BaseSettings
from typing import List, Optional
import os


class Settings(BaseSettings):
    # Environment
    environment: str = "development"
    debug: bool = True
    
    # Database
    mongodb_url: str
    database_name: str = "orienteeringx_db"
    
    # API
    api_v1_str: str = "/api/v1"
    project_name: str = "OrienteeringX API"
    project_version: str = "1.0.0"
    
    # Security
    secret_key: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    # Redis
    redis_url: str = "redis://localhost:6379/0"
    
    # Scheduler
    scheduler_timezone: str = "UTC"
    max_workers: int = 4
    
    # CORS
    backend_cors_origins: List[str] = []

    model_config = {
        "env_file": os.getenv("ENV_FILE", ".env"),
        "case_sensitive": False
    }


settings = Settings()