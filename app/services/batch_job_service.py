from typing import List, Optional, Dict, Any
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.models.models import BatchJob
from app.schemas.schemas import BatchJobCreate, BatchJobUpdate
from bson import ObjectId
from datetime import datetime


class BatchJobService:
    def __init__(self, database: AsyncIOMotorDatabase):
        self.database = database
        self.collection = database.batch_jobs

    async def create_job(self, job_data: BatchJobCreate, created_by: Optional[str] = None) -> BatchJob:
        """Create a new batch job"""
        job_dict = job_data.dict()
        job_dict["status"] = "pending"
        job_dict["created_at"] = datetime.utcnow()
        job_dict["created_by"] = created_by
        
        result = await self.collection.insert_one(job_dict)
        job_dict["_id"] = result.inserted_id
        
        return BatchJob(**job_dict)

    async def get_job_by_id(self, job_id: str) -> Optional[BatchJob]:
        """Get job by ID"""
        job_data = await self.collection.find_one({"_id": ObjectId(job_id)})
        if job_data:
            return BatchJob(**job_data)
        return None

    async def update_job(self, job_id: str, job_update: BatchJobUpdate) -> Optional[BatchJob]:
        """Update job"""
        update_data = {k: v for k, v in job_update.dict().items() if v is not None}
        if not update_data:
            return await self.get_job_by_id(job_id)
        
        update_data["updated_at"] = datetime.utcnow()
        
        result = await self.collection.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": update_data}
        )
        
        if result.modified_count:
            return await self.get_job_by_id(job_id)
        return None

    async def update_job_status(self, job_id: str, status: str, 
                               result: Optional[Dict[str, Any]] = None,
                               error_message: Optional[str] = None) -> Optional[BatchJob]:
        """Update job status and result"""
        update_data = {
            "status": status,
            "updated_at": datetime.utcnow()
        }
        
        if status == "running":
            update_data["started_at"] = datetime.utcnow()
        elif status in ["completed", "failed"]:
            update_data["completed_at"] = datetime.utcnow()
            
        if result is not None:
            update_data["result"] = result
            
        if error_message is not None:
            update_data["error_message"] = error_message
        
        result = await self.collection.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": update_data}
        )
        
        if result.modified_count:
            return await self.get_job_by_id(job_id)
        return None

    async def delete_job(self, job_id: str) -> bool:
        """Delete job"""
        result = await self.collection.delete_one({"_id": ObjectId(job_id)})
        return result.deleted_count > 0

    async def get_jobs(self, skip: int = 0, limit: int = 100, 
                      status: Optional[str] = None,
                      created_by: Optional[str] = None) -> List[BatchJob]:
        """Get list of jobs with optional filtering"""
        query = {}
        if status:
            query["status"] = status
        if created_by:
            query["created_by"] = created_by
            
        cursor = self.collection.find(query).sort("created_at", -1).skip(skip).limit(limit)
        jobs = []
        async for job_data in cursor:
            jobs.append(BatchJob(**job_data))
        return jobs

    async def get_pending_jobs(self) -> List[BatchJob]:
        """Get all pending jobs for scheduler"""
        cursor = self.collection.find({
            "status": "pending",
            "$or": [
                {"scheduled_at": {"$lte": datetime.utcnow()}},
                {"scheduled_at": None}
            ]
        })
        jobs = []
        async for job_data in cursor:
            jobs.append(BatchJob(**job_data))
        return jobs