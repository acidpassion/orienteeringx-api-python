from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional
from datetime import datetime
from app.core.deps import get_current_active_user
from app.schemas.schemas import BatchJobCreate, BatchJobUpdate, BatchJobResponse
from app.services.batch_job_service import BatchJobService
from app.jobs.scheduler import scheduler
from app.db.mongodb import get_database
from app.models.models import User

router = APIRouter()


@router.post("/", response_model=BatchJobResponse)
async def create_batch_job(
    job_data: BatchJobCreate,
    db=Depends(get_database),
    current_user: User = Depends(get_current_active_user)
):
    """Create a new batch job"""
    job_service = BatchJobService(db)
    
    job = await job_service.create_job(job_data, created_by=current_user.username)
    
    # Schedule the job
    run_time = job.scheduled_at if job.scheduled_at else datetime.now()
    await scheduler.schedule_job(str(job.id), run_time)
    
    return BatchJobResponse(
        id=str(job.id),
        name=job.name,
        description=job.description,
        job_type=job.job_type,
        parameters=job.parameters,
        status=job.status,
        scheduled_at=job.scheduled_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        result=job.result,
        error_message=job.error_message,
        created_at=job.created_at,
        created_by=job.created_by
    )


@router.get("/", response_model=List[BatchJobResponse])
async def read_batch_jobs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(None),
    db=Depends(get_database),
    current_user: User = Depends(get_current_active_user)
):
    """Get list of batch jobs"""
    job_service = BatchJobService(db)
    
    # Non-superusers can only see their own jobs
    created_by = None if current_user.is_superuser else current_user.username
    
    jobs = await job_service.get_jobs(
        skip=skip, 
        limit=limit, 
        status=status,
        created_by=created_by
    )
    
    return [
        BatchJobResponse(
            id=str(job.id),
            name=job.name,
            description=job.description,
            job_type=job.job_type,
            parameters=job.parameters,
            status=job.status,
            scheduled_at=job.scheduled_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            result=job.result,
            error_message=job.error_message,
            created_at=job.created_at,
            created_by=job.created_by
        )
        for job in jobs
    ]


@router.get("/{job_id}", response_model=BatchJobResponse)
async def read_batch_job(
    job_id: str,
    db=Depends(get_database),
    current_user: User = Depends(get_current_active_user)
):
    """Get batch job by ID"""
    job_service = BatchJobService(db)
    job = await job_service.get_job_by_id(job_id)
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    # Check permissions
    if not current_user.is_superuser and job.created_by != current_user.username:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    
    return BatchJobResponse(
        id=str(job.id),
        name=job.name,
        description=job.description,
        job_type=job.job_type,
        parameters=job.parameters,
        status=job.status,
        scheduled_at=job.scheduled_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        result=job.result,
        error_message=job.error_message,
        created_at=job.created_at,
        created_by=job.created_by
    )


@router.put("/{job_id}", response_model=BatchJobResponse)
async def update_batch_job(
    job_id: str,
    job_update: BatchJobUpdate,
    db=Depends(get_database),
    current_user: User = Depends(get_current_active_user)
):
    """Update batch job"""
    job_service = BatchJobService(db)
    
    # Check if job exists and user has permission
    existing_job = await job_service.get_job_by_id(job_id)
    if not existing_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if not current_user.is_superuser and existing_job.created_by != current_user.username:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    
    # Can only update pending jobs
    if existing_job.status != "pending":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Can only update pending jobs"
        )
    
    job = await job_service.update_job(job_id, job_update)
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    # Reschedule if needed
    if job_update.scheduled_at is not None:
        await scheduler.cancel_job(job_id)
        await scheduler.schedule_job(job_id, job_update.scheduled_at)
    
    return BatchJobResponse(
        id=str(job.id),
        name=job.name,
        description=job.description,
        job_type=job.job_type,
        parameters=job.parameters,
        status=job.status,
        scheduled_at=job.scheduled_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        result=job.result,
        error_message=job.error_message,
        created_at=job.created_at,
        created_by=job.created_by
    )


@router.delete("/{job_id}")
async def delete_batch_job(
    job_id: str,
    db=Depends(get_database),
    current_user: User = Depends(get_current_active_user)
):
    """Delete batch job"""
    job_service = BatchJobService(db)
    
    # Check if job exists and user has permission
    existing_job = await job_service.get_job_by_id(job_id)
    if not existing_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if not current_user.is_superuser and existing_job.created_by != current_user.username:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    
    # Cancel scheduled job
    await scheduler.cancel_job(job_id)
    
    # Delete from database
    success = await job_service.delete_job(job_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    return {"message": "Job deleted successfully"}


@router.get("/types/available")
async def get_available_job_types(
    current_user: User = Depends(get_current_active_user)
):
    """Get list of available job types"""
    from app.jobs.executor import job_executor
    
    return {
        "job_types": list(job_executor.job_handlers.keys()),
        "descriptions": {
            "sample_job": "A sample job for demonstration purposes",
            "data_processing": "Process data from various sources",
            "email_notification": "Send email notifications",
            "report_generation": "Generate various types of reports"
        }
    }


@router.get("/scheduler/status")
async def get_scheduler_status(
    current_user: User = Depends(get_current_active_user)
):
    """Get scheduler status and scheduled jobs"""
    if not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    
    return {
        "is_running": scheduler.is_running,
        "scheduled_jobs": scheduler.list_scheduled_jobs()
    }