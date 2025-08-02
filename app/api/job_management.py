"""
Web UI for Job Management
Provides a web interface for managing and monitoring batch jobs
"""
from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json
from app.db.mongodb import get_database
from app.models.models import BatchJob
from app.jobs.scheduler import scheduler
from apscheduler.job import Job
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["job-management"])
templates = Jinja2Templates(directory="app/templates")

@router.get("/dashboard", response_class=HTMLResponse)
async def job_dashboard(request: Request):
    """Render the job management dashboard"""
    return templates.TemplateResponse("job_dashboard.html", {"request": request})

@router.get("/api/status")
async def get_scheduler_status():
    """Get scheduler status and statistics"""
    try:
        jobs = scheduler.get_jobs()
        
        # Get job statistics
        total_jobs = len(jobs)
        running_jobs = len([job for job in jobs if job.next_run_time])
        paused_jobs = len([job for job in jobs if not job.next_run_time])
        
        # Get recent job executions (simulated for demo)
        recent_executions = []
        for i, job in enumerate(jobs[:5]):  # Show last 5 jobs
            recent_executions.append({
                "job_id": job.id,
                "job_name": job.name or job.id,
                "status": "completed" if i % 3 == 0 else "running" if i % 3 == 1 else "failed",
                "start_time": (datetime.now() - timedelta(minutes=i*10)).isoformat(),
                "duration": f"{(i+1)*2} minutes",
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None
            })
        
        return {
            "scheduler_running": scheduler.running,
            "total_jobs": total_jobs,
            "running_jobs": running_jobs,
            "paused_jobs": paused_jobs,
            "recent_executions": recent_executions,
            "uptime": "2 hours 15 minutes",  # Simulated
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting scheduler status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/jobs")
async def list_jobs():
    """List all scheduled jobs"""
    try:
        jobs = scheduler.get_jobs()
        job_list = []
        
        for job in jobs:
            job_info = {
                "id": job.id,
                "name": job.name or job.id,
                "func": f"{job.func.__module__}.{job.func.__name__}" if hasattr(job.func, '__name__') else str(job.func),
                "trigger": str(job.trigger),
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                "args": list(job.args) if job.args else [],
                "kwargs": dict(job.kwargs) if job.kwargs else {},
                "misfire_grace_time": job.misfire_grace_time,
                "max_instances": job.max_instances,
                "coalesce": job.coalesce
            }
            job_list.append(job_info)
        
        return {"jobs": job_list}
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/jobs/{job_id}/run")
async def run_job_now(job_id: str):
    """Trigger a job to run immediately"""
    try:
        job = scheduler.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Run the job immediately
        job.modify(next_run_time=datetime.now())
        
        return {
            "message": f"Job {job_id} triggered successfully",
            "job_id": job_id,
            "triggered_at": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error running job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/jobs/{job_id}/pause")
async def pause_job(job_id: str):
    """Pause a scheduled job"""
    try:
        job = scheduler.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        scheduler.pause_job(job_id)
        
        return {
            "message": f"Job {job_id} paused successfully",
            "job_id": job_id,
            "paused_at": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error pausing job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/jobs/{job_id}/resume")
async def resume_job(job_id: str):
    """Resume a paused job"""
    try:
        job = scheduler.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        scheduler.resume_job(job_id)
        
        return {
            "message": f"Job {job_id} resumed successfully",
            "job_id": job_id,
            "resumed_at": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error resuming job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a scheduled job"""
    try:
        job = scheduler.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        scheduler.remove_job(job_id)
        
        return {
            "message": f"Job {job_id} deleted successfully",
            "job_id": job_id,
            "deleted_at": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error deleting job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/jobs")
async def create_job(job_data: Dict[str, Any]):
    """Create a new scheduled job"""
    try:
        job_id = job_data.get("id")
        job_name = job_data.get("name")
        job_type = job_data.get("type", "sample")
        schedule = job_data.get("schedule", "interval")
        interval_minutes = job_data.get("interval_minutes", 60)
        
        if not job_id:
            raise HTTPException(status_code=400, detail="Job ID is required")
        
        # Import the job function based on type
        if job_type == "sample":
            from app.jobs.sample_jobs import sample_job
            job_func = sample_job
        elif job_type == "data_processing":
            from app.jobs.sample_jobs import data_processing_job
            job_func = data_processing_job
        elif job_type == "email_notification":
            from app.jobs.sample_jobs import email_notification_job
            job_func = email_notification_job
        elif job_type == "report_generation":
            from app.jobs.sample_jobs import report_generation_job
            job_func = report_generation_job
        else:
            raise HTTPException(status_code=400, detail=f"Unknown job type: {job_type}")
        
        # Schedule the job
        if schedule == "interval":
            scheduler.add_job(
                func=job_func,
                trigger="interval",
                minutes=interval_minutes,
                id=job_id,
                name=job_name,
                replace_existing=True
            )
        elif schedule == "cron":
            cron_expr = job_data.get("cron_expression", "0 * * * *")  # Default: every hour
            # Parse cron expression (simplified)
            parts = cron_expr.split()
            if len(parts) == 5:
                minute, hour, day, month, day_of_week = parts
                scheduler.add_job(
                    func=job_func,
                    trigger="cron",
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    id=job_id,
                    name=job_name,
                    replace_existing=True
                )
            else:
                raise HTTPException(status_code=400, detail="Invalid cron expression")
        
        return {
            "message": f"Job {job_id} created successfully",
            "job_id": job_id,
            "job_name": job_name,
            "job_type": job_type,
            "schedule": schedule,
            "created_at": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error creating job: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/jobs/{job_id}")
async def get_job_details(job_id: str):
    """Get detailed information about a specific job"""
    try:
        job = scheduler.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Get job execution history (simulated)
        execution_history = []
        for i in range(10):  # Show last 10 executions
            execution_history.append({
                "execution_id": f"exec_{i+1}",
                "start_time": (datetime.now() - timedelta(hours=i*2)).isoformat(),
                "end_time": (datetime.now() - timedelta(hours=i*2) + timedelta(minutes=5)).isoformat(),
                "status": "completed" if i % 4 != 3 else "failed",
                "duration_seconds": 300 + (i * 30),
                "output": f"Job execution {i+1} completed successfully" if i % 4 != 3 else f"Job execution {i+1} failed with error",
                "error_message": None if i % 4 != 3 else "Sample error message for demonstration"
            })
        
        job_details = {
            "id": job.id,
            "name": job.name or job.id,
            "func": f"{job.func.__module__}.{job.func.__name__}" if hasattr(job.func, '__name__') else str(job.func),
            "trigger": str(job.trigger),
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "args": list(job.args) if job.args else [],
            "kwargs": dict(job.kwargs) if job.kwargs else {},
            "misfire_grace_time": job.misfire_grace_time,
            "max_instances": job.max_instances,
            "coalesce": job.coalesce,
            "execution_history": execution_history,
            "statistics": {
                "total_executions": len(execution_history),
                "successful_executions": len([e for e in execution_history if e["status"] == "completed"]),
                "failed_executions": len([e for e in execution_history if e["status"] == "failed"]),
                "average_duration_seconds": sum([e["duration_seconds"] for e in execution_history]) / len(execution_history) if execution_history else 0,
                "last_execution": execution_history[0] if execution_history else None
            }
        }
        
        return job_details
    except Exception as e:
        logger.error(f"Error getting job details for {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/logs/{job_id}")
async def get_job_logs(job_id: str, limit: int = 100):
    """Get logs for a specific job"""
    try:
        # In a real implementation, you would read from actual log files
        # For demo purposes, we'll return simulated logs
        logs = []
        for i in range(min(limit, 50)):
            log_level = "INFO" if i % 5 != 4 else "ERROR"
            timestamp = (datetime.now() - timedelta(minutes=i*5)).isoformat()
            message = f"Job {job_id} execution step {i+1}" if log_level == "INFO" else f"Error in job {job_id} at step {i+1}"
            
            logs.append({
                "timestamp": timestamp,
                "level": log_level,
                "message": message,
                "job_id": job_id
            })
        
        return {
            "job_id": job_id,
            "total_logs": len(logs),
            "logs": logs
        }
    except Exception as e:
        logger.error(f"Error getting logs for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/scrape/run-now")
async def run_scrape_job_now():
    """Run the orienteering data scrape job immediately (ad-hoc)"""
    try:
        success = await scheduler.run_scrape_job_now()
        
        if success:
            return {
                "message": "Orienteering data scrape job triggered successfully",
                "triggered_at": datetime.now().isoformat(),
                "status": "scheduled"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to schedule scrape job")
            
    except Exception as e:
        logger.error(f"Error triggering scrape job: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/scrape/debug-run")
async def debug_run_scrape_job():
    """Run the orienteering data scrape job synchronously for debugging"""
    try:
        from app.jobs.sample_jobs import scrape_orienteering_data_job
        
        # Run the job directly in the main process (debugger-friendly)
        result = await scrape_orienteering_data_job()
        
        return {
            "message": "Scrape job executed synchronously",
            "executed_at": datetime.now().isoformat(),
            "result": result
        }
            
    except Exception as e:
        logger.error(f"Error executing scrape job synchronously: {e}")
        raise HTTPException(status_code=500, detail=str(e))