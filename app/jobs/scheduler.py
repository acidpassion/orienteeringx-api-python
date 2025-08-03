import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from datetime import datetime, timedelta
from app.core.config import settings
from app.jobs.executor import job_executor
from app.services.batch_job_service import BatchJobService
from app.db.mongodb import get_database

logger = logging.getLogger(__name__)


class JobScheduler:
    def __init__(self):
        # Configure job stores and executors
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': AsyncIOExecutor()
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 3
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=settings.scheduler_timezone
        )
        
        self.is_running = False

    async def start(self):
        """Start the scheduler"""
        if not self.is_running:
            self.scheduler.start()
            self.is_running = True
            
            # Schedule periodic job to check for pending jobs
            self.scheduler.add_job(
                self.check_pending_jobs,
                'interval',
                seconds=30,
                id='check_pending_jobs',
                replace_existing=True
            )
            
            # Add default cyclic jobs
            await self.add_default_jobs()
            
            logger.info("Job scheduler started")

    async def add_default_jobs(self):
        """Add default cyclic jobs to the scheduler"""
        try:
            from app.jobs.sample_jobs import (
                sample_job, data_processing_job, email_notification_job,
                report_generation_job, system_health_check_job, database_cleanup_job,
                scrape_orienteering_data_job
            )
            
            # Sample job - every 5 minutes
            self.scheduler.add_job(
                sample_job,
                'interval',
                minutes=5,
                id='sample_job_cyclic',
                name='Sample Job (Every 5 minutes)',
                replace_existing=True
            )
            
            # System health check - every 10 minutes
            self.scheduler.add_job(
                system_health_check_job,
                'interval',
                minutes=10,
                id='system_health_check',
                name='System Health Check',
                replace_existing=True
            )
            
            # Data processing - every 30 minutes
            self.scheduler.add_job(
                data_processing_job,
                'interval',
                minutes=30,
                id='data_processing_cyclic',
                name='Data Processing Job',
                replace_existing=True
            )
            
            # Email notifications - every 15 minutes
            self.scheduler.add_job(
                email_notification_job,
                'interval',
                minutes=15,
                id='email_notification_cyclic',
                name='Email Notification Job',
                replace_existing=True
            )
            
            # Report generation - every 2 hours
            self.scheduler.add_job(
                report_generation_job,
                'interval',
                hours=2,
                id='report_generation_cyclic',
                name='Report Generation Job',
                replace_existing=True
            )
            
            # Database cleanup - every 6 hours
            self.scheduler.add_job(
                database_cleanup_job,
                'interval',
                hours=6,
                id='database_cleanup_cyclic',
                name='Database Cleanup Job',
                replace_existing=True
            )
            
            # Orienteering data scraping - every 1 minute
            self.scheduler.add_job(
                scrape_orienteering_data_job,
                'interval',
                minutes=60,
                id='scrape_orienteering_data',
                name='Orienteering Data Scraper (Every 1 minute)',
                replace_existing=True
            )
            
            logger.info("Default cyclic jobs added to scheduler")
            
        except Exception as e:
            logger.error(f"Error adding default jobs: {str(e)}")

    async def stop(self):
        """Stop the scheduler"""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("Job scheduler stopped")

    async def schedule_job(self, job_id: str, run_time: datetime = None):
        """Schedule a job for execution"""
        if run_time is None:
            run_time = datetime.now() + timedelta(seconds=1)
        
        self.scheduler.add_job(
            job_executor.execute_job,
            'date',
            run_date=run_time,
            args=[job_id],
            id=f"job_{job_id}",
            replace_existing=True
        )
        
        logger.info(f"Scheduled job {job_id} for {run_time}")

    async def cancel_job(self, job_id: str):
        """Cancel a scheduled job"""
        try:
            self.scheduler.remove_job(f"job_{job_id}")
            logger.info(f"Cancelled job {job_id}")
        except Exception as e:
            logger.warning(f"Could not cancel job {job_id}: {str(e)}")

    async def check_pending_jobs(self):
        """Check for pending jobs and schedule them"""
        try:
            database = await get_database()
            job_service = BatchJobService(database)
            
            pending_jobs = await job_service.get_pending_jobs()
            
            for job in pending_jobs:
                # Schedule the job
                run_time = job.scheduled_at if job.scheduled_at else datetime.now()
                await self.schedule_job(str(job.id), run_time)
                
                logger.info(f"Found and scheduled pending job: {job.id}")
                
        except Exception as e:
            logger.error(f"Error checking pending jobs: {str(e)}")

    def get_job_status(self, job_id: str):
        """Get the status of a scheduled job"""
        try:
            job = self.scheduler.get_job(f"job_{job_id}")
            if job:
                return {
                    "id": job.id,
                    "next_run_time": job.next_run_time,
                    "pending": True
                }
            return {"pending": False}
        except Exception as e:
            logger.error(f"Error getting job status for {job_id}: {str(e)}")
            return {"pending": False, "error": str(e)}

    def list_scheduled_jobs(self):
        """List all scheduled jobs"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "next_run_time": job.next_run_time,
                "func": str(job.func)
            })
        return jobs

    def get_jobs(self):
        """Get all scheduled jobs (for web UI)"""
        return self.scheduler.get_jobs()

    def get_job(self, job_id: str):
        """Get a specific job by ID"""
        return self.scheduler.get_job(job_id)

    def add_job(self, func, trigger, **kwargs):
        """Add a new job to the scheduler"""
        return self.scheduler.add_job(func, trigger, **kwargs)

    def remove_job(self, job_id: str):
        """Remove a job from the scheduler"""
        return self.scheduler.remove_job(job_id)

    def pause_job(self, job_id: str):
        """Pause a job"""
        return self.scheduler.pause_job(job_id)

    def resume_job(self, job_id: str):
        """Resume a paused job"""
        return self.scheduler.resume_job(job_id)

    async def run_scrape_job_now(self):
        """Run the orienteering data scrape job immediately (ad-hoc)"""
        try:
            from app.jobs.sample_jobs import scrape_orienteering_data_job
            
            # Add a one-time job to run immediately
            self.scheduler.add_job(
                scrape_orienteering_data_job,
                'date',
                run_date=datetime.now() + timedelta(seconds=1),
                id=f'scrape_orienteering_data_adhoc_{int(datetime.now().timestamp())}',
                name='Orienteering Data Scraper (Ad-hoc)',
                replace_existing=False
            )
            
            logger.info("Orienteering data scrape job scheduled to run immediately")
            return True
        except Exception as e:
            logger.error(f"Error scheduling ad-hoc scrape job: {str(e)}")
            return False

    @property
    def running(self):
        """Check if scheduler is running"""
        return self.is_running


# Global scheduler instance
scheduler = JobScheduler()