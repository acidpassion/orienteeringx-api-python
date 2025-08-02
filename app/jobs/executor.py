import asyncio
import logging
from typing import Dict, Any, Callable
from datetime import datetime
from app.services.batch_job_service import BatchJobService
from app.db.mongodb import get_database

logger = logging.getLogger(__name__)


class JobExecutor:
    def __init__(self):
        self.job_handlers: Dict[str, Callable] = {}
        self.register_default_handlers()

    def register_handler(self, job_type: str, handler: Callable):
        """Register a job handler for a specific job type"""
        self.job_handlers[job_type] = handler
        logger.info(f"Registered handler for job type: {job_type}")

    def register_default_handlers(self):
        """Register default job handlers"""
        self.register_handler("sample_job", self.sample_job_handler)
        self.register_handler("data_processing", self.data_processing_handler)
        self.register_handler("email_notification", self.email_notification_handler)
        self.register_handler("report_generation", self.report_generation_handler)

    async def execute_job(self, job_id: str):
        """Execute a specific job"""
        try:
            database = await get_database()
            job_service = BatchJobService(database)
            
            # Get job details
            job = await job_service.get_job_by_id(job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                return
            
            # Update job status to running
            await job_service.update_job_status(job_id, "running")
            logger.info(f"Starting execution of job {job_id} ({job.job_type})")
            
            # Get handler for job type
            handler = self.job_handlers.get(job.job_type)
            if not handler:
                error_msg = f"No handler found for job type: {job.job_type}"
                logger.error(error_msg)
                await job_service.update_job_status(job_id, "failed", error_message=error_msg)
                return
            
            # Execute the job
            result = await handler(job.parameters)
            
            # Update job status to completed
            await job_service.update_job_status(job_id, "completed", result=result)
            logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Error executing job {job_id}: {str(e)}")
            database = await get_database()
            job_service = BatchJobService(database)
            await job_service.update_job_status(job_id, "failed", error_message=str(e))

    # Default job handlers
    async def sample_job_handler(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Sample job handler for demonstration"""
        await asyncio.sleep(2)  # Simulate work
        return {
            "message": "Sample job completed",
            "processed_items": parameters.get("items", 0),
            "timestamp": datetime.utcnow().isoformat()
        }

    async def data_processing_handler(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Data processing job handler"""
        data_source = parameters.get("data_source", "default")
        batch_size = parameters.get("batch_size", 100)
        
        # Simulate data processing
        await asyncio.sleep(5)
        
        return {
            "message": "Data processing completed",
            "data_source": data_source,
            "processed_records": batch_size * 10,
            "timestamp": datetime.utcnow().isoformat()
        }

    async def email_notification_handler(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Email notification job handler"""
        recipients = parameters.get("recipients", [])
        subject = parameters.get("subject", "Notification")
        
        # Simulate email sending
        await asyncio.sleep(1)
        
        return {
            "message": "Email notifications sent",
            "recipients_count": len(recipients),
            "subject": subject,
            "timestamp": datetime.utcnow().isoformat()
        }

    async def report_generation_handler(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Report generation job handler"""
        report_type = parameters.get("report_type", "summary")
        date_range = parameters.get("date_range", "last_30_days")
        
        # Simulate report generation
        await asyncio.sleep(10)
        
        return {
            "message": "Report generated successfully",
            "report_type": report_type,
            "date_range": date_range,
            "file_size": "2.5MB",
            "timestamp": datetime.utcnow().isoformat()
        }


# Global job executor instance
job_executor = JobExecutor()