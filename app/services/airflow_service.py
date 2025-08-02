"""
Airflow integration service for OrienteeringX API
"""
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

# Add the project root to Python path
sys.path.append('/Users/alex/dev/orienteeringx-api-python')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from app.services.batch_job_service import BatchJobService
from app.services.user_service import UserService
from app.db.mongodb import get_database
from app.core.config import settings

logger = logging.getLogger(__name__)


class AirflowIntegrationService:
    """Service to integrate Airflow with OrienteeringX batch job system"""
    
    def __init__(self):
        self.batch_job_service = BatchJobService()
        self.user_service = UserService()
    
    async def sync_job_to_airflow(self, job_id: str) -> bool:
        """
        Sync a batch job from MongoDB to Airflow DAG
        """
        try:
            # Get job from database
            job = await self.batch_job_service.get_job(job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                return False
            
            # Create DAG dynamically
            dag_id = f"orienteeringx_job_{job_id}"
            
            # Define default arguments
            default_args = {
                'owner': job.created_by,
                'depends_on_past': False,
                'start_date': job.created_at,
                'email_on_failure': True,
                'email_on_retry': False,
                'retries': job.parameters.get('retries', 1),
                'retry_delay': timedelta(minutes=5),
            }
            
            # Create DAG
            dag = DAG(
                dag_id,
                default_args=default_args,
                description=f'OrienteeringX Job: {job.name}',
                schedule_interval=job.schedule if job.schedule else None,
                catchup=False,
                tags=['orienteeringx', job.job_type],
            )
            
            # Create task based on job type
            task = self._create_task_for_job(job, dag)
            
            # Store DAG reference
            globals()[dag_id] = dag
            
            logger.info(f"Successfully synced job {job_id} to Airflow DAG {dag_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error syncing job {job_id} to Airflow: {str(e)}")
            return False
    
    def _create_task_for_job(self, job, dag):
        """Create appropriate Airflow task based on job type"""
        
        if job.job_type == "sample_job":
            return PythonOperator(
                task_id=f"execute_{job.name}",
                python_callable=self._execute_sample_job,
                op_kwargs={'job_id': str(job.id), 'parameters': job.parameters},
                dag=dag,
            )
        
        elif job.job_type == "data_processing":
            return PythonOperator(
                task_id=f"execute_{job.name}",
                python_callable=self._execute_data_processing,
                op_kwargs={'job_id': str(job.id), 'parameters': job.parameters},
                dag=dag,
            )
        
        elif job.job_type == "email_notification":
            return PythonOperator(
                task_id=f"execute_{job.name}",
                python_callable=self._execute_email_notification,
                op_kwargs={'job_id': str(job.id), 'parameters': job.parameters},
                dag=dag,
            )
        
        elif job.job_type == "report_generation":
            return PythonOperator(
                task_id=f"execute_{job.name}",
                python_callable=self._execute_report_generation,
                op_kwargs={'job_id': str(job.id), 'parameters': job.parameters},
                dag=dag,
            )
        
        else:
            # Default bash operator for custom scripts
            return BashOperator(
                task_id=f"execute_{job.name}",
                bash_command=f"echo 'Executing job {job.name} of type {job.job_type}'",
                dag=dag,
            )
    
    def _execute_sample_job(self, job_id: str, parameters: Dict[str, Any]):
        """Execute sample job"""
        import time
        import random
        
        logger.info(f"Starting sample job {job_id} with parameters: {parameters}")
        
        # Simulate work
        duration = parameters.get('duration', 10)
        time.sleep(duration)
        
        # Simulate random success/failure
        if random.random() > 0.1:  # 90% success rate
            result = {
                'status': 'completed',
                'message': f'Sample job completed successfully after {duration} seconds',
                'processed_items': random.randint(100, 1000)
            }
            logger.info(f"Sample job {job_id} completed successfully")
        else:
            raise Exception(f"Sample job {job_id} failed randomly")
        
        return result
    
    def _execute_data_processing(self, job_id: str, parameters: Dict[str, Any]):
        """Execute data processing job"""
        import pandas as pd
        import numpy as np
        
        logger.info(f"Starting data processing job {job_id}")
        
        # Simulate data processing
        data_size = parameters.get('data_size', 1000)
        data = pd.DataFrame({
            'id': range(data_size),
            'value': np.random.randn(data_size),
            'category': np.random.choice(['A', 'B', 'C'], data_size)
        })
        
        # Process data
        processed_data = data.groupby('category').agg({
            'value': ['mean', 'std', 'count']
        }).round(2)
        
        result = {
            'status': 'completed',
            'message': f'Processed {data_size} records',
            'summary': processed_data.to_dict()
        }
        
        logger.info(f"Data processing job {job_id} completed")
        return result
    
    def _execute_email_notification(self, job_id: str, parameters: Dict[str, Any]):
        """Execute email notification job"""
        logger.info(f"Starting email notification job {job_id}")
        
        # Simulate email sending
        recipients = parameters.get('recipients', ['admin@orienteeringx.com'])
        subject = parameters.get('subject', 'OrienteeringX Notification')
        message = parameters.get('message', 'This is a test notification')
        
        # In a real implementation, you would send actual emails here
        logger.info(f"Sending email to {recipients} with subject: {subject}")
        
        result = {
            'status': 'completed',
            'message': f'Email sent to {len(recipients)} recipients',
            'recipients': recipients
        }
        
        return result
    
    def _execute_report_generation(self, job_id: str, parameters: Dict[str, Any]):
        """Execute report generation job"""
        import json
        from datetime import datetime
        
        logger.info(f"Starting report generation job {job_id}")
        
        # Simulate report generation
        report_type = parameters.get('report_type', 'summary')
        date_range = parameters.get('date_range', 30)
        
        # Generate mock report data
        report_data = {
            'report_type': report_type,
            'generated_at': datetime.now().isoformat(),
            'date_range_days': date_range,
            'total_users': 1250,
            'active_users': 890,
            'total_jobs': 456,
            'successful_jobs': 423,
            'failed_jobs': 33,
            'success_rate': 92.8
        }
        
        # Save report (in real implementation, save to file or database)
        report_filename = f"report_{report_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        result = {
            'status': 'completed',
            'message': f'Report generated successfully',
            'report_filename': report_filename,
            'report_data': report_data
        }
        
        logger.info(f"Report generation job {job_id} completed")
        return result


# Global instance for use in DAGs
airflow_service = AirflowIntegrationService()