"""
Sample Cyclic Jobs DAG for OrienteeringX
This DAG demonstrates various types of recurring jobs
"""
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Add project root to path
sys.path.append('/Users/alex/dev/orienteeringx-api-python')

from app.services.airflow_service import airflow_service

# Default arguments for all tasks
default_args = {
    'owner': 'orienteeringx',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'orienteeringx_cyclic_jobs',
    default_args=default_args,
    description='Sample cyclic jobs for OrienteeringX platform',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    tags=['orienteeringx', 'cyclic', 'sample'],
)

# Task 1: Hourly system health check
def system_health_check(**context):
    """Check system health and log metrics"""
    import psutil
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get system metrics
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    health_data = {
        'timestamp': datetime.now().isoformat(),
        'cpu_usage_percent': cpu_percent,
        'memory_usage_percent': memory.percent,
        'memory_available_gb': round(memory.available / (1024**3), 2),
        'disk_usage_percent': disk.percent,
        'disk_free_gb': round(disk.free / (1024**3), 2),
    }
    
    logger.info(f"System Health Check: {health_data}")
    
    # Alert if any metric is concerning
    alerts = []
    if cpu_percent > 80:
        alerts.append(f"High CPU usage: {cpu_percent}%")
    if memory.percent > 85:
        alerts.append(f"High memory usage: {memory.percent}%")
    if disk.percent > 90:
        alerts.append(f"High disk usage: {disk.percent}%")
    
    if alerts:
        logger.warning(f"System alerts: {alerts}")
    
    return health_data

health_check_task = PythonOperator(
    task_id='system_health_check',
    python_callable=system_health_check,
    dag=dag,
)

# Task 2: Database cleanup (runs every 6 hours)
def database_cleanup(**context):
    """Clean up old logs and temporary data"""
    import logging
    from datetime import datetime, timedelta
    
    logger = logging.getLogger(__name__)
    
    # Simulate database cleanup
    cutoff_date = datetime.now() - timedelta(days=30)
    
    cleanup_stats = {
        'timestamp': datetime.now().isoformat(),
        'cutoff_date': cutoff_date.isoformat(),
        'old_logs_deleted': 245,
        'temp_files_deleted': 67,
        'space_freed_mb': 1250,
    }
    
    logger.info(f"Database cleanup completed: {cleanup_stats}")
    return cleanup_stats

cleanup_task = PythonOperator(
    task_id='database_cleanup',
    python_callable=database_cleanup,
    dag=dag,
)

# Task 3: User activity summary (runs daily)
def user_activity_summary(**context):
    """Generate daily user activity summary"""
    import logging
    import random
    from datetime import datetime, timedelta
    
    logger = logging.getLogger(__name__)
    
    # Simulate user activity analysis
    yesterday = datetime.now() - timedelta(days=1)
    
    activity_summary = {
        'date': yesterday.date().isoformat(),
        'total_active_users': random.randint(50, 200),
        'new_registrations': random.randint(5, 25),
        'jobs_created': random.randint(20, 100),
        'jobs_completed': random.randint(18, 95),
        'average_session_duration_minutes': round(random.uniform(15, 45), 2),
        'peak_usage_hour': random.randint(9, 17),
    }
    
    logger.info(f"User activity summary for {yesterday.date()}: {activity_summary}")
    return activity_summary

activity_summary_task = PythonOperator(
    task_id='user_activity_summary',
    python_callable=user_activity_summary,
    dag=dag,
)

# Task 4: Backup verification
def backup_verification(**context):
    """Verify that backups are working correctly"""
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # Simulate backup verification
    backup_status = {
        'timestamp': datetime.now().isoformat(),
        'database_backup_status': 'success' if random.random() > 0.05 else 'failed',
        'file_backup_status': 'success' if random.random() > 0.03 else 'failed',
        'backup_size_gb': round(random.uniform(5, 50), 2),
        'backup_duration_minutes': round(random.uniform(10, 30), 2),
    }
    
    logger.info(f"Backup verification: {backup_status}")
    
    # Raise alert if backup failed
    if backup_status['database_backup_status'] == 'failed':
        raise Exception("Database backup verification failed!")
    if backup_status['file_backup_status'] == 'failed':
        raise Exception("File backup verification failed!")
    
    return backup_status

backup_task = PythonOperator(
    task_id='backup_verification',
    python_callable=backup_verification,
    dag=dag,
)

# Task 5: Performance metrics collection
def collect_performance_metrics(**context):
    """Collect and analyze performance metrics"""
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # Simulate performance metrics collection
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'api_response_time_ms': round(random.uniform(50, 500), 2),
        'database_query_time_ms': round(random.uniform(10, 100), 2),
        'active_connections': random.randint(10, 100),
        'requests_per_minute': random.randint(50, 500),
        'error_rate_percent': round(random.uniform(0, 5), 2),
        'cache_hit_rate_percent': round(random.uniform(80, 98), 2),
    }
    
    logger.info(f"Performance metrics: {metrics}")
    
    # Alert on poor performance
    if metrics['api_response_time_ms'] > 1000:
        logger.warning(f"High API response time: {metrics['api_response_time_ms']}ms")
    if metrics['error_rate_percent'] > 2:
        logger.warning(f"High error rate: {metrics['error_rate_percent']}%")
    
    return metrics

performance_task = PythonOperator(
    task_id='collect_performance_metrics',
    python_callable=collect_performance_metrics,
    dag=dag,
)

# Task 6: Send daily report email
def send_daily_report(**context):
    """Send daily summary report"""
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get results from previous tasks
    ti = context['ti']
    health_data = ti.xcom_pull(task_ids='system_health_check')
    activity_data = ti.xcom_pull(task_ids='user_activity_summary')
    performance_data = ti.xcom_pull(task_ids='collect_performance_metrics')
    
    # Create report summary
    report = {
        'date': datetime.now().date().isoformat(),
        'system_health': health_data,
        'user_activity': activity_data,
        'performance': performance_data,
        'overall_status': 'healthy',
    }
    
    logger.info(f"Daily report generated: {report}")
    
    # In a real implementation, send email here
    logger.info("Daily report email sent to administrators")
    
    return report

report_task = PythonOperator(
    task_id='send_daily_report',
    python_callable=send_daily_report,
    dag=dag,
)

# Set up task dependencies
health_check_task >> cleanup_task
health_check_task >> activity_summary_task
health_check_task >> performance_task
[activity_summary_task, performance_task] >> report_task
backup_task  # Independent task