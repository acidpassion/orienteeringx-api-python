"""
Data Processing Pipeline DAG for OrienteeringX
This DAG handles data processing, ETL, and analytics jobs
"""
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Add project root to path
sys.path.append('/Users/alex/dev/orienteeringx-api-python')

# Default arguments
default_args = {
    'owner': 'orienteeringx-data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

# Create the DAG - runs daily at 2 AM
dag = DAG(
    'orienteeringx_data_processing',
    default_args=default_args,
    description='Daily data processing and analytics pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['orienteeringx', 'data', 'etl', 'analytics'],
)

# Task 1: Extract data from various sources
def extract_data(**context):
    """Extract data from multiple sources"""
    import logging
    import random
    import pandas as pd
    
    logger = logging.getLogger(__name__)
    
    # Simulate data extraction from different sources
    sources = ['user_events', 'job_logs', 'system_metrics', 'api_calls']
    extracted_data = {}
    
    for source in sources:
        # Simulate data extraction
        record_count = random.randint(1000, 10000)
        extracted_data[source] = {
            'records': record_count,
            'size_mb': round(record_count * 0.001, 2),
            'extraction_time': round(random.uniform(5, 30), 2),
            'status': 'success'
        }
        
        logger.info(f"Extracted {record_count} records from {source}")
    
    total_records = sum(data['records'] for data in extracted_data.values())
    total_size = sum(data['size_mb'] for data in extracted_data.values())
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'sources': extracted_data,
        'total_records': total_records,
        'total_size_mb': round(total_size, 2),
        'extraction_status': 'completed'
    }
    
    logger.info(f"Data extraction completed: {summary}")
    return summary

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Task 2: Transform and clean data
def transform_data(**context):
    """Transform and clean extracted data"""
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # Get extraction results
    ti = context['ti']
    extraction_data = ti.xcom_pull(task_ids='extract_data')
    
    # Simulate data transformation
    transformation_stats = {
        'timestamp': datetime.now().isoformat(),
        'input_records': extraction_data['total_records'],
        'cleaned_records': int(extraction_data['total_records'] * 0.95),  # 5% data quality issues
        'duplicates_removed': random.randint(50, 500),
        'null_values_handled': random.randint(100, 1000),
        'data_types_converted': random.randint(10, 50),
        'validation_errors': random.randint(0, 20),
        'transformation_time_minutes': round(random.uniform(10, 45), 2),
    }
    
    # Calculate data quality score
    quality_score = (transformation_stats['cleaned_records'] / transformation_stats['input_records']) * 100
    transformation_stats['data_quality_score'] = round(quality_score, 2)
    
    logger.info(f"Data transformation completed: {transformation_stats}")
    
    if quality_score < 90:
        logger.warning(f"Data quality score is low: {quality_score}%")
    
    return transformation_stats

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 3: Load data to data warehouse
def load_data(**context):
    """Load transformed data to data warehouse"""
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # Get transformation results
    ti = context['ti']
    transform_data = ti.xcom_pull(task_ids='transform_data')
    
    # Simulate data loading
    load_stats = {
        'timestamp': datetime.now().isoformat(),
        'records_to_load': transform_data['cleaned_records'],
        'records_loaded': transform_data['cleaned_records'] - random.randint(0, 10),
        'load_time_minutes': round(random.uniform(15, 60), 2),
        'target_tables': ['fact_user_activity', 'dim_jobs', 'fact_system_metrics'],
        'indexes_rebuilt': random.randint(3, 8),
        'load_status': 'success'
    }
    
    # Calculate load success rate
    success_rate = (load_stats['records_loaded'] / load_stats['records_to_load']) * 100
    load_stats['load_success_rate'] = round(success_rate, 2)
    
    logger.info(f"Data loading completed: {load_stats}")
    
    if success_rate < 99:
        logger.warning(f"Load success rate is below threshold: {success_rate}%")
    
    return load_stats

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Task 4: Generate analytics reports
def generate_analytics(**context):
    """Generate analytics and insights from processed data"""
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # Simulate analytics generation
    analytics = {
        'timestamp': datetime.now().isoformat(),
        'reports_generated': [
            'daily_user_engagement',
            'job_performance_metrics',
            'system_utilization_trends',
            'error_analysis',
            'capacity_planning'
        ],
        'insights': {
            'top_performing_job_type': random.choice(['data_processing', 'email_notification', 'report_generation']),
            'peak_usage_hour': random.randint(9, 17),
            'average_job_duration_minutes': round(random.uniform(5, 30), 2),
            'user_growth_rate_percent': round(random.uniform(2, 15), 2),
            'system_efficiency_score': round(random.uniform(85, 98), 2),
        },
        'anomalies_detected': random.randint(0, 5),
        'recommendations': [
            'Consider scaling during peak hours',
            'Optimize slow-running job types',
            'Review error patterns for improvement'
        ]
    }
    
    logger.info(f"Analytics generation completed: {analytics}")
    return analytics

analytics_task = PythonOperator(
    task_id='generate_analytics',
    python_callable=generate_analytics,
    dag=dag,
)

# Task 5: Update dashboards and send notifications
def update_dashboards(**context):
    """Update dashboards and send notifications"""
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get results from previous tasks
    ti = context['ti']
    load_data = ti.xcom_pull(task_ids='load_data')
    analytics_data = ti.xcom_pull(task_ids='generate_analytics')
    
    # Simulate dashboard updates
    dashboard_update = {
        'timestamp': datetime.now().isoformat(),
        'dashboards_updated': [
            'executive_summary',
            'operational_metrics',
            'user_analytics',
            'system_health',
            'job_performance'
        ],
        'cache_refreshed': True,
        'alerts_sent': analytics_data['anomalies_detected'],
        'notification_recipients': ['admin@orienteeringx.com', 'data-team@orienteeringx.com'],
        'update_status': 'success'
    }
    
    logger.info(f"Dashboard update completed: {dashboard_update}")
    
    # Send summary notification
    summary = {
        'pipeline_status': 'completed',
        'records_processed': load_data['records_loaded'],
        'data_quality_score': 95.5,  # Example score
        'processing_time_minutes': 120,  # Example time
        'anomalies_detected': analytics_data['anomalies_detected']
    }
    
    logger.info(f"Data processing pipeline summary: {summary}")
    return dashboard_update

dashboard_task = PythonOperator(
    task_id='update_dashboards',
    python_callable=update_dashboards,
    dag=dag,
)

# Task 6: Data quality validation
def validate_data_quality(**context):
    """Validate data quality and generate quality report"""
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # Simulate data quality checks
    quality_checks = {
        'timestamp': datetime.now().isoformat(),
        'checks_performed': [
            'completeness_check',
            'accuracy_check',
            'consistency_check',
            'timeliness_check',
            'validity_check'
        ],
        'results': {
            'completeness_score': round(random.uniform(95, 100), 2),
            'accuracy_score': round(random.uniform(92, 99), 2),
            'consistency_score': round(random.uniform(90, 98), 2),
            'timeliness_score': round(random.uniform(88, 100), 2),
            'validity_score': round(random.uniform(94, 100), 2),
        },
        'issues_found': random.randint(0, 10),
        'critical_issues': random.randint(0, 2),
    }
    
    # Calculate overall quality score
    scores = quality_checks['results']
    overall_score = sum(scores.values()) / len(scores)
    quality_checks['overall_quality_score'] = round(overall_score, 2)
    
    logger.info(f"Data quality validation completed: {quality_checks}")
    
    if overall_score < 95:
        logger.warning(f"Data quality score is below threshold: {overall_score}")
    
    if quality_checks['critical_issues'] > 0:
        logger.error(f"Critical data quality issues found: {quality_checks['critical_issues']}")
    
    return quality_checks

quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# Set up task dependencies
extract_task >> transform_task >> load_task
load_task >> [analytics_task, quality_task]
[analytics_task, quality_task] >> dashboard_task