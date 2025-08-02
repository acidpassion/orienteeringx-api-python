"""
Notification and Alerting DAG for OrienteeringX
This DAG handles various notification and alerting tasks
"""
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

# Add project root to path
sys.path.append('/Users/alex/dev/orienteeringx-api-python')

# Default arguments
default_args = {
    'owner': 'orienteeringx-ops',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG - runs every 30 minutes
dag = DAG(
    'orienteeringx_notifications',
    default_args=default_args,
    description='Notification and alerting system for OrienteeringX',
    schedule_interval=timedelta(minutes=30),  # Every 30 minutes
    catchup=False,
    tags=['orienteeringx', 'notifications', 'alerts', 'monitoring'],
)

# Task 1: Check system alerts
def check_system_alerts(**context):
    """Check for system alerts and issues"""
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # Simulate system monitoring
    alerts = []
    
    # Check various system components
    components = {
        'api_server': random.choice(['healthy', 'warning', 'critical']),
        'database': random.choice(['healthy', 'warning']),
        'job_scheduler': random.choice(['healthy', 'warning']),
        'file_storage': random.choice(['healthy', 'warning']),
        'cache_system': random.choice(['healthy', 'warning', 'critical']),
    }
    
    for component, status in components.items():
        if status == 'warning':
            alerts.append({
                'component': component,
                'severity': 'warning',
                'message': f'{component} is experiencing performance issues',
                'timestamp': datetime.now().isoformat()
            })
        elif status == 'critical':
            alerts.append({
                'component': component,
                'severity': 'critical',
                'message': f'{component} is down or critically impaired',
                'timestamp': datetime.now().isoformat()
            })
    
    # Add some random alerts
    if random.random() < 0.3:  # 30% chance of disk space alert
        alerts.append({
            'component': 'disk_space',
            'severity': 'warning',
            'message': f'Disk usage is at {random.randint(85, 95)}%',
            'timestamp': datetime.now().isoformat()
        })
    
    if random.random() < 0.1:  # 10% chance of memory alert
        alerts.append({
            'component': 'memory',
            'severity': 'critical',
            'message': f'Memory usage is at {random.randint(95, 99)}%',
            'timestamp': datetime.now().isoformat()
        })
    
    alert_summary = {
        'timestamp': datetime.now().isoformat(),
        'total_alerts': len(alerts),
        'critical_alerts': len([a for a in alerts if a['severity'] == 'critical']),
        'warning_alerts': len([a for a in alerts if a['severity'] == 'warning']),
        'alerts': alerts,
        'system_status': 'critical' if any(a['severity'] == 'critical' for a in alerts) else 'warning' if alerts else 'healthy'
    }
    
    logger.info(f"System alert check completed: {alert_summary}")
    
    if alert_summary['critical_alerts'] > 0:
        logger.error(f"Critical alerts detected: {alert_summary['critical_alerts']}")
    elif alert_summary['warning_alerts'] > 0:
        logger.warning(f"Warning alerts detected: {alert_summary['warning_alerts']}")
    
    return alert_summary

alert_check_task = PythonOperator(
    task_id='check_system_alerts',
    python_callable=check_system_alerts,
    dag=dag,
)

# Task 2: Check failed jobs
def check_failed_jobs(**context):
    """Check for failed or stuck jobs"""
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # Simulate checking for failed jobs
    failed_jobs = []
    stuck_jobs = []
    
    # Generate some random failed jobs
    for i in range(random.randint(0, 5)):
        failed_jobs.append({
            'job_id': f'job_{random.randint(1000, 9999)}',
            'job_name': random.choice(['data_processing', 'email_notification', 'report_generation']),
            'failure_reason': random.choice(['timeout', 'resource_error', 'data_error', 'network_error']),
            'failed_at': (datetime.now() - timedelta(minutes=random.randint(5, 120))).isoformat(),
            'retry_count': random.randint(1, 3)
        })
    
    # Generate some random stuck jobs
    for i in range(random.randint(0, 3)):
        stuck_jobs.append({
            'job_id': f'job_{random.randint(1000, 9999)}',
            'job_name': random.choice(['data_processing', 'backup', 'cleanup']),
            'stuck_duration_minutes': random.randint(60, 300),
            'last_heartbeat': (datetime.now() - timedelta(minutes=random.randint(60, 300))).isoformat(),
            'expected_duration_minutes': random.randint(10, 60)
        })
    
    job_status = {
        'timestamp': datetime.now().isoformat(),
        'failed_jobs_count': len(failed_jobs),
        'stuck_jobs_count': len(stuck_jobs),
        'failed_jobs': failed_jobs,
        'stuck_jobs': stuck_jobs,
        'requires_attention': len(failed_jobs) > 0 or len(stuck_jobs) > 0
    }
    
    logger.info(f"Job status check completed: {job_status}")
    
    if job_status['requires_attention']:
        logger.warning(f"Jobs requiring attention - Failed: {len(failed_jobs)}, Stuck: {len(stuck_jobs)}")
    
    return job_status

job_check_task = PythonOperator(
    task_id='check_failed_jobs',
    python_callable=check_failed_jobs,
    dag=dag,
)

# Task 3: Check user notifications
def check_user_notifications(**context):
    """Check for pending user notifications"""
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # Simulate checking for pending notifications
    notifications = []
    
    notification_types = ['job_completion', 'job_failure', 'system_maintenance', 'account_update', 'security_alert']
    
    for i in range(random.randint(0, 10)):
        notifications.append({
            'notification_id': f'notif_{random.randint(1000, 9999)}',
            'user_id': f'user_{random.randint(100, 999)}',
            'type': random.choice(notification_types),
            'priority': random.choice(['low', 'medium', 'high']),
            'message': f'Sample notification message for {random.choice(notification_types)}',
            'created_at': (datetime.now() - timedelta(minutes=random.randint(1, 60))).isoformat(),
            'delivery_method': random.choice(['email', 'sms', 'push', 'in_app'])
        })
    
    # Group by priority
    high_priority = [n for n in notifications if n['priority'] == 'high']
    medium_priority = [n for n in notifications if n['priority'] == 'medium']
    low_priority = [n for n in notifications if n['priority'] == 'low']
    
    notification_summary = {
        'timestamp': datetime.now().isoformat(),
        'total_pending': len(notifications),
        'high_priority_count': len(high_priority),
        'medium_priority_count': len(medium_priority),
        'low_priority_count': len(low_priority),
        'notifications': notifications,
        'urgent_notifications': high_priority
    }
    
    logger.info(f"User notification check completed: {notification_summary}")
    
    if len(high_priority) > 0:
        logger.warning(f"High priority notifications pending: {len(high_priority)}")
    
    return notification_summary

notification_check_task = PythonOperator(
    task_id='check_user_notifications',
    python_callable=check_user_notifications,
    dag=dag,
)

# Task 4: Send critical alerts
def send_critical_alerts(**context):
    """Send critical alerts to administrators"""
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get results from previous tasks
    ti = context['ti']
    system_alerts = ti.xcom_pull(task_ids='check_system_alerts')
    job_status = ti.xcom_pull(task_ids='check_failed_jobs')
    
    critical_issues = []
    
    # Check for critical system alerts
    if system_alerts['critical_alerts'] > 0:
        critical_issues.extend([a for a in system_alerts['alerts'] if a['severity'] == 'critical'])
    
    # Check for multiple failed jobs
    if job_status['failed_jobs_count'] > 3:
        critical_issues.append({
            'component': 'job_system',
            'severity': 'critical',
            'message': f'Multiple job failures detected: {job_status["failed_jobs_count"]} failed jobs',
            'timestamp': datetime.now().isoformat()
        })
    
    # Check for stuck jobs
    if job_status['stuck_jobs_count'] > 0:
        critical_issues.append({
            'component': 'job_system',
            'severity': 'warning',
            'message': f'Stuck jobs detected: {job_status["stuck_jobs_count"]} jobs',
            'timestamp': datetime.now().isoformat()
        })
    
    alert_response = {
        'timestamp': datetime.now().isoformat(),
        'critical_issues_count': len([i for i in critical_issues if i['severity'] == 'critical']),
        'warning_issues_count': len([i for i in critical_issues if i['severity'] == 'warning']),
        'issues': critical_issues,
        'alerts_sent': len(critical_issues) > 0,
        'recipients': ['admin@orienteeringx.com', 'ops-team@orienteeringx.com'] if critical_issues else []
    }
    
    if critical_issues:
        logger.error(f"Critical alerts sent: {alert_response}")
        # In a real implementation, send actual alerts here (email, SMS, Slack, etc.)
    else:
        logger.info("No critical alerts to send")
    
    return alert_response

critical_alert_task = PythonOperator(
    task_id='send_critical_alerts',
    python_callable=send_critical_alerts,
    dag=dag,
)

# Task 5: Process user notifications
def process_user_notifications(**context):
    """Process and send user notifications"""
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get notification data
    ti = context['ti']
    notification_data = ti.xcom_pull(task_ids='check_user_notifications')
    
    # Process notifications by priority
    processed = {
        'timestamp': datetime.now().isoformat(),
        'high_priority_sent': 0,
        'medium_priority_sent': 0,
        'low_priority_sent': 0,
        'failed_deliveries': 0,
        'delivery_methods_used': {'email': 0, 'sms': 0, 'push': 0, 'in_app': 0}
    }
    
    for notification in notification_data['notifications']:
        # Simulate notification delivery
        delivery_success = random.random() > 0.05  # 95% success rate
        
        if delivery_success:
            if notification['priority'] == 'high':
                processed['high_priority_sent'] += 1
            elif notification['priority'] == 'medium':
                processed['medium_priority_sent'] += 1
            else:
                processed['low_priority_sent'] += 1
            
            processed['delivery_methods_used'][notification['delivery_method']] += 1
        else:
            processed['failed_deliveries'] += 1
    
    processed['total_sent'] = (processed['high_priority_sent'] + 
                              processed['medium_priority_sent'] + 
                              processed['low_priority_sent'])
    
    logger.info(f"User notifications processed: {processed}")
    
    if processed['failed_deliveries'] > 0:
        logger.warning(f"Failed notification deliveries: {processed['failed_deliveries']}")
    
    return processed

user_notification_task = PythonOperator(
    task_id='process_user_notifications',
    python_callable=process_user_notifications,
    dag=dag,
)

# Task 6: Generate notification summary
def generate_notification_summary(**context):
    """Generate summary report of all notification activities"""
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Get results from all previous tasks
    ti = context['ti']
    system_alerts = ti.xcom_pull(task_ids='check_system_alerts')
    job_status = ti.xcom_pull(task_ids='check_failed_jobs')
    critical_alerts = ti.xcom_pull(task_ids='send_critical_alerts')
    user_notifications = ti.xcom_pull(task_ids='process_user_notifications')
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'period': '30_minutes',
        'system_health': {
            'status': system_alerts['system_status'],
            'total_alerts': system_alerts['total_alerts'],
            'critical_alerts': system_alerts['critical_alerts']
        },
        'job_health': {
            'failed_jobs': job_status['failed_jobs_count'],
            'stuck_jobs': job_status['stuck_jobs_count'],
            'requires_attention': job_status['requires_attention']
        },
        'alert_activity': {
            'critical_alerts_sent': critical_alerts['alerts_sent'],
            'recipients_notified': len(critical_alerts['recipients'])
        },
        'user_notifications': {
            'total_sent': user_notifications['total_sent'],
            'failed_deliveries': user_notifications['failed_deliveries'],
            'delivery_success_rate': round((user_notifications['total_sent'] / 
                                          (user_notifications['total_sent'] + user_notifications['failed_deliveries']) * 100), 2) 
                                          if (user_notifications['total_sent'] + user_notifications['failed_deliveries']) > 0 else 100
        },
        'overall_status': 'critical' if system_alerts['system_status'] == 'critical' or critical_alerts['alerts_sent'] 
                         else 'warning' if system_alerts['system_status'] == 'warning' or job_status['requires_attention']
                         else 'healthy'
    }
    
    logger.info(f"Notification summary generated: {summary}")
    return summary

summary_task = PythonOperator(
    task_id='generate_notification_summary',
    python_callable=generate_notification_summary,
    dag=dag,
)

# Set up task dependencies
[alert_check_task, job_check_task, notification_check_task] >> critical_alert_task
notification_check_task >> user_notification_task
[critical_alert_task, user_notification_task] >> summary_task