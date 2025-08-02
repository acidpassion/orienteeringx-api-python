"""
Sample Jobs for OrienteeringX
This module contains various sample jobs that can be scheduled and executed
"""
import asyncio
import logging
import requests
import json
import time
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import random

logger = logging.getLogger(__name__)

async def sample_job():
    """A simple sample job for demonstration"""
    logger.info("Sample job started")
    
    # Simulate some work
    await asyncio.sleep(2)
    
    # Generate some sample data
    result = {
        "job_type": "sample",
        "execution_time": datetime.now().isoformat(),
        "status": "completed",
        "data": {
            "random_number": random.randint(1, 100),
            "message": "Sample job executed successfully"
        }
    }
    
    logger.info(f"Sample job completed: {result}")
    return result

async def data_processing_job():
    """Simulate a data processing job"""
    logger.info("Data processing job started")
    
    try:
        # Simulate data processing steps
        steps = [
            "Loading data from database",
            "Validating data integrity",
            "Processing records",
            "Calculating aggregations",
            "Updating cache",
            "Generating reports"
        ]
        
        processed_records = 0
        for i, step in enumerate(steps):
            logger.info(f"Step {i+1}/{len(steps)}: {step}")
            await asyncio.sleep(1)  # Simulate processing time
            processed_records += random.randint(100, 500)
        
        result = {
            "job_type": "data_processing",
            "execution_time": datetime.now().isoformat(),
            "status": "completed",
            "data": {
                "total_records_processed": processed_records,
                "processing_duration_seconds": len(steps),
                "steps_completed": len(steps),
                "cache_updated": True
            }
        }
        
        logger.info(f"Data processing job completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Data processing job failed: {e}")
        return {
            "job_type": "data_processing",
            "execution_time": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }

async def email_notification_job():
    """Simulate sending email notifications"""
    logger.info("Email notification job started")
    
    try:
        # Simulate checking for pending notifications
        await asyncio.sleep(1)
        
        # Generate sample notifications
        notifications = []
        notification_types = ["welcome", "reminder", "alert", "report"]
        
        for i in range(random.randint(5, 15)):
            notifications.append({
                "id": f"notif_{i+1}",
                "type": random.choice(notification_types),
                "recipient": f"user{i+1}@example.com",
                "subject": f"Sample {random.choice(notification_types)} notification",
                "sent_at": datetime.now().isoformat()
            })
        
        # Simulate sending emails
        for notification in notifications:
            logger.info(f"Sending email to {notification['recipient']}: {notification['subject']}")
            await asyncio.sleep(0.1)  # Simulate email sending time
        
        result = {
            "job_type": "email_notification",
            "execution_time": datetime.now().isoformat(),
            "status": "completed",
            "data": {
                "notifications_sent": len(notifications),
                "notification_types": list(set([n["type"] for n in notifications])),
                "total_recipients": len(set([n["recipient"] for n in notifications]))
            }
        }
        
        logger.info(f"Email notification job completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Email notification job failed: {e}")
        return {
            "job_type": "email_notification",
            "execution_time": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }

async def report_generation_job():
    """Simulate generating various reports"""
    logger.info("Report generation job started")
    
    try:
        # Simulate report generation
        reports = [
            {"name": "Daily User Activity", "type": "user_activity"},
            {"name": "System Performance", "type": "performance"},
            {"name": "Error Summary", "type": "errors"},
            {"name": "Revenue Report", "type": "revenue"}
        ]
        
        generated_reports = []
        
        for report in reports:
            logger.info(f"Generating {report['name']} report")
            await asyncio.sleep(1.5)  # Simulate report generation time
            
            # Simulate report data
            report_data = {
                "name": report["name"],
                "type": report["type"],
                "generated_at": datetime.now().isoformat(),
                "file_size_kb": random.randint(50, 500),
                "records_count": random.randint(1000, 10000),
                "file_path": f"/reports/{report['type']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            }
            
            generated_reports.append(report_data)
            logger.info(f"Report generated: {report_data['file_path']}")
        
        result = {
            "job_type": "report_generation",
            "execution_time": datetime.now().isoformat(),
            "status": "completed",
            "data": {
                "reports_generated": len(generated_reports),
                "total_file_size_kb": sum([r["file_size_kb"] for r in generated_reports]),
                "total_records": sum([r["records_count"] for r in generated_reports]),
                "reports": generated_reports
            }
        }
        
        logger.info(f"Report generation job completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Report generation job failed: {e}")
        return {
            "job_type": "report_generation",
            "execution_time": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }

async def system_health_check_job():
    """Perform system health checks"""
    logger.info("System health check job started")
    
    try:
        # Simulate health checks
        components = {
            "database": {"status": "healthy", "response_time_ms": random.randint(10, 50)},
            "cache": {"status": "healthy", "response_time_ms": random.randint(5, 20)},
            "api_server": {"status": "healthy", "response_time_ms": random.randint(20, 100)},
            "job_scheduler": {"status": "healthy", "response_time_ms": random.randint(5, 15)},
            "file_storage": {"status": "healthy", "response_time_ms": random.randint(30, 80)}
        }
        
        # Randomly introduce some warnings
        if random.random() < 0.2:  # 20% chance
            component = random.choice(list(components.keys()))
            components[component]["status"] = "warning"
            components[component]["response_time_ms"] = random.randint(100, 300)
        
        # Check each component
        for component, data in components.items():
            logger.info(f"Checking {component}: {data['status']} ({data['response_time_ms']}ms)")
            await asyncio.sleep(0.5)
        
        # Calculate overall health
        healthy_count = len([c for c in components.values() if c["status"] == "healthy"])
        warning_count = len([c for c in components.values() if c["status"] == "warning"])
        
        overall_status = "healthy" if warning_count == 0 else "warning"
        
        result = {
            "job_type": "system_health_check",
            "execution_time": datetime.now().isoformat(),
            "status": "completed",
            "data": {
                "overall_status": overall_status,
                "components_checked": len(components),
                "healthy_components": healthy_count,
                "warning_components": warning_count,
                "average_response_time_ms": sum([c["response_time_ms"] for c in components.values()]) / len(components),
                "component_details": components
            }
        }
        
        logger.info(f"System health check completed: Overall status = {overall_status}")
        return result
        
    except Exception as e:
        logger.error(f"System health check failed: {e}")
        return {
            "job_type": "system_health_check",
            "execution_time": datetime.now().isoformat(),
            "status": "failed",
            "error": str(e)
        }

async def database_cleanup_job():
    """Database cleanup job - removes old data"""
    logger.info("Starting database cleanup job")
    
    # Simulate database cleanup
    await asyncio.sleep(2)
    
    # Simulate cleanup operations
    tables_cleaned = random.randint(3, 8)
    records_deleted = random.randint(100, 1000)
    
    logger.info(f"Database cleanup completed - {tables_cleaned} tables cleaned, {records_deleted} old records deleted")
    return {
        "status": "completed",
        "tables_cleaned": tables_cleaned,
        "records_deleted": records_deleted,
        "cleanup_time": datetime.now().isoformat()
    }


async def scrape_orienteering_data_job():
    """Scrape orienteering data from API every minute"""
    logger.info("Starting orienteering data scraping job")
    
    # Configuration
    BASE_URL = 'https://api.verymuchsport.cn/app-api/match/game/runner/list'
    logger.info(f"DEBUG: Using BASE_URL: {BASE_URL}")  # Debug line for breakpoint tracking
    
    try:
        # Get database connection
        from app.db.mongodb import get_database
        db = await get_database()
        
        if db is None:
            logger.error("Database connection not available")
            return {
                "status": "error",
                "reason": "Database connection not available",
                "timestamp": datetime.now().isoformat()
            }
        
        # Get reference data from match_ref collection
        match_ref_collection = db.match_ref
        games_data = await match_ref_collection.find({}).to_list(length=None)
        
        if not games_data:
            logger.warning("No reference data found in match_ref collection. Skipping scrape job.")
            return {
                "status": "skipped",
                "reason": "No reference data found in match_ref collection",
                "timestamp": datetime.now().isoformat()
            }
        
        logger.info(f"Found {len(games_data)} games in match_ref collection")
        
        # Clear all existing records in match_result collection
        match_result_collection = db.match_result
        delete_result = await match_result_collection.delete_many({})
        logger.info(f"Cleared {delete_result.deleted_count} existing records from match_result collection")
        
        total_runners_scraped = 0
        games_processed = 0
        all_scraped_data = []
        
        for game_info in games_data:
            try:
                game_id = game_info.get('gameId')
                game_name = game_info.get('name', f'Game_{game_id}')
                groups = game_info.get('groups', [])
                group_ids = [group.get('groupId') for group in groups if group.get('groupId')]
                
                if not game_id:
                    logger.warning("Game entry missing gameId, skipping")
                    continue
                
                if not group_ids:
                    logger.warning(f"No valid group IDs found for game '{game_name}', skipping")
                    continue
                
                logger.info(f"Processing game '{game_name}' ({game_id}) with {len(group_ids)} groups")
                
                # Fetch data for this game
                game_runners = await _fetch_data_for_game_async(game_id, group_ids, BASE_URL)
                
                if game_runners:
                    # Add metadata to each runner record
                    for runner in game_runners:
                        runner['gameId'] = game_id
                        runner['gameName'] = game_name
                        runner['scrapedAt'] = datetime.now()
                    
                    all_scraped_data.extend(game_runners)
                    total_runners_scraped += len(game_runners)
                    games_processed += 1
                    logger.info(f"Fetched {len(game_runners)} runners for game '{game_name}'")
                else:
                    logger.warning(f"No data fetched for game '{game_name}'")
                    
            except Exception as e:
                logger.error(f"Error processing game {game_info.get('name', 'Unknown')}: {e}")
                continue
        
        # Insert all scraped data into match_result collection
        if all_scraped_data:
            insert_result = await match_result_collection.insert_many(all_scraped_data)
            logger.info(f"Inserted {len(insert_result.inserted_ids)} runner records into match_result collection")
        
        logger.info(f"Scraping job completed - {games_processed} games processed, {total_runners_scraped} total runners scraped")
        
        return {
            "status": "completed",
            "games_processed": games_processed,
            "total_runners_scraped": total_runners_scraped,
            "records_cleared": delete_result.deleted_count,
            "records_inserted": len(all_scraped_data),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Scraping job failed: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


async def _fetch_data_for_game_async(game_id, group_ids, base_url):
    """Async helper function to fetch data for a specific game across multiple groups"""
    all_runners = []
    
    for group_id in group_ids:
        params = {'gameId': game_id, 'groupIds': group_id}
        try:
            # Use asyncio to run the synchronous requests call in a thread pool
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: requests.get(base_url, params=params, timeout=10)
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == 0 and data.get('data'):
                runners = data['data']
                all_runners.extend(runners)
                logger.debug(f"Fetched {len(runners)} runners for group {group_id}")
            else:
                logger.warning(f"No data found for group {group_id}. Response: {data.get('msg', 'No message')}")
            
            # Be respectful to the API server
            await asyncio.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for group {group_id}: {e}")
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON for group {group_id}")
        except Exception as e:
            logger.error(f"Unexpected error for group {group_id}: {e}")
    
    return all_runners