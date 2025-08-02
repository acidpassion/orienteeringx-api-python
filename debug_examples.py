#!/usr/bin/env python3
"""
Debug Examples for OrienteeringX API
Demonstrates various debugging techniques and patterns
"""
import asyncio
import sys
import os
import logging
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging for debugging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def debug_database_operations():
    """Example: Debug database operations"""
    print("🔍 Debugging Database Operations")
    print("-" * 40)
    
    try:
        from app.db.mongodb import connect_to_mongo, get_database, close_mongo_connection
        
        # Connect to database
        logger.debug("Connecting to MongoDB...")
        await connect_to_mongo()
        
        # Get database instance
        db = await get_database()
        logger.debug(f"Database instance: {db}")
        
        # Test collection access
        collections = await db.list_collection_names()
        logger.debug(f"Available collections: {collections}")
        
        # Test a simple query
        if 'match_ref' in collections:
            count = await db.match_ref.count_documents({})
            logger.debug(f"match_ref document count: {count}")
            
            # Sample document
            sample = await db.match_ref.find_one({})
            if sample:
                logger.debug(f"Sample document keys: {list(sample.keys())}")
        
        await close_mongo_connection()
        print("✅ Database debugging completed")
        
    except Exception as e:
        logger.error(f"Database debugging failed: {e}")
        # Add breakpoint for debugging
        breakpoint()  # This will pause execution for debugging


async def debug_job_execution():
    """Example: Debug job execution"""
    print("\n🔍 Debugging Job Execution")
    print("-" * 40)
    
    try:
        from app.jobs.sample_jobs import scrape_orienteering_data_job
        
        # Add timing
        start_time = datetime.now()
        logger.debug(f"Starting job at: {start_time}")
        
        # Execute job with debugging
        result = await scrape_orienteering_data_job()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.debug(f"Job completed in {duration:.2f} seconds")
        logger.debug(f"Job result: {result}")
        
        print("✅ Job debugging completed")
        
    except Exception as e:
        logger.error(f"Job debugging failed: {e}")
        # Add breakpoint for debugging
        breakpoint()


def debug_scheduler_operations():
    """Example: Debug scheduler operations"""
    print("\n🔍 Debugging Scheduler Operations")
    print("-" * 40)
    
    try:
        from app.jobs.scheduler import scheduler
        
        logger.debug(f"Scheduler running: {scheduler.running}")
        
        if scheduler.running:
            jobs = scheduler.get_jobs()
            logger.debug(f"Number of jobs: {len(jobs)}")
            
            for job in jobs:
                logger.debug(f"Job: {job.name}")
                logger.debug(f"  - ID: {job.id}")
                logger.debug(f"  - Next run: {job.next_run_time}")
                logger.debug(f"  - Trigger: {job.trigger}")
        
        print("✅ Scheduler debugging completed")
        
    except Exception as e:
        logger.error(f"Scheduler debugging failed: {e}")
        breakpoint()


def debug_api_endpoints():
    """Example: Debug API endpoints"""
    print("\n🔍 Debugging API Endpoints")
    print("-" * 40)
    
    try:
        import requests
        
        base_url = "http://localhost:8000"
        
        # Test health endpoint
        logger.debug("Testing health endpoint...")
        response = requests.get(f"{base_url}/api/v1/health", timeout=5)
        logger.debug(f"Health response: {response.status_code}")
        
        # Test job status endpoint
        logger.debug("Testing job status endpoint...")
        response = requests.get(f"{base_url}/jobs/api/status", timeout=5)
        logger.debug(f"Job status response: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"Job status data: {data}")
        
        print("✅ API debugging completed")
        
    except requests.exceptions.ConnectionError:
        logger.warning("API server not running - start with 'python run.py'")
    except Exception as e:
        logger.error(f"API debugging failed: {e}")
        breakpoint()


def debug_with_pdb():
    """Example: Using Python debugger (pdb)"""
    print("\n🔍 Debugging with PDB")
    print("-" * 40)
    
    # Sample data for debugging
    data = {
        "games": [
            {"id": "1", "name": "Game 1", "groups": ["A", "B"]},
            {"id": "2", "name": "Game 2", "groups": ["C", "D", "E"]},
        ]
    }
    
    # Set a breakpoint here
    import pdb; pdb.set_trace()
    
    # Process data (you can step through this in debugger)
    for game in data["games"]:
        game_id = game["id"]
        game_name = game["name"]
        group_count = len(game["groups"])
        
        logger.debug(f"Processing game {game_id}: {game_name} with {group_count} groups")
    
    print("✅ PDB debugging completed")


async def main():
    """Main debugging function"""
    print("🐛 OrienteeringX API Debug Examples")
    print("=" * 50)
    
    # Choose which debugging examples to run
    examples = [
        ("Database Operations", debug_database_operations),
        ("Job Execution", debug_job_execution),
        ("Scheduler Operations", debug_scheduler_operations),
        ("API Endpoints", debug_api_endpoints),
        ("PDB Debugging", debug_with_pdb),
    ]
    
    for name, func in examples:
        try:
            if asyncio.iscoroutinefunction(func):
                await func()
            else:
                func()
        except KeyboardInterrupt:
            print(f"\n🛑 Debugging interrupted during: {name}")
            break
        except Exception as e:
            logger.error(f"Error in {name}: {e}")
            continue
    
    print("\n🎉 All debugging examples completed!")


if __name__ == "__main__":
    # You can run specific examples by passing arguments
    if len(sys.argv) > 1:
        example = sys.argv[1].lower()
        
        if example == "db":
            asyncio.run(debug_database_operations())
        elif example == "job":
            asyncio.run(debug_job_execution())
        elif example == "scheduler":
            debug_scheduler_operations()
        elif example == "api":
            debug_api_endpoints()
        elif example == "pdb":
            debug_with_pdb()
        else:
            print(f"Unknown example: {example}")
            print("Available examples: db, job, scheduler, api, pdb")
    else:
        # Run all examples
        asyncio.run(main())