#!/usr/bin/env python3
"""
Debug Helper Script for OrienteeringX API
Provides utilities for debugging and testing various components
"""
import asyncio
import sys
import os
import json
from datetime import datetime
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.mongodb import connect_to_mongo, get_database, close_mongo_connection
from app.jobs.scheduler import scheduler
from app.jobs.sample_jobs import scrape_orienteering_data_job


class DebugHelper:
    """Debug helper utilities"""
    
    def __init__(self):
        self.db = None
    
    async def setup(self):
        """Setup database connection"""
        await connect_to_mongo()
        self.db = await get_database()
        print("✅ Database connection established")
    
    async def cleanup(self):
        """Cleanup connections"""
        await close_mongo_connection()
        print("🧹 Connections closed")
    
    async def check_database_status(self):
        """Check database connection and collections"""
        try:
            if not self.db:
                await self.setup()
            
            # Check connection
            server_info = await self.db.client.server_info()
            print(f"📊 MongoDB Version: {server_info.get('version', 'Unknown')}")
            
            # List collections
            collections = await self.db.list_collection_names()
            print(f"📁 Available Collections: {collections}")
            
            # Check collection counts
            for collection_name in collections:
                count = await self.db[collection_name].count_documents({})
                print(f"   - {collection_name}: {count} documents")
            
            return True
        except Exception as e:
            print(f"❌ Database check failed: {e}")
            return False
    
    async def check_match_ref_data(self):
        """Check match_ref collection data"""
        try:
            if not self.db:
                await self.setup()
            
            collection = self.db.match_ref
            count = await collection.count_documents({})
            print(f"🎯 match_ref collection: {count} documents")
            
            if count > 0:
                print("\n📋 Sample documents:")
                async for doc in collection.find({}).limit(3):
                    print(f"   - Game: {doc.get('name', 'Unknown')} (ID: {doc.get('gameId', 'Unknown')})")
                    print(f"     Groups: {len(doc.get('groups', []))}")
            
            return count > 0
        except Exception as e:
            print(f"❌ match_ref check failed: {e}")
            return False
    
    async def check_match_result_data(self):
        """Check match_result collection data"""
        try:
            if not self.db:
                await self.setup()
            
            collection = self.db.match_result
            count = await collection.count_documents({})
            print(f"🏆 match_result collection: {count} documents")
            
            if count > 0:
                print("\n📋 Recent results:")
                async for doc in collection.find({}).sort("timestamp", -1).limit(5):
                    timestamp = doc.get('timestamp', 'Unknown')
                    game_id = doc.get('gameId', 'Unknown')
                    runners = len(doc.get('runners', []))
                    print(f"   - Game {game_id}: {runners} runners at {timestamp}")
            
            return count > 0
        except Exception as e:
            print(f"❌ match_result check failed: {e}")
            return False
    
    async def test_scrape_job(self):
        """Test the scrape job manually"""
        try:
            print("🔄 Testing scrape job...")
            result = await scrape_orienteering_data_job()
            print(f"✅ Scrape job completed: {result}")
            return True
        except Exception as e:
            print(f"❌ Scrape job test failed: {e}")
            return False
    
    async def check_scheduler_status(self):
        """Check scheduler status"""
        try:
            print("⏰ Checking scheduler status...")
            
            if scheduler.running:
                print("✅ Scheduler is running")
                
                jobs = scheduler.get_jobs()
                print(f"📋 Active jobs: {len(jobs)}")
                
                for job in jobs:
                    next_run = job.next_run_time
                    print(f"   - {job.name}: next run at {next_run}")
            else:
                print("❌ Scheduler is not running")
            
            return scheduler.running
        except Exception as e:
            print(f"❌ Scheduler check failed: {e}")
            return False
    
    async def clear_match_results(self):
        """Clear match_result collection"""
        try:
            if not self.db:
                await self.setup()
            
            collection = self.db.match_result
            result = await collection.delete_many({})
            print(f"🧹 Cleared {result.deleted_count} documents from match_result")
            return True
        except Exception as e:
            print(f"❌ Clear operation failed: {e}")
            return False
    
    async def add_sample_match_ref(self):
        """Add sample data to match_ref collection"""
        try:
            if not self.db:
                await self.setup()
            
            sample_data = [
                {
                    "gameId": "debug_game_1",
                    "name": "Debug_Test_Event_2024",
                    "description": "Debug test event",
                    "groups": [
                        {"groupId": "debug_group_1", "name": "Debug Group 1"},
                        {"groupId": "debug_group_2", "name": "Debug Group 2"}
                    ],
                    "createdAt": datetime.utcnow().isoformat() + "Z",
                    "status": "active"
                }
            ]
            
            collection = self.db.match_ref
            result = await collection.insert_many(sample_data)
            print(f"✅ Added {len(result.inserted_ids)} debug documents to match_ref")
            return True
        except Exception as e:
            print(f"❌ Sample data insertion failed: {e}")
            return False


async def main():
    """Main debug function"""
    helper = DebugHelper()
    
    print("🐛 OrienteeringX API Debug Helper")
    print("=" * 50)
    
    try:
        # Check database
        print("\n1. 📊 Checking Database Status...")
        await helper.check_database_status()
        
        # Check match_ref data
        print("\n2. 🎯 Checking match_ref Data...")
        has_ref_data = await helper.check_match_ref_data()
        
        # Check match_result data
        print("\n3. 🏆 Checking match_result Data...")
        await helper.check_match_result_data()
        
        # Test scrape job if we have reference data
        if has_ref_data:
            print("\n4. 🔄 Testing Scrape Job...")
            await helper.test_scrape_job()
        else:
            print("\n4. ⚠️  No reference data found, skipping scrape job test")
            print("   Run with --add-sample to add debug data")
        
        print("\n✅ Debug check completed!")
        
    except Exception as e:
        print(f"\n❌ Debug check failed: {e}")
    finally:
        await helper.cleanup()


def print_usage():
    """Print usage information"""
    print("""
🐛 Debug Helper Usage:

python debug_helper.py [command]

Commands:
  check          - Run full debug check (default)
  db             - Check database status only
  ref            - Check match_ref data
  result         - Check match_result data
  scrape         - Test scrape job
  clear          - Clear match_result collection
  add-sample     - Add sample debug data
  scheduler      - Check scheduler status

Examples:
  python debug_helper.py
  python debug_helper.py db
  python debug_helper.py scrape
  python debug_helper.py clear
""")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "check"
    
    if command in ["-h", "--help", "help"]:
        print_usage()
        sys.exit(0)
    
    helper = DebugHelper()
    
    try:
        if command == "check":
            asyncio.run(main())
        elif command == "db":
            asyncio.run(helper.check_database_status())
        elif command == "ref":
            asyncio.run(helper.check_match_ref_data())
        elif command == "result":
            asyncio.run(helper.check_match_result_data())
        elif command == "scrape":
            async def test_scrape():
                await helper.setup()
                await helper.test_scrape_job()
                await helper.cleanup()
            asyncio.run(test_scrape())
        elif command == "clear":
            async def clear_data():
                await helper.setup()
                await helper.clear_match_results()
                await helper.cleanup()
            asyncio.run(clear_data())
        elif command == "add-sample":
            async def add_sample():
                await helper.setup()
                await helper.add_sample_match_ref()
                await helper.cleanup()
            asyncio.run(add_sample())
        elif command == "scheduler":
            asyncio.run(helper.check_scheduler_status())
        else:
            print(f"❌ Unknown command: {command}")
            print_usage()
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n🛑 Debug helper interrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)