#!/usr/bin/env python3
"""
Script to populate the match_ref collection with sample data
"""
import asyncio
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.mongodb import connect_to_mongo, get_database, close_mongo_connection


async def setup_sample_data():
    """Setup sample data in match_ref collection"""
    try:
        # Connect to database
        await connect_to_mongo()
        db = await get_database()
        
        # Sample reference data
        sample_games = [
            {
                "gameId": "12345",
                "name": "Sample_Orienteering_Event_2024",
                "description": "Sample orienteering event for testing",
                "groups": [
                    {
                        "groupId": "group1",
                        "name": "Men Elite"
                    },
                    {
                        "groupId": "group2", 
                        "name": "Women Elite"
                    }
                ],
                "createdAt": "2024-01-01T00:00:00Z",
                "status": "active"
            },
            {
                "gameId": "67890",
                "name": "Test_Competition_2024",
                "description": "Test competition for development",
                "groups": [
                    {
                        "groupId": "group3",
                        "name": "Junior Men"
                    },
                    {
                        "groupId": "group4",
                        "name": "Junior Women"
                    }
                ],
                "createdAt": "2024-01-15T00:00:00Z",
                "status": "active"
            },
            {
                "gameId": "11111",
                "name": "Regional_Championship_2024",
                "description": "Regional championship event",
                "groups": [
                    {
                        "groupId": "group5",
                        "name": "Open Class"
                    }
                ],
                "createdAt": "2024-02-01T00:00:00Z",
                "status": "active"
            }
        ]
        
        # Clear existing data and insert new sample data
        match_ref_collection = db.match_ref
        
        # Clear existing data
        delete_result = await match_ref_collection.delete_many({})
        print(f"Cleared {delete_result.deleted_count} existing records from match_ref collection")
        
        # Insert sample data
        insert_result = await match_ref_collection.insert_many(sample_games)
        print(f"Inserted {len(insert_result.inserted_ids)} sample games into match_ref collection")
        
        # Verify the data
        count = await match_ref_collection.count_documents({})
        print(f"Total documents in match_ref collection: {count}")
        
        # Show the inserted data
        print("\nInserted games:")
        async for game in match_ref_collection.find({}):
            print(f"- {game['name']} (ID: {game['gameId']}) with {len(game['groups'])} groups")
        
        print("\nSample data setup completed successfully!")
        
    except Exception as e:
        print(f"Error setting up sample data: {e}")
        return False
    finally:
        await close_mongo_connection()
    
    return True


if __name__ == "__main__":
    asyncio.run(setup_sample_data())