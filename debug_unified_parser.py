
#!/usr/bin/env python3
"""
Debug script for unified log parser
Helps diagnose why the parser is not finding or processing files
"""

import asyncio
import logging
import os
import glob
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def debug_parser():
    """Debug the unified parser configuration and file paths"""
    print("🔍 Unified Parser Diagnostics")
    print("=" * 50)
    
    # Import the parser
    try:
        from bot.parsers.unified_log_parser import UnifiedLogParser
        print("✅ Successfully imported UnifiedLogParser")
    except Exception as e:
        print(f"❌ Failed to import UnifiedLogParser: {e}")
        return
    
    # Check for mock bot
    class MockBot:
        def __init__(self):
            self.db_manager = MockDB()
    
    class MockDB:
        def __init__(self):
            self.guilds = MockCollection()
            self.db = {'parser_state': MockCollection()}
    
    class MockCollection:
        async def find(self, query):
            return MockCursor()
        
        async def find_one(self, query):
            return None
        
        async def replace_one(self, filter_doc, replacement, upsert=False):
            pass
    
    class MockCursor:
        async def to_list(self, length=None):
            # Return a mock guild with server data
            return [{
                '_id': 123456789,
                'name': 'Test Guild',
                'servers': [{
                    '_id': '7020',
                    'name': 'Test Server',
                    'host': '79.127.236.1',
                    'username': 'testuser',
                    'password': 'testpass',
                    'port': 22
                }]
            }]
    
    # Initialize parser with mock bot
    bot = MockBot()
    parser = UnifiedLogParser(bot)
    
    print("✅ Successfully initialized UnifiedLogParser")
    
    # Check current directory structure
    print("\n📂 Current Directory Structure:")
    current_dir = os.getcwd()
    print(f"Current working directory: {current_dir}")
    
    # Look for server directories
    server_patterns = [
        "./*_*",  # host_serverid pattern
        "./logs/*",
        "./data/*",
        "./**/Logs/Deadside.log"
    ]
    
    found_dirs = []
    for pattern in server_patterns:
        matches = glob.glob(pattern)
        if matches:
            found_dirs.extend(matches)
    
    if found_dirs:
        print("📁 Found potential server directories:")
        for directory in found_dirs:
            print(f"  - {directory}")
            if os.path.isdir(directory):
                try:
                    contents = os.listdir(directory)
                    print(f"    Contents: {contents[:10]}{'...' if len(contents) > 10 else ''}")
                except Exception as e:
                    print(f"    Error reading directory: {e}")
    else:
        print("❌ No server directories found")
    
    # Search for any Deadside.log files
    print("\n🔍 Searching for Deadside.log files:")
    deadside_logs = []
    for root, dirs, files in os.walk('.'):
        if 'Deadside.log' in files:
            log_path = os.path.join(root, 'Deadside.log')
            deadside_logs.append(log_path)
            file_size = os.path.getsize(log_path)
            print(f"  📄 Found: {log_path} ({file_size} bytes)")
    
    if not deadside_logs:
        print("❌ No Deadside.log files found")
    
    # Test the parser's path resolution logic
    print("\n🔧 Testing Parser Path Resolution:")
    test_server = {
        '_id': '7020',
        'name': 'Test Server',
        'host': '79.127.236.1'
    }
    
    expected_path = f"./{test_server['host']}_{test_server['_id']}/Logs/Deadside.log"
    print(f"Expected path: {expected_path}")
    print(f"Path exists: {os.path.exists(expected_path)}")
    
    # Check parser file states
    print(f"\n📊 Parser State:")
    print(f"File states: {len(parser.file_states)} servers tracked")
    print(f"Player sessions: {len(parser.player_sessions)} active sessions")
    print(f"Last log positions: {len(parser.last_log_position)} servers")
    
    # Test actual parsing if we found any log files
    if deadside_logs:
        print(f"\n🧪 Testing parsing on first found log file:")
        test_log = deadside_logs[0]
        try:
            with open(test_log, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.splitlines()
            print(f"📊 File has {len(lines)} lines")
            
            if lines:
                print("📋 First few lines:")
                for i, line in enumerate(lines[:3]):
                    print(f"  {i+1}: {line[:100]}{'...' if len(line) > 100 else ''}")
                
                # Test pattern matching
                print("\n🔍 Testing pattern matching:")
                mission_pattern = parser.patterns.get('mission_state_change')
                connection_pattern = parser.patterns.get('player_queue_join')
                
                mission_matches = 0
                connection_matches = 0
                
                for line in lines[:50]:  # Test first 50 lines
                    if mission_pattern and mission_pattern.search(line):
                        mission_matches += 1
                    if connection_pattern and connection_pattern.search(line):
                        connection_matches += 1
                
                print(f"  Mission events found: {mission_matches}")
                print(f"  Connection events found: {connection_matches}")
                
        except Exception as e:
            print(f"❌ Error reading log file: {e}")
    
    print("\n✅ Diagnostics completed")

if __name__ == "__main__":
    asyncio.run(debug_parser())
