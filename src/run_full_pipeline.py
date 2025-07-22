#!/usr/bin/env python3
"""
Script to run the complete ETL pipeline
1. Extract data from CoinGecko
2. Transform data
3. Load into PostgreSQL
"""

import subprocess
import sys
import time
from datetime import datetime

def run_command(command, description):
    print(f"\n🔄 {description}")
    print("="*60)
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print("✅ Command executed successfully")
        if result.stdout:
            print("📤 Output:")
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error executing command: {e}")
        if e.stdout:
            print("📤 Output:")
            print(e.stdout)
        if e.stderr:
            print("📤 Error:")
            print(e.stderr)
        return False

def main():
    start_time = time.time()
    
    print("🚀 STARTING PIPELINE ETL COMPLETE")
    print("="*60)
    print(f"⏰ Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # List of commands to execute
    commands = [
        {
            'command': 'python src/extract_coingecko.py',
            'description': 'EXTRACTION: Getting data from CoinGecko API'
        },
        {
            'command': 'python src/transform_data.py',
            'description': 'TRANSFORMATION: Cleaning and structuring data'
        },
        {
            'command': 'python src/generate_schema.py',
            'description': 'SCHEME: Generating database schema'
        },
        {
            'command': 'python src/load_to_postgres.py',
            'description': 'LOAD: Inserting data into PostgreSQL'
        }
    ]
    
    success_count = 0
    for i, cmd_info in enumerate(commands, 1):
        print(f"\n📋 Passed {i}/{len(commands)}")
        
        if run_command(cmd_info['command'], cmd_info['description']):
            success_count += 1
        else:
            print(f"❌ Pipeline failed at the crossing {i}")
            break
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n📊 PIPELINE SUMMARY")
    print("="*60)
    print(f"⏰ Total duration: {duration:.2f} seconds")
    print(f"✅ Successful steps: {success_count}/{len(commands)}")
    print(f"❌ Failed steps: {len(commands) - success_count}")
    
    if success_count == len(commands):
        print(f"\n🎉 ¡ETL PIPELINE SUCCESSFULLY COMPLETED!")
        print(f"💡 Data is available in PostgreSQL")
        print(f"🔗 Connection: localhost:5432")
        print(f"📊 Database: coingecko_etl")
    else:
        print(f"\n❌ PIPELINE FAILED")
        print(f"💡 Review the logs to identify the problem")
        sys.exit(1)

if __name__ == "__main__":
    main() 