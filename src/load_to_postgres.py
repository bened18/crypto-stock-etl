#!/usr/bin/env python3
"""
Script to load transformed data into PostgreSQL
"""

import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
from typing import Optional, Dict, Any
import json

# logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgresLoader:
    """Class for loading data into PostgreSQL"""
    
    def __init__(self):
        self.data_dir = "data"
        
        # Connection configuration from environment variables
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'coingecko_etl'),
            'user': os.getenv('POSTGRES_USER', 'coingecko_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'coingecko_password')
        }
        
        # Create SQLAlchemy engine
        self.connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        self.engine = None
    
    def connect(self) -> bool:
        """Establishes connection to PostgreSQL"""
        try:
            logger.info(f"Connecting to PostgreSQL in {self.db_config['host']}:{self.db_config['port']}")
            
            # Test connection with psycopg2
            conn = psycopg2.connect(**self.db_config)
            conn.close()
            
            # Create SQLAlchemy engine
            self.engine = create_engine(self.connection_string)
            
            logger.info("✅ Connection to PostgreSQL established successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error connecting to PostgreSQL: {e}")
            return False
    
    def execute_schema(self, schema_file: str) -> bool:
        """Execute the SQL schema in the database"""
        try:
            schema_path = os.path.join(self.data_dir, schema_file)
            
            if not os.path.exists(schema_path):
                logger.error(f"❌ Schema file not found: {schema_path}")
                return False
            
            logger.info(f"📋 Running schema from: {schema_file}")
            
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Split the SQL into individual statements
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            with self.engine.connect() as conn:
                for statement in statements:
                    if statement and not statement.startswith('--'):
                        try:
                            conn.execute(text(statement))
                            logger.info(f"✅ Executed: {statement[:50]}...")
                        except Exception as e:
                            logger.warning(f"⚠️ Error executing statement: {e}")
                            # Continue with the next statement
                
                conn.commit()
            
            logger.info("✅ Scheme executed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error executing schema: {e}")
            return False
    
    def load_transformed_data(self, filename_pattern: str, table_name: str) -> bool:
        """Loads transformed data into a specific table"""
        try:
            files = [f for f in os.listdir(self.data_dir) if f.startswith(filename_pattern)]
            if not files:
                logger.error(f"❌ No files were found matching: {filename_pattern}")
                return False
            
            latest_file = max(files)
            file_path = os.path.join(self.data_dir, latest_file)
            
            logger.info(f"📁 Loading data from: {latest_file} to table {table_name}")
            
            # Load data
            if latest_file.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_json(file_path)
            
            if df.empty:
                logger.warning("⚠️ Empty DataFrame, no data to load")
                return False
            
            # Convert timestamps to PostgreSQL compatible format
            timestamp_columns = df.select_dtypes(include=['datetime64']).columns
            for col in timestamp_columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Loading data using SQLAlchemy
            df.to_sql(
                name=table_name.split('.')[-1],  # Only the table name, not the schema
                con=self.engine,
                schema=table_name.split('.')[0],  # Just the schematic
                if_exists='replace',  # Replace table if exists
                index=False,
                method='multi'  # For better performance with multiple rows
            )
            
            logger.info(f"✅ Data loaded successfully: {len(df)} records in {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            return False
    
    def verify_data_loaded(self, table_name: str) -> Dict[str, Any]:
        """Verify that the data was loaded correctly"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                count = result.fetchone()[0]
            
            logger.info(f"✅ Verification completed: {count} records in {table_name}")
            
            return {
                'table': table_name,
                'record_count': count,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"❌ Error verifying data: {e}")
            return {
                'table': table_name,
                'record_count': 0,
                'status': 'error',
                'error': str(e)
            }
    
    def test_queries(self) -> None:
        """Run test queries to verify functionality"""
        logger.info("🧪 Running test queries...")
        
        test_queries = [
            {
                'name': 'Top 5 por Market Cap',
                'query': 'SELECT coin_id, symbol, name, current_price_usd, market_cap_usd FROM curated.market_data ORDER BY market_cap_usd DESC LIMIT 5'
            },
            {
                'name': 'Top Gainers 24h',
                'query': 'SELECT coin_id, symbol, name, price_change_percentage_24h FROM curated.market_data WHERE price_change_percentage_24h > 0 ORDER BY price_change_percentage_24h DESC LIMIT 3'
            },
            {
                'name': 'Top Losers 24h',
                'query': 'SELECT coin_id, symbol, name, price_change_percentage_24h FROM curated.market_data WHERE price_change_percentage_24h < 0 ORDER BY price_change_percentage_24h ASC LIMIT 3'
            },
            {
                'name': 'Estadísticas Generales',
                'query': 'SELECT COUNT(*) as total_coins, AVG(current_price_usd) as avg_price, SUM(market_cap_usd) as total_market_cap FROM curated.market_data'
            }
        ]
        
        with self.engine.connect() as conn:
            for test in test_queries:
                try:
                    logger.info(f"\n📊 {test['name']}:")
                    logger.info("-" * 40)
                    
                    result = conn.execute(text(test['query']))
                    rows = result.fetchall()
                    
                    if rows:
                        columns = result.keys()
                        logger.info(f"Columnas: {', '.join(columns)}")
                        
                        for row in rows:
                            logger.info(f"  {row}")
                    else:
                        logger.info("  There is no data")
                        
                except Exception as e:
                    logger.error(f"❌ Error in query '{test['name']}': {e}")
    
    def generate_summary(self) -> None:
        """Generates a summary of the data load"""
        print(f"\n📋 DATA LOADING SUMMARY")
        print("="*60)
        
        tables = ['curated.market_data', 'curated.historical_data']
        total_records = 0
        
        for table in tables:
            result = self.verify_data_loaded(table)
            if result['status'] == 'success':
                total_records += result['record_count']
                print(f"✅ {table}: {result['record_count']} records")
            else:
                print(f"❌ {table}: Error - {result.get('error', 'Unknown error')}")
        
        print(f"\n📊 Total records uploaded: {total_records}")
        print(f"🗄️ Database: {self.db_config['database']}")
        print(f"🔗 Host: {self.db_config['host']}:{self.db_config['port']}")

def main():
    print("🗄️ LOADING DATA INTO POSTGRESQL")
    print("="*60)
    
    loader = PostgresLoader()
    
    # 1.Connect to PostgreSQL
    if not loader.connect():
        print("❌ Could not connect to PostgreSQL")
        return
    
    # 2. Find and run the latest schema
    schema_files = [f for f in os.listdir(loader.data_dir) if f.startswith('schema_coingecko_')]
    if schema_files:
        latest_schema = max(schema_files)
        if not loader.execute_schema(latest_schema):
            print("❌ Error executing schema")
            return
    else:
        print("⚠️ No schema file found, continuing without creating tables...")
    
    # 3. Load transformed data
    print("\n📤 LOADING TRANSFORMED DATA...")
    
    # Load market data
    market_loaded = loader.load_transformed_data("transformed_market_data", "curated.market_data")
    
    # Load historical data
    historical_loaded = loader.load_transformed_data("transformed_historical_data", "curated.historical_data")
    
    if not market_loaded and not historical_loaded:
        print("❌ Data could not be loaded")
        return
    
    # 4. Generate summary
    loader.generate_summary()
    
    # 5. Run test queries
    print(f"\n🧪 RUNNING TEST QUERIES...")
    loader.test_queries()
    
    print(f"\n✅ Data upload completed successfully!")
    print(f"💡 You can connect to PostgreSQL at localhost:5432")
    print(f"   User: {loader.db_config['user']}")
    print(f"   Database: {loader.db_config['database']}")

if __name__ == "__main__":
    main() 