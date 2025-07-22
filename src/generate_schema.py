#!/usr/bin/env python3
"""
Script to generate a PostgreSQL database schema based on transformed CoinGecko data
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any, List

class SchemaGenerator:
    
    def __init__(self):
        self.data_dir = "data"
    
    def load_transformed_data(self, filename_pattern: str) -> pd.DataFrame:
        """Loads the most recent transformed data"""
        try:
            files = [f for f in os.listdir(self.data_dir) if f.startswith(filename_pattern)]
            if not files:
                print(f"❌ No files were found matching: {filename_pattern}")
                return pd.DataFrame()
            
            latest_file = max(files)
            file_path = os.path.join(self.data_dir, latest_file)
            
            print(f"📁 Loading data from: {latest_file}")
            
            if latest_file.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_json(file_path)
            
            print(f"✅ Data uploaded: {len(df)} records, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return pd.DataFrame()
    
    def analyze_column_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Analyze the data types of the columns"""
        type_mapping = {}
        
        for column in df.columns:
            # Get pandas data type
            dtype = str(df[column].dtype)
            
            # Map to PostgreSQL types
            if 'int' in dtype:
                # Check if it is an ID or a large value
                if 'id' in column.lower() or df[column].max() < 2147483647:
                    type_mapping[column] = 'INTEGER'
                else:
                    type_mapping[column] = 'BIGINT'
            elif 'float' in dtype:
                type_mapping[column] = 'DECIMAL(20,8)'
            elif 'datetime' in dtype or 'timestamp' in dtype:
                type_mapping[column] = 'TIMESTAMP WITH TIME ZONE'
            elif 'bool' in dtype:
                type_mapping[column] = 'BOOLEAN'
            else:
                # For strings, check maximum length
                max_length = df[column].astype(str).str.len().max()
                if pd.isna(max_length) or max_length <= 255:
                    type_mapping[column] = f'VARCHAR(255)'  # Buffer extra
                else:
                    type_mapping[column] = 'TEXT'
        
        return type_mapping
    
    def generate_table_schema(self, df: pd.DataFrame, table_name: str) -> str:
        """Genera el esquema SQL para una tabla"""
        type_mapping = self.analyze_column_types(df)
        
        # Identify columns that could be primary keys
        primary_key_candidates = []
        for col in df.columns:
            if 'id' in col.lower() and df[col].is_unique:
                primary_key_candidates.append(col)
        
        # If there are no obvious candidates, use the first single column
        if not primary_key_candidates:
            for col in df.columns:
                if df[col].is_unique:
                    primary_key_candidates.append(col)
                    break
        
        primary_key = primary_key_candidates[0] if primary_key_candidates else None
        
        # Generate SQL
        sql_lines = [f"CREATE TABLE {table_name} ("]
        
        for i, (column, pg_type) in enumerate(type_mapping.items()):
            line = f"    {column} {pg_type}"
            
            if not df[column].isnull().any():
                line += " NOT NULL"
            
            if column == primary_key:
                line += " PRIMARY KEY"
            
            if i < len(type_mapping) - 1:
                line += ","
            
            sql_lines.append(line)
        
        sql_lines.append(");")
        
        # Add indexes for important columns
        sql_lines.append("")
        sql_lines.append("-- Indexes to improve performance")
        
        # Index in extraction timestamp
        if 'extraction_timestamp' in type_mapping:
            sql_lines.append(f"CREATE INDEX idx_{table_name}_extraction_timestamp ON {table_name} (extraction_timestamp);")
        
        # Index in symbol if exists
        if 'symbol' in type_mapping:
            sql_lines.append(f"CREATE INDEX idx_{table_name}_symbol ON {table_name} (symbol);")
        
        # Index in market cap rank if it exists
        if 'market_cap_rank' in type_mapping:
            sql_lines.append(f"CREATE INDEX idx_{table_name}_market_cap_rank ON {table_name} (market_cap_rank);")
        
        return "\n".join(sql_lines)
    
    def generate_upsert_statement(self, df: pd.DataFrame, table_name: str) -> str:
        """Generates an UPSERT statement (INSERT ... ON CONFLICT)"""
        columns = list(df.columns)
        
        # Identify columns for the ON CONFLICT clause
        conflict_columns = []
        for col in df.columns:
            if 'id' in col.lower() and df[col].is_unique:
                conflict_columns.append(col)
                break
        
        if not conflict_columns:
            # Use the first unique column
            for col in df.columns:
                if df[col].is_unique:
                    conflict_columns.append(col)
                    break
        
        conflict_col = conflict_columns[0] if conflict_columns else columns[0]
        
        sql = f"""
        -- UPSERT statement for {table_name}
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        ON CONFLICT ({conflict_col}) 
        DO UPDATE SET
        """
        
        # Add all columns except the conflict column
        update_columns = [col for col in columns if col != conflict_col]
        update_clauses = [f"    {col} = EXCLUDED.{col}" for col in update_columns]
        sql += ",\n".join(update_clauses)
        sql += ";"
        
        return sql
    
    def generate_schema_file(self) -> str:
        """Generates the complete schema file"""
        print("🔧 GENERATING DATABASE SCHEMA")
        print("="*60)
        
        # Load transformed data
        market_df = self.load_transformed_data("transformed_market_data")
        historical_df = self.load_transformed_data("transformed_historical_data")
        
        if market_df.empty and historical_df.empty:
            print("❌ No transformed data found")
            return ""
        
        # Generate complete schema
        schema_lines = [
            "-- ========================================",
            "-- COINGECKO ETL DATABASE SCHEMA",
            "-- Automatically generated " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "-- ========================================",
            "",
            "-- Create schematics",
            "CREATE SCHEMA IF NOT EXISTS staging;",
            "CREATE SCHEMA IF NOT EXISTS curated;",
            ""
        ]
        
        # Schema for market data
        if not market_df.empty:
            schema_lines.extend([
                "-- ========================================",
                "-- TABLE: Current market data",
                "-- ========================================",
                ""
            ])
            
            market_schema = self.generate_table_schema(market_df, "curated.market_data")
            schema_lines.append(market_schema)
            schema_lines.append("")
            
            schema_lines.extend([
                "-- Comments on the table",
                f"COMMENT ON TABLE curated.market_data IS 'Current cryptocurrency market data from CoinGecko';",
                f"COMMENT ON COLUMN curated.market_data.coin_id IS 'Unique cryptocurrency identifier';",
                f"COMMENT ON COLUMN curated.market_data.current_price_usd IS 'Current price in USD';",
                f"COMMENT ON COLUMN curated.market_data.market_cap_usd IS 'Market cap in USD';",
                f"COMMENT ON COLUMN curated.market_data.extraction_timestamp IS 'Timestamp of when the data was extracted';",
                ""
            ])
        
        # Schema for historical data
        if not historical_df.empty:
            schema_lines.extend([
                "-- ========================================",
                "-- TABLE: Historical data",
                "-- ========================================",
                ""
            ])
            
            historical_schema = self.generate_table_schema(historical_df, "curated.historical_data")
            schema_lines.append(historical_schema)
            schema_lines.append("")
            
            schema_lines.extend([
                "-- Comments on the table",
                f"COMMENT ON TABLE curated.historical_data IS 'Historical cryptocurrency data from CoinGecko';",
                f"COMMENT ON COLUMN curated.historical_data.coin_id IS 'Unique cryptocurrency identifier';",
                f"COMMENT ON COLUMN curated.historical_data.price_usd IS 'Price in USD for the historical date';",
                ""
            ])
        
        # Add useful views
        schema_lines.extend([
            "-- ========================================",
            "-- USEFUL VIEWS",
            "-- ========================================",
            "",
            "-- View of top 10 by market cap",
            "CREATE OR REPLACE VIEW curated.top_market_cap AS",
            "SELECT coin_id, symbol, name, current_price_usd, market_cap_usd, market_cap_rank",
            "FROM curated.market_data",
            "WHERE market_cap_rank <= 10",
            "ORDER BY market_cap_rank;",
            "",
            "-- 24-hour overview of the most profitable cryptocurrencies",
            "CREATE OR REPLACE VIEW curated.top_gainers_24h AS",
            "SELECT coin_id, symbol, name, current_price_usd, price_change_percentage_24h",
            "FROM curated.market_data",
            "WHERE price_change_percentage_24h > 0",
            "ORDER BY price_change_percentage_24h DESC",
            "LIMIT 10;",
            "",
            "-- Overview of cryptocurrencies with the biggest loss in the last 24 hours",
            "CREATE OR REPLACE VIEW curated.top_losers_24h AS",
            "SELECT coin_id, symbol, name, current_price_usd, price_change_percentage_24h",
            "FROM curated.market_data",
            "WHERE price_change_percentage_24h < 0",
            "ORDER BY price_change_percentage_24h ASC",
            "LIMIT 10;",
            ""
        ])
        
        return "\n".join(schema_lines)
    
    def save_schema(self, schema_content: str) -> str:
        """Save the schema to a SQL file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"schema_coingecko_{timestamp}.sql"
        filepath = os.path.join(self.data_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(schema_content)
        
        print(f"✅ Scheme saved in: {filepath}")
        return filepath

def main():
    generator = SchemaGenerator()
    
    schema_content = generator.generate_schema_file()
    
    if schema_content:
        schema_file = generator.save_schema(schema_content)
        
        print(f"\n📋 SUMMARY OF THE GENERATED SCHEMA:")
        print("="*60)
        print(f"📁 File: {schema_file}")
        print(f"📏 Size: {len(schema_content)} characters")
        print(f"📄 Lines: {schema_content.count(chr(10)) + 1}")
        
        print(f"\n👀 SCHEMA PREVIEW:")
        print("-" * 40)
        lines = schema_content.split('\n')
        for i, line in enumerate(lines[:20]):  # Show first 20 lines
            print(line)
        
        if len(lines) > 20:
            print("...")
            print(f"(showing 20 of {len(lines)} lines)")
        
        print(f"\n✅ Scheme generated successfully!")
    
    else:
        print("❌ The schema could not be generated")

if __name__ == "__main__":
    main() 