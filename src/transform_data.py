#!/usr/bin/env python3
"""
Script to transform and cleanse CoinGecko data
Prepares data for storage in PostgreSQL
"""

import json
import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging

# logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CoinGeckoTransformer:
    
    def __init__(self):
        self.data_dir = "data"
        
    def load_latest_market_data(self) -> Optional[List[Dict[str, Any]]]:
        """Load the latest market data"""
        try:
            files = [f for f in os.listdir(self.data_dir) if f.startswith('coingecko_market_data_')]
            if not files:
                logger.error("No market data files found")
                return None
            
            latest_file = max(files)
            file_path = os.path.join(self.data_dir, latest_file)
            
            logger.info(f"Loading market data from: {latest_file}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Data uploaded: {len(data)} records")
            return data
            
        except Exception as e:
            logger.error(f"Error loading market data: {e}")
            return None
    
    def load_latest_historical_data(self) -> Optional[Dict[str, Any]]:
        """Loads the most recent historical data"""
        try:
            files = [f for f in os.listdir(self.data_dir) if f.startswith('coingecko_historical_data_')]
            if not files:
                logger.error("No historical data files found")
                return None
            
            latest_file = max(files)
            file_path = os.path.join(self.data_dir, latest_file)
            
            logger.info(f"Loading historical data from: {latest_file}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info("Historical data loaded successfully")
            return data
            
        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
            return None
    
    def transform_market_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transforms raw market data into a clean DataFrame

        Args:
        raw_data: List of dictionaries containing market data

        Returns:
        DataFrame containing transformed data
        """
        logger.info("Transforming market data...")
        
        transformed_records = []
        
        for coin in raw_data:
            try:
                # Extract key fields and apply transformations
                record = {
                    'coin_id': coin.get('id'),
                    'symbol': coin.get('symbol', '').upper(),
                    'name': coin.get('name'),
                    
                    # Prices
                    'current_price_usd': self._safe_float(coin.get('current_price')),
                    'high_24h_usd': self._safe_float(coin.get('high_24h')),
                    'low_24h_usd': self._safe_float(coin.get('low_24h')),
                    
                    # Market Cap
                    'market_cap_usd': self._safe_int(coin.get('market_cap')),
                    'market_cap_rank': self._safe_int(coin.get('market_cap_rank')),
                    'fully_diluted_valuation_usd': self._safe_int(coin.get('fully_diluted_valuation')),
                    
                    # Volume
                    'total_volume_usd': self._safe_int(coin.get('total_volume')),
                    
                    # Price changes
                    'price_change_24h_usd': self._safe_float(coin.get('price_change_24h')),
                    'price_change_percentage_24h': self._safe_float(coin.get('price_change_percentage_24h')),
                    'market_cap_change_24h_usd': self._safe_float(coin.get('market_cap_change_24h')),
                    'market_cap_change_percentage_24h': self._safe_float(coin.get('market_cap_change_percentage_24h')),
                    
                    # Supply
                    'circulating_supply': self._safe_float(coin.get('circulating_supply')),
                    'total_supply': self._safe_float(coin.get('total_supply')),
                    'max_supply': self._safe_float(coin.get('max_supply')),
                    
                    # Maximums and minimums
                    'ath_usd': self._safe_float(coin.get('ath')),
                    'ath_change_percentage': self._safe_float(coin.get('ath_change_percentage')),
                    'ath_date': self._parse_datetime(coin.get('ath_date')),
                    'atl_usd': self._safe_float(coin.get('atl')),
                    'atl_change_percentage': self._safe_float(coin.get('atl_change_percentage')),
                    'atl_date': self._parse_datetime(coin.get('atl_date')),
                    
                    # Metadata
                    'last_updated': self._parse_datetime(coin.get('last_updated')),
                    'extraction_timestamp': self._parse_datetime(coin.get('extraction_timestamp')),
                    
                    # Calculated fields
                    'price_to_ath_ratio': self._calculate_ratio(coin.get('current_price'), coin.get('ath')),
                    'price_to_atl_ratio': self._calculate_ratio(coin.get('current_price'), coin.get('atl')),
                    'market_cap_to_volume_ratio': self._calculate_ratio(coin.get('market_cap'), coin.get('total_volume')),
                }
                
                transformed_records.append(record)
                
            except Exception as e:
                logger.error(f"Error transforming data for {coin.get('id', 'unknown')}: {e}")
                continue
        
        df = pd.DataFrame(transformed_records)
        
        # Sort by market cap rank
        if 'market_cap_rank' in df.columns:
            df = df.sort_values('market_cap_rank')
        
        logger.info(f"Transformation completed: {len(df)} valid records")
        return df
    
    def transform_historical_data(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Transforms raw historical data into a clean DataFrame

        Args:
        raw_data: Dictionary containing historical data

        Returns:
        DataFrame containing transformed data
        """
        logger.info("Transforming historical data...")
        
        try:
            # Extract historical market data
            market_data = raw_data.get('market_data', {})
            
            if not market_data:
                logger.warning("No market data found in historical data")
                return pd.DataFrame()
            
            # Create historical record
            record = {
                'coin_id': raw_data.get('id'),
                'symbol': raw_data.get('symbol', '').upper(),
                'name': raw_data.get('name'),
                
                # Prices in different currencies (only major ones)
                'price_usd': self._safe_float(market_data.get('current_price', {}).get('usd')),
                'price_eur': self._safe_float(market_data.get('current_price', {}).get('eur')),
                'price_btc': self._safe_float(market_data.get('current_price', {}).get('btc')),
                'price_eth': self._safe_float(market_data.get('current_price', {}).get('eth')),
                
                # Market Cap in different currencies
                'market_cap_usd': self._safe_int(market_data.get('market_cap', {}).get('usd')),
                'market_cap_eur': self._safe_int(market_data.get('market_cap', {}).get('eur')),
                'market_cap_btc': self._safe_int(market_data.get('market_cap', {}).get('btc')),
                
                # Volume in different currencies
                'total_volume_usd': self._safe_int(market_data.get('total_volume', {}).get('usd')),
                'total_volume_eur': self._safe_int(market_data.get('total_volume', {}).get('eur')),
                'total_volume_btc': self._safe_int(market_data.get('total_volume', {}).get('btc')),
                
                # Metadata
                'extraction_timestamp': self._parse_datetime(raw_data.get('extraction_timestamp')),
            }
            
            df = pd.DataFrame([record])
            logger.info("Historical data transformation completed")
            return df
            
        except Exception as e:
            logger.error(f"Error transforming historical data: {e}")
            return pd.DataFrame()
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely converts value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely converts value to int"""
        if value is None:
            return None
        try:
            return int(float(value))  # Convert float to int for values like 1.0
        except (ValueError, TypeError):
            return None
    
    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        """Parse datetime safely"""
        if value is None:
            return None
        try:
            return pd.to_datetime(value)
        except (ValueError, TypeError):
            return None
    
    def _calculate_ratio(self, numerator: Any, denominator: Any) -> Optional[float]:
        """Calculate ratio safely"""
        try:
            num = self._safe_float(numerator)
            den = self._safe_float(denominator)
            
            if num is not None and den is not None and den != 0:
                return num / den
            return None
        except:
            return None
    
    def save_transformed_data(self, df: pd.DataFrame, filename: str) -> str:
        """Save transformed data in CSV and JSON formats"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save as CSV
        csv_filename = f"data/transformed_{filename}_{timestamp}.csv"
        df.to_csv(csv_filename, index=False)
        logger.info(f"Data saved as CSV: {csv_filename}")
        
        # Save as JSON
        json_filename = f"data/transformed_{filename}_{timestamp}.json"
        df.to_json(json_filename, orient='records', indent=2, date_format='iso')
        logger.info(f"Data saved as JSON: {json_filename}")
        
        return csv_filename
    
    def generate_summary(self, df: pd.DataFrame, data_type: str) -> None:
        """Generates a summary of the transformed data"""
        print(f"\n📊 SUMMARY OF TRANSFORMED DATA ({data_type})")
        print("="*60)
        
        print(f"📈 Total records: {len(df)}")
        print(f"📅 Date range: {df['extraction_timestamp'].min()} a {df['extraction_timestamp'].max()}")
        
        if 'current_price_usd' in df.columns:
            print(f"\n💰 PRICE STATISTICS (USD):")
            print("-" * 40)
            print(f"  Higher price: ${df['current_price_usd'].max():,.2f}")
            print(f"  lowest price: ${df['current_price_usd'].min():,.2f}")
            print(f"  Average price: ${df['current_price_usd'].mean():,.2f}")
        
        if 'market_cap_usd' in df.columns:
            print(f"\n🏆 MARKET CAP STATISTICS (USD):")
            print("-" * 40)
            print(f"  Total market cap: ${df['market_cap_usd'].sum():,.0f}")
            print(f"  Average market cap: ${df['market_cap_usd'].mean():,.0f}")
        
        print(f"\n🔍 TRANSFORMED FIELDS ({len(df.columns)} fields):")
        print("-" * 40)
        for col in df.columns:
            null_count = df[col].isnull().sum()
            null_pct = (null_count / len(df)) * 100
            print(f"  {col}: {null_count} null ({null_pct:.1f}%)")

def main():
    print("🔄 COINGECKO DATA TRANSFORMATION")
    print("="*60)
    
    transformer = CoinGeckoTransformer()
    
    # 1. Transform market data
    market_data = transformer.load_latest_market_data()
    if market_data:
        market_df = transformer.transform_market_data(market_data)
        if not market_df.empty:
            transformer.save_transformed_data(market_df, "market_data")
            transformer.generate_summary(market_df, "Market Data")
    
    # 2. Transform historical data
    historical_data = transformer.load_latest_historical_data()
    if historical_data:
        historical_df = transformer.transform_historical_data(historical_data)
        if not historical_df.empty:
            transformer.save_transformed_data(historical_df, "historical_data")
            transformer.generate_summary(historical_df, "Historical Data")
    
    print(f"\n✅ Transformation completed!")
    print(f"📁 Files generated in directory 'data/'")

if __name__ == "__main__":
    main() 