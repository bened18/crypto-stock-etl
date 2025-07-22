#!/usr/bin/env python3
"""
Script to extract data from the CoinGecko API
Extracts cryptocurrency price information
"""

import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Any
import logging
from datetime import timedelta

# logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CoinGeckoExtractor:
    
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        # Headers to simulate a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_coin_markets(self, coin_ids: List[str], vs_currency: str = 'usd') -> List[Dict[str, Any]]:
        """
        Gets market data for the specified cryptocurrency

        Args:
        coin_ids: List of cryptocurrency IDs (e.g., ['bitcoin', 'ethereum'])
        vs_currency: Reference currency (default: 'usd')

        Returns:
        List of dictionaries with market data
        """
        try:
            # Build the URL with the cryptocurrency IDs
            ids_param = ','.join(coin_ids)
            url = f"{self.base_url}/coins/markets"
            
            params = {
                'vs_currency': vs_currency,
                'ids': ids_param,
                'order': 'market_cap_desc',
                'per_page': 250,
                'page': 1,
                'sparkline': False,
                'locale': 'en'
            }
            
            logger.info(f"Extracting data for: {ids_param}")
            logger.info(f"URL: {url}")
            logger.info(f"Parameters: {params}")
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Data successfully extracted. Records obtained: {len(data)}")
            
            extraction_time = datetime.utcnow().isoformat()
            for coin in data:
                coin['extraction_timestamp'] = extraction_time
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error in HTTP request: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def get_coin_history(self, coin_id: str, date: str, vs_currency: str = 'usd') -> Dict[str, Any]:
        """
        Gets historical data for a cryptocurrency for a specific date

        Args:
        coin_id: Cryptocurrency ID (e.g., 'bitcoin')
        date: Date in dd-mm-yyyy format
        vs_currency: Reference currency (default: 'usd')

        Returns:
        Dictionary with historical data
        """
        try:
            url = f"{self.base_url}/coins/{coin_id}/history"
            
            params = {
                'date': date,
                'vs_currency': vs_currency,
                'localization': False
            }
            
            logger.info(f"Extracting historical data for {coin_id} on date {date}")
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            data['extraction_timestamp'] = datetime.utcnow().isoformat()
            
            logger.info(f"Historical data successfully extracted for {coin_id}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error in HTTP request: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

def main():
    
    test_coins = ['bitcoin', 'ethereum', 'cardano', 'solana']
    
    extractor = CoinGeckoExtractor()
    
    try:
        # 1. Extract current market data
        logger.info("=== MARKET DATA EXTRACTION ===")
        market_data = extractor.get_coin_markets(test_coins)
        
        print("\n" + "="*50)
        print("EXTRACTED MARKET DATA:")
        print("="*50)
        
        for coin in market_data:
            print(f"\n🪙 {coin['name']} ({coin['symbol'].upper()})")
            print(f"   Price: ${coin['current_price']:,.2f}")
            print(f"   Market Cap: ${coin['market_cap']:,.0f}")
            print(f"   Volume 24h: ${coin['total_volume']:,.0f}")
            print(f"   Change 24h: {coin['price_change_percentage_24h']:.2f}%")
            print(f"   Extracted: {coin['extraction_timestamp']}")
        
        # Save data to JSON file for inspection
        output_file = f"data/coingecko_market_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(market_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Data saved in: {output_file}")
        
        # 2. Extract historical data (example with Bitcoin)
        logger.info("\n=== EXTRACTION OF HISTORICAL DATA ===")
        
        # Use yesterday's date for historical data
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')
        
        historical_data = extractor.get_coin_history('bitcoin', yesterday)
        
        print(f"\n📅 BITCOIN HISTORICAL DATA ({yesterday}):")
        print("="*50)
        
        if 'market_data' in historical_data:
            market_info = historical_data['market_data']
            print(f"   Price: ${market_info['current_price']['usd']:,.2f}")
            print(f"   Market Cap: ${market_info['market_cap']['usd']:,.0f}")
            print(f"   Volume: ${market_info['total_volume']['usd']:,.0f}")
            print(f"   Extracted: {historical_data['extraction_timestamp']}")
        
        # Save historical data
        hist_output_file = f"data/coingecko_historical_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(hist_output_file, 'w', encoding='utf-8') as f:
            json.dump(historical_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Historical data stored in: {hist_output_file}")
        
        print(f"\n✅ Extraction completed successfully!")
        print(f"📁 Generated files:")
        print(f"   - {output_file}")
        print(f"   - {hist_output_file}")
        
    except Exception as e:
        logger.error(f"Extraction error: {e}")
        raise

if __name__ == "__main__":
    main() 