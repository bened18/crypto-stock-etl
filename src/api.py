#!/usr/bin/env python3
"""
REST API with FastAPI to query cryptocurrency data
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, date
import os
import logging

# logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="CoinGecko ETL API",
    description="REST API for querying cryptocurrency data from CoinGecko",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'coingecko_etl'),
    'user': os.getenv('POSTGRES_USER', 'coingecko_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'coingecko_password')
}

# Create SQLAlchemy engine
connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(connection_string)

# Response models
class MarketDataResponse(BaseModel):
    coin_id: str
    symbol: str
    name: str
    current_price_usd: float
    market_cap_usd: int
    market_cap_rank: int
    total_volume_usd: int
    price_change_percentage_24h: float
    last_updated: str
    extraction_timestamp: str

class HistoricalDataResponse(BaseModel):
    coin_id: str
    symbol: str
    name: str
    price_usd: float
    price_eur: float
    price_btc: float
    market_cap_usd: int
    total_volume_usd: int
    extraction_timestamp: str

class StatsResponse(BaseModel):
    total_coins: int
    total_market_cap: int
    avg_price: float
    last_update: str

class ErrorResponse(BaseModel):
    error: str
    detail: str

@app.on_event("startup")
async def startup_event():
    """Evento de inicio de la aplicación"""
    logger.info("🚀 Starting CoinGecko ETL API")
    logger.info(f"📊 Connecting to PostgreSQL in {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM curated.market_data"))
            count = result.fetchone()[0]
            logger.info(f"✅ Successful connection. {count} records in market_data")
    except Exception as e:
        logger.error(f"❌ Error connecting to PostgreSQL: {e}")
        raise

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "CoinGecko ETL API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "running"
    }

@app.get("/health", response_model=Dict[str, Any])
async def health_check():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM curated.market_data"))
            market_count = result.fetchone()[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM curated.historical_data"))
            historical_count = result.fetchone()[0]
            
            return {
                "status": "healthy",
                "database": "connected",
                "market_data_records": market_count,
                "historical_data_records": historical_count,
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/v1/coins", response_model=List[MarketDataResponse])
async def get_all_coins(
    limit: Optional[int] = Query(100, ge=1, le=1000, description="Maximum number of records"),
    offset: Optional[int] = Query(0, ge=0, description="Number of records to skip"),
    sort_by: Optional[str] = Query("market_cap_rank", description="Sort field (market_cap_rank, current_price_usd, price_change_percentage_24h)"),
    order: Optional[str] = Query("asc", description="Order (asc, desc)")
):
    """Get all cryptocurrencies with pagination and sorting"""
    try:
        # Validate sorting parameters
        valid_sort_fields = ["market_cap_rank", "current_price_usd", "price_change_percentage_24h", "total_volume_usd"]
        if sort_by not in valid_sort_fields:
            raise HTTPException(status_code=400, detail=f"sort_by must be one of: {valid_sort_fields}")
        
        if order not in ["asc", "desc"]:
            raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")
        
        # Build query
        query = f"""
            SELECT 
                coin_id, symbol, name, current_price_usd, market_cap_usd, 
                market_cap_rank, total_volume_usd, price_change_percentage_24h,
                last_updated, extraction_timestamp
            FROM curated.market_data
            ORDER BY {sort_by} {order}
            LIMIT {limit} OFFSET {offset}
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            
            return [
                {
                    "coin_id": row[0],
                    "symbol": row[1],
                    "name": row[2],
                    "current_price_usd": float(row[3]),
                    "market_cap_usd": int(row[4]),
                    "market_cap_rank": int(row[5]),
                    "total_volume_usd": int(row[6]),
                    "price_change_percentage_24h": float(row[7]),
                    "last_updated": str(row[8]),
                    "extraction_timestamp": str(row[9])
                }
                for row in rows
            ]
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error obtaining cryptocurrencies: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/v1/coins/{coin_id}", response_model=MarketDataResponse)
async def get_coin_by_id(coin_id: str):
    """Get a specific cryptocurrency by ID"""
    try:
        query = """
            SELECT 
                coin_id, symbol, name, current_price_usd, market_cap_usd, 
                market_cap_rank, total_volume_usd, price_change_percentage_24h,
                last_updated, extraction_timestamp
            FROM curated.market_data
            WHERE coin_id = :coin_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"coin_id": coin_id})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail=f"Cryptocurrency '{coin_id}' not found")
            
            return {
                "coin_id": row[0],
                "symbol": row[1],
                "name": row[2],
                "current_price_usd": float(row[3]),
                "market_cap_usd": int(row[4]),
                "market_cap_rank": int(row[5]),
                "total_volume_usd": int(row[6]),
                "price_change_percentage_24h": float(row[7]),
                "last_updated": str(row[8]),
                "extraction_timestamp": str(row[9])
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cryptocurrency {coin_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/v1/coins/{coin_id}/historical", response_model=HistoricalDataResponse)
async def get_coin_historical(coin_id: str):
    """Get historical data for a specific cryptocurrency"""
    try:
        query = """
            SELECT 
                coin_id, symbol, name, price_usd, price_eur, price_btc,
                market_cap_usd, total_volume_usd, extraction_timestamp
            FROM curated.historical_data
            WHERE coin_id = :coin_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"coin_id": coin_id})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail=f"Historical data for '{coin_id}' not found")
            
            return {
                "coin_id": row[0],
                "symbol": row[1],
                "name": row[2],
                "price_usd": float(row[3]),
                "price_eur": float(row[4]),
                "price_btc": float(row[5]),
                "market_cap_usd": int(row[6]),
                "total_volume_usd": int(row[7]),
                "extraction_timestamp": str(row[8])
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting historical data from {coin_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/v1/top-gainers", response_model=List[MarketDataResponse])
async def get_top_gainers(
    limit: Optional[int] = Query(10, ge=1, le=50, description="Number of cryptocurrencies")
):
    """Get the highest-earning cryptocurrencies in 24 hours"""
    try:
        query = f"""
            SELECT 
                coin_id, symbol, name, current_price_usd, market_cap_usd, 
                market_cap_rank, total_volume_usd, price_change_percentage_24h,
                last_updated, extraction_timestamp
            FROM curated.market_data
            WHERE price_change_percentage_24h > 0
            ORDER BY price_change_percentage_24h DESC
            LIMIT {limit}
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            
            return [
                {
                    "coin_id": row[0],
                    "symbol": row[1],
                    "name": row[2],
                    "current_price_usd": float(row[3]),
                    "market_cap_usd": int(row[4]),
                    "market_cap_rank": int(row[5]),
                    "total_volume_usd": int(row[6]),
                    "price_change_percentage_24h": float(row[7]),
                    "last_updated": str(row[8]),
                    "extraction_timestamp": str(row[9])
                }
                for row in rows
            ]
            
    except Exception as e:
        logger.error(f"Error getting top gainers: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/v1/top-losers", response_model=List[MarketDataResponse])
async def get_top_losers(
    limit: Optional[int] = Query(10, ge=1, le=50, description="Number of cryptocurrencies")
):
    """Get the cryptocurrencies with the biggest loss in 24 hours"""
    try:
        query = f"""
            SELECT 
                coin_id, symbol, name, current_price_usd, market_cap_usd, 
                market_cap_rank, total_volume_usd, price_change_percentage_24h,
                last_updated, extraction_timestamp
            FROM curated.market_data
            WHERE price_change_percentage_24h < 0
            ORDER BY price_change_percentage_24h ASC
            LIMIT {limit}
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            
            return [
                {
                    "coin_id": row[0],
                    "symbol": row[1],
                    "name": row[2],
                    "current_price_usd": float(row[3]),
                    "market_cap_usd": int(row[4]),
                    "market_cap_rank": int(row[5]),
                    "total_volume_usd": int(row[6]),
                    "price_change_percentage_24h": float(row[7]),
                    "last_updated": str(row[8]),
                    "extraction_timestamp": str(row[9])
                }
                for row in rows
            ]
            
    except Exception as e:
        logger.error(f"Error getting top losers: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/v1/stats", response_model=StatsResponse)
async def get_stats():
    """Obtain general market statistics"""
    try:
        query = """
            SELECT 
                COUNT(*) as total_coins,
                SUM(market_cap_usd) as total_market_cap,
                AVG(current_price_usd) as avg_price,
                MAX(extraction_timestamp) as last_update
            FROM curated.market_data
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            row = result.fetchone()
            
            return {
                "total_coins": int(row[0]),
                "total_market_cap": int(row[1]) if row[1] else 0,
                "avg_price": float(row[2]) if row[2] else 0.0,
                "last_update": str(row[3]) if row[3] else ""
            }
            
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/v1/search", response_model=List[MarketDataResponse])
async def search_coins(
    q: str = Query(..., min_length=1, description="Search term"),
    limit: Optional[int] = Query(10, ge=1, le=50, description="Maximum number of results")
):
    """Search for cryptocurrencies by name or symbol"""
    try:
        query = f"""
            SELECT 
                coin_id, symbol, name, current_price_usd, market_cap_usd, 
                market_cap_rank, total_volume_usd, price_change_percentage_24h,
                last_updated, extraction_timestamp
            FROM curated.market_data
            WHERE 
                LOWER(name) LIKE LOWER(:search_term) OR 
                LOWER(symbol) LIKE LOWER(:search_term) OR
                LOWER(coin_id) LIKE LOWER(:search_term)
            ORDER BY market_cap_rank ASC
            LIMIT {limit}
        """
        
        search_term = f"%{q}%"
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"search_term": search_term})
            rows = result.fetchall()
            
            return [
                {
                    "coin_id": row[0],
                    "symbol": row[1],
                    "name": row[2],
                    "current_price_usd": float(row[3]),
                    "market_cap_usd": int(row[4]),
                    "market_cap_rank": int(row[5]),
                    "total_volume_usd": int(row[6]),
                    "price_change_percentage_24h": float(row[7]),
                    "last_updated": str(row[8]),
                    "extraction_timestamp": str(row[9])
                }
                for row in rows
            ]
            
    except Exception as e:
        logger.error(f"Error searching for cryptocurrencies: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 