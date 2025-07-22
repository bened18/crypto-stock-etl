# Crypto Stock ETL - POC

## Design Document 
- https://docs.google.com/document/d/139srkycJ-y8u8Pm-PnxoNGFAUT97aEK2XK_gHn7vuno/edit?usp=sharing
- Docs/Data Ops Technical Challenge - Solution - Crypto Stock ETL

Minimum viable solution for mining cryptocurrency data using the CoinGecko API.

## 🚀 Quick Start

### Prerequisites
- Docker
- Docker Compose

### Run Full Pipeline

1. **Start services:**
```bash
docker-compose up -d postgres api
```

2. **Run full ETL pipeline:**
```bash
docker-compose run --rm coingecko-extractor python src/run_full_pipeline.py
```

### Access the API

- **API Base URL**: http://localhost:8000
- **Documentation Swagger**: http://localhost:8000/docs
- **Documentation ReDoc**: http://localhost:8000/redoc

### Run with Docker

1. **Build and run:**
```bash
docker-compose up --build
```

2. **Run in detached mode:**
```bash
docker-compose up -d --build
```

3. **View logs:**
```bash
docker-compose logs -f
```

### Run locally (without Docker)

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Run script:**
```bash
python src/extract_coingecko.py
```

## 📁 Project Structure

```
.
├── src/
│   ├── extract_coingecko.py       # Extracting data from CoinGecko API
│   ├── transform_data.py          # Data transformation and cleansing
│   ├── generate_schema.py         # PostgreSQL Schema Generation
│   ├── load_to_postgres.py        # Loading data into PostgreSQL
│   ├── run_full_pipeline.py       # Complete ETL Pipeline
│   └── api.py                     # REST API with FastAPI
├── data/                          # Directory for generated files
├── requirements.txt               # Python Dependencies
├── Dockerfile                     # Docker configuration
├── docker-compose.yml             # Docker Orchestration
└── README.md                      # This file
```

## 🔧 Features

### ✅ Data Extraction
- **Market Data**: Current prices, market cap, volume, etc.
- **Historical Data**: Specific date information
- **Rate Limiting**: Automatic API limit management
- **Logging**: Detailed transaction log

### ✅ Data Transformation
- **Data Cleaning**: Type conversion, handling of null values
- **Structuring**: Normalized and calculated fields
- **Validation**: Data integrity verification
- **Calculated Fields**: Price ratios, market cap, etc.
- **DB Schema**: Automatic PostgreSQL schema generation

### ✅ Storage in PostgreSQL
- **Database**: PostgreSQL 15 containerized
- **Schemes**: staging and curated separately
- **Tables**: market_data and historical_data
- **Indexes**: Optimized for common queries
- **Complete Pipeline**: Automated end-to-end ETL

### ✅ REST API with FastAPI
- **FastAPI Server**: Complete and documented REST API
- **Endpoints**: 10+ endpoints to query data
- **Automatic Documentation**: Swagger/OpenAPI docs
- **Validation**: Pydantic models for requests/responses
- **CORS**: Configured for frontend development
- **Health Check**: Endpoint monitoring

### Supported Cryptocurrencies
- Bitcoin (BTC)
- Ethereum (ETH)
- Cardano (ADA)
- Solana (SOL)

## 🌐 API REST Endpoints

### General Information
- `GET /` - API information
- `GET /health` - Health check and DB statistics

### Cryptocurrencies
- `GET /v1/coins` - List all cryptocurrencies (with pagination and sorting)
- `GET /v1/coins/{coin_id}` - Get specific cryptocurrency
- `GET /v1/coins/{coin_id}/historical` - Historical data of a cryptocurrency

### Specialized Consultations
- `GET /v1/top-gainers` - Top cryptocurrencies with the highest 24-hour gains
- `GET /v1/top-losers` - Top cryptocurrencies with the biggest 24-hour loss
- `GET /v1/stats` - General market statistics
- `GET /v1/search` - Search by name, symbol or ID

### Query Parameters
- `limit`: Maximum number of records (1-1000)
- `offset`: Number of records to skip
- `sort_by`: Sort field (market_cap_rank, current_price_usd, price_change_percentage_24h, total_volume_usd)
- `order`: Order (asc, desc)
- `q`: Search term

### Examples of Use

```bash
# Get all cryptocurrencies
curl http://localhost:8000/v1/coins

# Get Specific Bitcoin
curl http://localhost:8000/v1/coins/bitcoin

# Top 5 gainers
curl http://localhost:8000/v1/top-gainers?limit=5

# Market statistics
curl http://localhost:8000/v1/stats

# Search by term
curl http://localhost:8000/v1/search?q=bitcoin
```

## 📊 Extracted Data

### Market Data Structure
Each cryptocurrency includes 28 transformed fields, including:
- **ID**: `coin_id`, `symbol`, `name`
- **Prices**: `current_price_usd`, `high_24h_usd`, `low_24h_usd`
- **Market Cap**: `market_cap_usd`, `market_cap_rank`
- **Volume**: `total_volume_usd`
- **Changes**: `price_change_24h_usd`, `price_change_percentage_24h`
- **Supply**: `circulating_supply`, `total_supply`, `max_supply`
- **Maximums/Minimums**: `ath_usd`, `atl_usd`, `ath_date`, `atl_date`
- **Calculated Fields**: `price_to_ath_ratio`, `price_to_atl_ratio`, `market_cap_to_volume_ratio`
- **Metadata**: `last_updated`, `extraction_timestamp`

### Historical Data Structure
Includes detailed information for specific dates:
- **Multi-currency pricing**: USD, EUR, BTC, ETH
- **Multi-currency Market Cap**: USD, EUR, BTC
- **Multi-currency Volumem**: USD, EUR, BTC

## 📈 Transformed Output Example

### Transformed Market Data
```csv
coin_id,symbol,name,current_price_usd,market_cap_usd,market_cap_rank,price_change_percentage_24h
bitcoin,BTC,Bitcoin,117988.0,2347229497016,1,-0.17
ethereum,ETH,Ethereum,3786.36,456909242422,2,1.10
solana,SOL,Solana,196.68,105822085063,6,8.78
cardano,ADA,Cardano,0.90,32563309770,10,4.90
```

## 🗄️ Database Schema

### Generated Tables
- **`curated.market_data`**: Current market data (28 fields)
- **`curated.historical_data`**: Historical data (14 fields)

### Useful Views
- **`curated.top_market_cap`**: Top 10 per market cap
- **`curated.top_gainers_24h`**: Biggest gain in 24h
- **`curated.top_losers_24h`**: Biggest loss in 24h

### Performance Indices
- Index on `extraction_timestamp` for temporal queries
- Index on `symbol` for cryptocurrency searches
- Index on `market_cap_rank` for sorting

## 🚀 Complete ETL Pipeline

### Single Command
```bash
docker-compose run --rm coingecko-extractor python src/run_full_pipeline.py
```

### Pipeline Steps
1. **Extraction**: Get data from CoinGecko API
2. **Transformation**: Clean and structure data
3. **Scheme**: Generate database schema
4. **Load**: Inserting data into PostgreSQL

### Execution Time
- **Typical duration**: ~1.5 seconds
- **Processed data**: 4 cryptocurrencies + historical data
- **Records generated**: 5 total records

## 🐛 Debugging

### View detailed logs
```bash
docker-compose logs -f
```

### Run with interactive shell
```bash
docker-compose run --rm coingecko-extractor /bin/bash
```

### View generated files
```bash
ls -la data/
```

### Transform data
```bash
docker-compose run --rm coingecko-extractor python src/transform_data.py
```

### Generate DB schema
```bash
docker-compose run --rm coingecko-extractor python src/generate_schema.py
```

### Loading data into PostgreSQL
```bash
docker-compose run --rm coingecko-extractor python src/load_to_postgres.py
```

## 📝 Next Steps

1. ✅ **Data extraction from CoinGecko** - COMPLETE
2. ✅ **Data transformation and cleansing** - COMPLETE
3. ✅ **Storage in PostgreSQL** - COMPLETE
4. ✅ **REST API with FastAPI** - COMPLETE
5. 🔄 **Orchestration with Airflow** - PENDING

## 🎯 Current Status

### ✅ COMPLETE
- [x] CoinGecko API extraction script
- [x] Containerization with Docker
- [x] Current market data extraction
- [x] Historical data extraction
- [x] Detailed logging
- [x] Error handling
- [x] Data transformation and cleansing
- [x] Automatic PostgreSQL schema generation
- [x] Calculated fields and validations
- [x] Useful views for queries
- [x] PostgreSQL configuration
- [x] Data loading scripts
- [x] Automated full ETL pipeline
- [x] Verification of loaded data
- [x] REST API with FastAPI
- [x] 10+ working endpoints
- [x] Automatic documentation (Swagger/ReDoc)
- [x] Validation with Pydantic
- [x] Health check and monitoring

