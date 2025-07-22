-- ========================================
-- COINGECKO ETL DATABASE SCHEMA
-- Automatically generated 2025-07-22 04:48:10
-- ========================================

-- Create schematics
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;

-- ========================================
-- TABLE: Current market data
-- ========================================

CREATE TABLE curated.market_data (
    coin_id VARCHAR(255) NOT NULL PRIMARY KEY,
    symbol VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    current_price_usd DECIMAL(20,8) NOT NULL,
    high_24h_usd DECIMAL(20,8) NOT NULL,
    low_24h_usd DECIMAL(20,8) NOT NULL,
    market_cap_usd BIGINT NOT NULL,
    market_cap_rank INTEGER NOT NULL,
    fully_diluted_valuation_usd BIGINT NOT NULL,
    total_volume_usd BIGINT NOT NULL,
    price_change_24h_usd DECIMAL(20,8) NOT NULL,
    price_change_percentage_24h DECIMAL(20,8) NOT NULL,
    market_cap_change_24h_usd DECIMAL(20,8) NOT NULL,
    market_cap_change_percentage_24h DECIMAL(20,8) NOT NULL,
    circulating_supply DECIMAL(20,8) NOT NULL,
    total_supply DECIMAL(20,8) NOT NULL,
    max_supply DECIMAL(20,8),
    ath_usd DECIMAL(20,8) NOT NULL,
    ath_change_percentage DECIMAL(20,8) NOT NULL,
    ath_date VARCHAR(255) NOT NULL,
    atl_usd DECIMAL(20,8) NOT NULL,
    atl_change_percentage DECIMAL(20,8) NOT NULL,
    atl_date VARCHAR(255) NOT NULL,
    last_updated VARCHAR(255) NOT NULL,
    extraction_timestamp VARCHAR(255) NOT NULL,
    price_to_ath_ratio DECIMAL(20,8) NOT NULL,
    price_to_atl_ratio DECIMAL(20,8) NOT NULL,
    market_cap_to_volume_ratio DECIMAL(20,8) NOT NULL
);

-- Indexes to improve performance
CREATE INDEX idx_curated.market_data_extraction_timestamp ON curated.market_data (extraction_timestamp);
CREATE INDEX idx_curated.market_data_symbol ON curated.market_data (symbol);
CREATE INDEX idx_curated.market_data_market_cap_rank ON curated.market_data (market_cap_rank);

-- Comments on the table
COMMENT ON TABLE curated.market_data IS 'Current cryptocurrency market data from CoinGecko';
COMMENT ON COLUMN curated.market_data.coin_id IS 'Unique cryptocurrency identifier';
COMMENT ON COLUMN curated.market_data.current_price_usd IS 'Current price in USD';
COMMENT ON COLUMN curated.market_data.market_cap_usd IS 'Market cap in USD';
COMMENT ON COLUMN curated.market_data.extraction_timestamp IS 'Timestamp of when the data was extracted';

-- ========================================
-- TABLE: Historical data
-- ========================================

CREATE TABLE curated.historical_data (
    coin_id VARCHAR(255) NOT NULL PRIMARY KEY,
    symbol VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    price_usd DECIMAL(20,8) NOT NULL,
    price_eur DECIMAL(20,8) NOT NULL,
    price_btc INTEGER NOT NULL,
    price_eth DECIMAL(20,8) NOT NULL,
    market_cap_usd BIGINT NOT NULL,
    market_cap_eur BIGINT NOT NULL,
    market_cap_btc INTEGER NOT NULL,
    total_volume_usd BIGINT NOT NULL,
    total_volume_eur BIGINT NOT NULL,
    total_volume_btc INTEGER NOT NULL,
    extraction_timestamp VARCHAR(255) NOT NULL
);

-- Indexes to improve performance
CREATE INDEX idx_curated.historical_data_extraction_timestamp ON curated.historical_data (extraction_timestamp);
CREATE INDEX idx_curated.historical_data_symbol ON curated.historical_data (symbol);

-- Comments on the table
COMMENT ON TABLE curated.historical_data IS 'Historical cryptocurrency data from CoinGecko';
COMMENT ON COLUMN curated.historical_data.coin_id IS 'Unique cryptocurrency identifier';
COMMENT ON COLUMN curated.historical_data.price_usd IS 'Price in USD for the historical date';

-- ========================================
-- USEFUL VIEWS
-- ========================================

-- View of top 10 by market cap
CREATE OR REPLACE VIEW curated.top_market_cap AS
SELECT coin_id, symbol, name, current_price_usd, market_cap_usd, market_cap_rank
FROM curated.market_data
WHERE market_cap_rank <= 10
ORDER BY market_cap_rank;

-- 24-hour overview of the most profitable cryptocurrencies
CREATE OR REPLACE VIEW curated.top_gainers_24h AS
SELECT coin_id, symbol, name, current_price_usd, price_change_percentage_24h
FROM curated.market_data
WHERE price_change_percentage_24h > 0
ORDER BY price_change_percentage_24h DESC
LIMIT 10;

-- Overview of cryptocurrencies with the biggest loss in the last 24 hours
CREATE OR REPLACE VIEW curated.top_losers_24h AS
SELECT coin_id, symbol, name, current_price_usd, price_change_percentage_24h
FROM curated.market_data
WHERE price_change_percentage_24h < 0
ORDER BY price_change_percentage_24h ASC
LIMIT 10;
