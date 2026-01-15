-- ============================================
-- KPI Queries for Grafana Dashboard
-- ============================================
-- These queries provide key performance indicators
-- for the market analytics dashboard

-- KPI 1: Total Assets Tracked
-- Description: Count of distinct symbols being tracked
SELECT COUNT(DISTINCT symbol_id) AS total_assets
FROM market_data.dim_symbol;

-- KPI 2: Total Data Points
-- Description: Total number of market data records
SELECT COUNT(*) AS total_data_points
FROM market_data.fact_market_data;

-- KPI 3: Latest Data Timestamp
-- Description: Most recent data ingestion timestamp
SELECT MAX(timestamp) AS latest_timestamp
FROM market_data.fact_market_data;

-- KPI 4: Assets by Type
-- Description: Count of assets grouped by asset type
SELECT 
    asset_type,
    COUNT(*) AS asset_count
FROM market_data.dim_symbol
GROUP BY asset_type
ORDER BY asset_count DESC;

-- KPI 5: Data Coverage (Last 24 Hours)
-- Description: Number of assets with data in the last 24 hours
SELECT COUNT(DISTINCT symbol_id) AS assets_with_recent_data
FROM market_data.fact_market_data
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS;

-- KPI 6: Average Daily Volume (Last 7 Days)
-- Description: Average trading volume across all assets
SELECT 
    AVG(volume) AS avg_daily_volume
FROM market_data.fact_market_data
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND volume IS NOT NULL;

-- KPI 7: Total Market Cap
-- Description: Sum of market capitalization for all assets
SELECT 
    SUM(market_cap) AS total_market_cap
FROM market_data.fact_market_data
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
  AND market_cap IS NOT NULL;

-- KPI 8: Assets by Exchange
-- Description: Distribution of assets across exchanges
SELECT 
    COALESCE(exchange, 'Unknown') AS exchange,
    COUNT(*) AS asset_count
FROM market_data.dim_symbol
GROUP BY exchange
ORDER BY asset_count DESC;
