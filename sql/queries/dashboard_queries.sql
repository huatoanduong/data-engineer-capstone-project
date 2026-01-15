-- ============================================
-- Dashboard Queries for Grafana
-- ============================================
-- Comprehensive queries for all dashboard panels
-- This file consolidates queries from other query files

-- ============================================
-- PRICE TRENDS QUERIES
-- ============================================

-- Price Trend 1: Daily Average Close Price (Last 30 Days)
-- Description: Average closing price per day for trend visualization
SELECT 
    d.date,
    s.symbol,
    s.asset_type,
    AVG(f.close_price) AS avg_close_price,
    MIN(f.low_price) AS min_price,
    MAX(f.high_price) AS max_price,
    AVG(f.volume) AS avg_volume
FROM market_data.fact_market_data f
JOIN market_data.dim_date d ON f.date_id = d.date_id
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE d.date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND f.close_price IS NOT NULL
GROUP BY d.date, s.symbol, s.asset_type
ORDER BY d.date DESC, s.symbol;

-- Price Trend 2: Hourly Price Movement (Last 24 Hours)
-- Description: Price changes on an hourly basis
SELECT 
    DATE_TRUNC('hour', f.timestamp) AS hour,
    s.symbol,
    AVG(f.close_price) AS avg_close_price,
    AVG(f.hourly_change_pct) AS avg_hourly_change_pct,
    SUM(f.volume) AS total_volume
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
  AND f.close_price IS NOT NULL
GROUP BY DATE_TRUNC('hour', f.timestamp), s.symbol
ORDER BY hour DESC, s.symbol;

-- Price Trend 3: Weekly Performance Summary
-- Description: Weekly aggregated performance metrics
SELECT 
    d.year,
    d.week,
    s.symbol,
    AVG(f.close_price) AS avg_close_price,
    AVG(f.weekly_change_pct) AS avg_weekly_change_pct,
    SUM(f.volume) AS total_volume,
    AVG(f.market_cap) AS avg_market_cap
FROM market_data.fact_market_data f
JOIN market_data.dim_date d ON f.date_id = d.date_id
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE d.date >= CURRENT_DATE - INTERVAL 12 WEEKS
  AND f.close_price IS NOT NULL
GROUP BY d.year, d.week, s.symbol
ORDER BY d.year DESC, d.week DESC, s.symbol;

-- ============================================
-- PERFORMANCE ANALYSIS QUERIES
-- ============================================

-- Performance 1: Top Performers (Last 24 Hours)
-- Description: Assets with highest positive change
SELECT 
    s.symbol,
    s.asset_type,
    f.close_price,
    f.daily_change_pct,
    f.volume,
    f.market_cap,
    f.timestamp
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
  AND f.daily_change_pct IS NOT NULL
ORDER BY f.daily_change_pct DESC
LIMIT 20;

-- Performance 2: Worst Performers (Last 24 Hours)
-- Description: Assets with highest negative change
SELECT 
    s.symbol,
    s.asset_type,
    f.close_price,
    f.daily_change_pct,
    f.volume,
    f.market_cap,
    f.timestamp
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
  AND f.daily_change_pct IS NOT NULL
ORDER BY f.daily_change_pct ASC
LIMIT 20;

-- Performance 3: Volatility Analysis
-- Description: Assets with highest price volatility
SELECT 
    s.symbol,
    s.asset_type,
    STDDEV(f.close_price) AS price_volatility,
    AVG(f.close_price) AS avg_price,
    MIN(f.low_price) AS min_price,
    MAX(f.high_price) AS max_price,
    COUNT(*) AS data_points
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND f.close_price IS NOT NULL
GROUP BY s.symbol, s.asset_type
HAVING COUNT(*) > 10
ORDER BY price_volatility DESC
LIMIT 20;

-- Performance 4: Volume Leaders
-- Description: Assets with highest trading volume
SELECT 
    s.symbol,
    s.asset_type,
    SUM(f.volume) AS total_volume,
    AVG(f.volume) AS avg_volume,
    MAX(f.volume) AS max_volume,
    COUNT(*) AS data_points
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
  AND f.volume IS NOT NULL
GROUP BY s.symbol, s.asset_type
ORDER BY total_volume DESC
LIMIT 20;

-- ============================================
-- MARKET CAP TRENDS QUERIES
-- ============================================

-- Market Cap 1: Market Cap Trends Over Time
-- Description: Market capitalization trends by asset
SELECT 
    d.date,
    s.symbol,
    AVG(f.market_cap) AS avg_market_cap,
    MIN(f.market_cap) AS min_market_cap,
    MAX(f.market_cap) AS max_market_cap
FROM market_data.fact_market_data f
JOIN market_data.dim_date d ON f.date_id = d.date_id
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE d.date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND f.market_cap IS NOT NULL
GROUP BY d.date, s.symbol
ORDER BY d.date DESC, avg_market_cap DESC;

-- Market Cap 2: Market Cap Distribution by Asset Type
-- Description: Total market cap grouped by asset type
SELECT 
    s.asset_type,
    SUM(f.market_cap) AS total_market_cap,
    AVG(f.market_cap) AS avg_market_cap,
    COUNT(DISTINCT s.symbol_id) AS asset_count
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
  AND f.market_cap IS NOT NULL
GROUP BY s.asset_type
ORDER BY total_market_cap DESC;

-- Market Cap 3: Largest Market Cap Assets
-- Description: Top assets by current market capitalization
SELECT 
    s.symbol,
    s.asset_type,
    f.market_cap,
    f.close_price,
    f.timestamp
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp = (
    SELECT MAX(timestamp) 
    FROM market_data.fact_market_data 
    WHERE symbol_id = f.symbol_id
)
AND f.market_cap IS NOT NULL
ORDER BY f.market_cap DESC
LIMIT 20;

-- ============================================
-- COMBINED DASHBOARD QUERIES
-- ============================================

-- Combined Query 1: Asset Overview
-- Description: Comprehensive view of all assets with key metrics
SELECT 
    s.symbol,
    s.asset_type,
    s.sector,
    s.exchange,
    f.close_price,
    f.daily_change_pct,
    f.weekly_change_pct,
    f.rsi,
    f.macd,
    f.volume,
    f.market_cap,
    f.timestamp
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp = (
    SELECT MAX(timestamp) 
    FROM market_data.fact_market_data 
    WHERE symbol_id = f.symbol_id
)
AND f.close_price IS NOT NULL
ORDER BY s.symbol;

-- Combined Query 2: Time Series Data for Multiple Assets
-- Description: Time series data for selected symbols (for comparison)
SELECT 
    f.timestamp,
    s.symbol,
    f.close_price,
    f.volume,
    f.daily_change_pct,
    f.rsi
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND s.symbol IN (${symbol_list})  -- Grafana variable
  AND f.close_price IS NOT NULL
ORDER BY f.timestamp DESC, s.symbol;

-- Combined Query 3: Asset Health Score
-- Description: Composite health score based on multiple indicators
SELECT 
    s.symbol,
    s.asset_type,
    f.close_price,
    f.daily_change_pct,
    f.rsi,
    CASE 
        WHEN f.rsi < 30 THEN 'Oversold'
        WHEN f.rsi > 70 THEN 'Overbought'
        ELSE 'Normal'
    END AS rsi_status,
    CASE 
        WHEN f.macd > f.macd_signal THEN 'Bullish'
        WHEN f.macd < f.macd_signal THEN 'Bearish'
        ELSE 'Neutral'
    END AS macd_signal,
    f.timestamp
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp = (
    SELECT MAX(timestamp) 
    FROM market_data.fact_market_data 
    WHERE symbol_id = f.symbol_id
)
AND f.close_price IS NOT NULL
ORDER BY s.symbol;
