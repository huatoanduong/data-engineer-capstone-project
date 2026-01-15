-- ============================================
-- Technical Indicator Queries for Grafana Dashboard
-- ============================================
-- These queries retrieve technical indicators for analysis

-- Technical Indicator 1: RSI by Symbol (Latest)
-- Description: Relative Strength Index for each symbol
SELECT 
    s.symbol,
    s.asset_type,
    f.rsi,
    f.timestamp,
    f.close_price
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
  AND f.rsi IS NOT NULL
ORDER BY f.timestamp DESC, s.symbol;

-- Technical Indicator 2: MACD Signal Crossovers
-- Description: MACD and signal line values for trend analysis
SELECT 
    s.symbol,
    f.timestamp,
    f.macd,
    f.macd_signal,
    (f.macd - f.macd_signal) AS macd_histogram,
    f.close_price
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND f.macd IS NOT NULL
  AND f.macd_signal IS NOT NULL
ORDER BY f.timestamp DESC, s.symbol;

-- Technical Indicator 3: Bollinger Bands
-- Description: Upper and lower Bollinger Bands with price
SELECT 
    s.symbol,
    f.timestamp,
    f.close_price,
    f.bollinger_upper,
    f.bollinger_lower,
    (f.bollinger_upper - f.bollinger_lower) AS band_width,
    CASE 
        WHEN f.close_price > f.bollinger_upper THEN 'Above Upper'
        WHEN f.close_price < f.bollinger_lower THEN 'Below Lower'
        ELSE 'Within Bands'
    END AS band_position
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND f.bollinger_upper IS NOT NULL
  AND f.bollinger_lower IS NOT NULL
ORDER BY f.timestamp DESC, s.symbol;

-- Technical Indicator 4: Moving Averages Comparison
-- Description: Compare different SMA periods
SELECT 
    s.symbol,
    f.timestamp,
    f.close_price,
    f.sma_7,
    f.sma_14,
    f.sma_30,
    f.sma_50,
    f.sma_200,
    CASE 
        WHEN f.close_price > f.sma_50 THEN 'Above 50 SMA'
        ELSE 'Below 50 SMA'
    END AS trend_50_sma
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS
  AND f.sma_50 IS NOT NULL
ORDER BY f.timestamp DESC, s.symbol;

-- Technical Indicator 5: RSI Distribution
-- Description: Count of assets in different RSI ranges
SELECT 
    CASE 
        WHEN rsi < 30 THEN 'Oversold (<30)'
        WHEN rsi BETWEEN 30 AND 70 THEN 'Neutral (30-70)'
        WHEN rsi > 70 THEN 'Overbought (>70)'
        ELSE 'No Data'
    END AS rsi_category,
    COUNT(DISTINCT symbol_id) AS asset_count
FROM market_data.fact_market_data
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
  AND rsi IS NOT NULL
GROUP BY rsi_category
ORDER BY rsi_category;

-- Technical Indicator 6: MACD Bullish/Bearish Signals
-- Description: Count of bullish (MACD > Signal) vs bearish signals
SELECT 
    CASE 
        WHEN macd > macd_signal THEN 'Bullish'
        WHEN macd < macd_signal THEN 'Bearish'
        ELSE 'Neutral'
    END AS macd_signal_type,
    COUNT(DISTINCT symbol_id) AS asset_count
FROM market_data.fact_market_data
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
  AND macd IS NOT NULL
  AND macd_signal IS NOT NULL
GROUP BY macd_signal_type
ORDER BY macd_signal_type;

-- Technical Indicator 7: Price vs Moving Averages (Latest)
-- Description: Current price position relative to moving averages
SELECT 
    s.symbol,
    s.asset_type,
    f.close_price,
    f.sma_7,
    f.sma_50,
    f.sma_200,
    ((f.close_price - f.sma_50) / f.sma_50 * 100) AS pct_from_50_sma,
    ((f.close_price - f.sma_200) / f.sma_200 * 100) AS pct_from_200_sma,
    f.timestamp
FROM market_data.fact_market_data f
JOIN market_data.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE f.timestamp = (
    SELECT MAX(timestamp) 
    FROM market_data.fact_market_data 
    WHERE symbol_id = f.symbol_id
)
AND f.sma_50 IS NOT NULL
AND f.sma_200 IS NOT NULL
ORDER BY s.symbol;
