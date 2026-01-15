-- ============================================
-- Correlation Analysis Queries for Grafana Dashboard
-- ============================================
-- These queries analyze correlations between assets

-- Correlation Query 1: Top Correlated Pairs
-- Description: Most highly correlated asset pairs
SELECT 
    s1.symbol AS symbol_1,
    s2.symbol AS symbol_2,
    c.correlation_value,
    c.period,
    c.calculated_at
FROM market_data.dim_correlation c
JOIN market_data.dim_symbol s1 ON c.symbol_id_1 = s1.symbol_id
JOIN market_data.dim_symbol s2 ON c.symbol_id_2 = s2.symbol_id
WHERE c.calculated_at >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND c.correlation_value IS NOT NULL
ORDER BY ABS(c.correlation_value) DESC
LIMIT 50;

-- Correlation Query 2: Correlation by Asset Type
-- Description: Average correlation within and between asset types
SELECT 
    s1.asset_type AS asset_type_1,
    s2.asset_type AS asset_type_2,
    AVG(c.correlation_value) AS avg_correlation,
    COUNT(*) AS pair_count
FROM market_data.dim_correlation c
JOIN market_data.dim_symbol s1 ON c.symbol_id_1 = s1.symbol_id
JOIN market_data.dim_symbol s2 ON c.symbol_id_2 = s2.symbol_id
WHERE c.calculated_at >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
GROUP BY s1.asset_type, s2.asset_type
ORDER BY avg_correlation DESC;

-- Correlation Query 3: Correlation Distribution
-- Description: Distribution of correlation values
SELECT 
    CASE 
        WHEN correlation_value >= 0.7 THEN 'Strong Positive (≥0.7)'
        WHEN correlation_value BETWEEN 0.3 AND 0.7 THEN 'Moderate Positive (0.3-0.7)'
        WHEN correlation_value BETWEEN -0.3 AND 0.3 THEN 'Weak (-0.3 to 0.3)'
        WHEN correlation_value BETWEEN -0.7 AND -0.3 THEN 'Moderate Negative (-0.7 to -0.3)'
        WHEN correlation_value < -0.7 THEN 'Strong Negative (<-0.7)'
    END AS correlation_category,
    COUNT(*) AS pair_count
FROM market_data.dim_correlation
WHERE calculated_at >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
GROUP BY correlation_category
ORDER BY correlation_category;

-- Correlation Query 4: Correlation Over Time
-- Description: Correlation trends for specific pairs
SELECT 
    s1.symbol AS symbol_1,
    s2.symbol AS symbol_2,
    c.correlation_value,
    DATE(c.calculated_at) AS calculation_date,
    c.period
FROM market_data.dim_correlation c
JOIN market_data.dim_symbol s1 ON c.symbol_id_1 = s1.symbol_id
JOIN market_data.dim_symbol s2 ON c.symbol_id_2 = s2.symbol_id
WHERE c.calculated_at >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS
  AND s1.symbol = '${symbol_1}'  -- Grafana variable
  AND s2.symbol = '${symbol_2}'  -- Grafana variable
ORDER BY c.calculated_at DESC;

-- Correlation Query 5: Most Volatile Correlations
-- Description: Pairs with highest correlation changes
WITH correlation_changes AS (
    SELECT 
        symbol_id_1,
        symbol_id_2,
        correlation_value,
        calculated_at,
        LAG(correlation_value) OVER (
            PARTITION BY symbol_id_1, symbol_id_2 
            ORDER BY calculated_at
        ) AS prev_correlation,
        ABS(correlation_value - LAG(correlation_value) OVER (
            PARTITION BY symbol_id_1, symbol_id_2 
            ORDER BY calculated_at
        )) AS correlation_change
    FROM market_data.dim_correlation
    WHERE calculated_at >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
)
SELECT 
    s1.symbol AS symbol_1,
    s2.symbol AS symbol_2,
    cc.correlation_value,
    cc.prev_correlation,
    cc.correlation_change,
    cc.calculated_at
FROM correlation_changes cc
JOIN market_data.dim_symbol s1 ON cc.symbol_id_1 = s1.symbol_id
JOIN market_data.dim_symbol s2 ON cc.symbol_id_2 = s2.symbol_id
WHERE cc.correlation_change IS NOT NULL
ORDER BY cc.correlation_change DESC
LIMIT 20;

-- Correlation Query 6: Asset Correlation Matrix (Sample)
-- Description: Correlation matrix for top assets
SELECT 
    s1.symbol AS symbol_1,
    s2.symbol AS symbol_2,
    c.correlation_value
FROM market_data.dim_correlation c
JOIN market_data.dim_symbol s1 ON c.symbol_id_1 = s1.symbol_id
JOIN market_data.dim_symbol s2 ON c.symbol_id_2 = s2.symbol_id
WHERE c.calculated_at = (
    SELECT MAX(calculated_at) 
    FROM market_data.dim_correlation
)
AND s1.symbol IN (${symbol_list})  -- Grafana variable for symbol list
AND s2.symbol IN (${symbol_list})
ORDER BY s1.symbol, s2.symbol;
