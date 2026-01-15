-- Databricks Delta Lake Star Schema
-- Fact and Dimension Tables for Market Data Warehouse

-- ============================================
-- FACT TABLE: fact_market_data
-- ============================================
CREATE TABLE IF NOT EXISTS fact_market_data (
    market_data_id BIGINT GENERATED ALWAYS AS IDENTITY,
    symbol_id INT NOT NULL,
    date_id INT NOT NULL,
    time_id INT NOT NULL,
    open_price DECIMAL(18, 4),
    high_price DECIMAL(18, 4),
    low_price DECIMAL(18, 4),
    close_price DECIMAL(18, 4),
    volume BIGINT,
    market_cap BIGINT,
    change_pct DECIMAL(10, 4),
    hourly_change_pct DECIMAL(10, 4),
    daily_change_pct DECIMAL(10, 4),
    weekly_change_pct DECIMAL(10, 4),
    rsi DECIMAL(10, 4),
    macd DECIMAL(18, 4),
    macd_signal DECIMAL(18, 4),
    bollinger_upper DECIMAL(18, 4),
    bollinger_lower DECIMAL(18, 4),
    sma_7 DECIMAL(18, 4),
    sma_14 DECIMAL(18, 4),
    sma_30 DECIMAL(18, 4),
    sma_50 DECIMAL(18, 4),
    sma_200 DECIMAL(18, 4),
    timestamp TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP,
    year INT,
    month INT,
    day INT,
    hour INT
)
USING DELTA
PARTITIONED BY (year, month, day, symbol_id)
LOCATION 's3://{bucket}/delta/fact_market_data/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================
-- DIMENSION TABLE: dim_symbol
-- ============================================
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_id INT NOT NULL,
    symbol STRING NOT NULL,
    name STRING,
    asset_type STRING NOT NULL,
    sector STRING,
    exchange STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
LOCATION 's3://{bucket}/delta/dim_symbol/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================
-- DIMENSION TABLE: dim_date
-- ============================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT NOT NULL,
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name STRING,
    month_name STRING,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
)
USING DELTA
LOCATION 's3://{bucket}/delta/dim_date/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================
-- DIMENSION TABLE: dim_time
-- ============================================
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INT NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL,
    second INT NOT NULL,
    time_of_day STRING,
    period_of_day STRING
)
USING DELTA
LOCATION 's3://{bucket}/delta/dim_time/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================
-- DIMENSION TABLE: dim_correlation
-- ============================================
CREATE TABLE IF NOT EXISTS dim_correlation (
    correlation_id BIGINT GENERATED ALWAYS AS IDENTITY,
    symbol_id_1 INT NOT NULL,
    symbol_id_2 INT NOT NULL,
    correlation_value DECIMAL(10, 6) NOT NULL,
    period STRING NOT NULL,
    calculated_at TIMESTAMP NOT NULL,
    year INT,
    month INT,
    day INT
)
USING DELTA
PARTITIONED BY (year, month, day)
LOCATION 's3://{bucket}/delta/dim_correlation/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================
-- INDEXES AND CONSTRAINTS
-- ============================================

-- Primary key constraints (Delta Lake doesn't enforce but documents intent)
-- ALTER TABLE dim_symbol ADD CONSTRAINT pk_dim_symbol PRIMARY KEY (symbol_id);
-- ALTER TABLE dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_id);
-- ALTER TABLE dim_time ADD CONSTRAINT pk_dim_time PRIMARY KEY (time_id);

-- Foreign key constraints (Delta Lake doesn't enforce but documents intent)
-- ALTER TABLE fact_market_data ADD CONSTRAINT fk_symbol FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id);
-- ALTER TABLE fact_market_data ADD CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id);
-- ALTER TABLE fact_market_data ADD CONSTRAINT fk_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id);

-- ============================================
-- OPTIMIZATION COMMANDS
-- ============================================

-- Z-order optimization for fact table (improves query performance)
-- OPTIMIZE fact_market_data ZORDER BY (symbol_id, timestamp);

-- Compaction (run periodically)
-- OPTIMIZE fact_market_data;

-- Vacuum old files (retention period in hours, default 168 = 7 days)
-- VACUUM fact_market_data RETAIN 168 HOURS;
