# Data Engineering Capstone Project Plan
## Stock & Crypto Market Data Pipeline

### Project Overview
Build an end-to-end data engineering pipeline for stock and cryptocurrency market data, demonstrating skills in data ingestion, ETL processing, data warehousing, and visualization.

---

## Architecture Overview

```
┌─────────────────┐
│  Yahoo Finance  │ (yfinance API)
│   (CoinGecko)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Data Ingestion │ (Python script with retry logic)
│   (Airflow)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   AWS S3        │ (Data Lake - Bronze Layer)
│  Partitioned:   │ year/month/day/hour/symbol
│  Format: Parquet│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Spark   │ (PySpark ETL)
│  Transformations│ Bronze → Silver → Gold
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Kafka   │ (Event streaming)
│  Topics:        │ stocks, crypto
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Databricks    │ (Data Warehouse)
│  Delta Lake     │ (Star Schema)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Grafana      │ (Dashboard)
│  Visualizations │
└─────────────────┘
```

---

## Technology Stack

- **Data Lake**: AWS S3
- **ETL Processing**: Apache Spark (PySpark)
- **Orchestration**: Apache Airflow
- **Message Queue**: Apache Kafka
- **Data Warehouse**: Databricks (Delta Lake)
- **Dashboard**: Grafana
- **Data Source**: Yahoo Finance (yfinance), extensible to CoinGecko
- **Development**: Docker-based local environment

---

## Project Structure

```
DECapstoneProject/
├── docker/
│   ├── docker-compose.yml          # All services orchestration
│   ├── airflow/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── spark/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── kafka/
│       └── docker-compose-kafka.yml
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── yahoo_finance_ingester.py
│   │   ├── coingecko_ingester.py    # Future extension
│   │   └── config.py                 # Configurable assets, cron
│   ├── transformation/
│   │   ├── __init__.py
│   │   ├── bronze_to_silver.py
│   │   ├── silver_to_gold.py
│   │   ├── technical_indicators.py  # RSI, MACD, Bollinger Bands
│   │   └── correlation_analysis.py
│   ├── warehouse/
│   │   ├── __init__.py
│   │   ├── kafka_producer.py
│   │   ├── databricks_loader.py
│   │   └── delta_schema.py
│   ├── quality/
│   │   ├── __init__.py
│   │   ├── validators.py
│   │   └── data_profiling.py
│   └── utils/
│       ├── __init__.py
│       ├── s3_utils.py
│       ├── retry_handler.py
│       └── logger.py
├── dags/
│   ├── data_ingestion_dag.py
│   ├── etl_pipeline_dag.py
│   └── warehouse_load_dag.py
├── sql/
│   ├── databricks_schema.sql
│   └── queries/
│       └── dashboard_queries.sql
├── grafana/
│   ├── dashboards/
│   │   └── market_analytics.json
│   └── datasources/
│       └── databricks_datasource.json
├── config/
│   ├── assets.yaml                  # Configurable asset list
│   ├── pipeline_config.yaml         # Cron, backfill settings
│   └── aws_config.yaml              # S3 credentials
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── docs/
│   ├── architecture.md
│   ├── setup_guide.md
│   └── api_documentation.md
├── scripts/
│   ├── setup.sh
│   ├── backfill_historical_data.py
│   └── validate_pipeline.py
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Implementation Phases

### Phase 1: Infrastructure Setup (Week 1-2)

#### 1.1 Docker Environment Setup
- Create `docker-compose.yml` with:
  - Airflow (with PostgreSQL backend)
  - Spark (standalone cluster)
  - Kafka + Zookeeper
  - LocalStack (for S3 simulation) OR configure AWS S3 access
- Set up networking between containers
- Configure environment variables

#### 1.2 AWS S3 Setup
- Create S3 bucket structure:
  ```
  s3://your-bucket/
  ├── bronze/
  │   ├── stocks/
  │   │   └── year=YYYY/month=MM/day=DD/hour=HH/symbol=SYMBOL/
  │   └── crypto/
  │       └── year=YYYY/month=MM/day=DD/hour=HH/symbol=SYMBOL/
  ├── silver/
  │   ├── stocks/
  │   └── crypto/
  └── gold/
      ├── stocks/
      └── crypto/
  ```
- Configure IAM roles and access credentials
- Set up lifecycle policies

#### 1.3 Databricks Setup
- Create Databricks workspace (community edition or trial)
- Set up cluster or SQL warehouse
- Configure connection credentials (personal access token)
- Set up Unity Catalog (optional) or default Hive metastore
- Configure S3 access for Delta Lake storage
- Create initial Delta tables (see schema design below)

#### 1.4 Kafka Setup
- Configure Kafka topics:
  - `market-data-stocks`
  - `market-data-crypto`
- Set up consumer groups
- Configure retention policies

---

### Phase 2: Data Ingestion (Week 2-3)

#### 2.1 Yahoo Finance Ingestion Script
**File**: `src/ingestion/yahoo_finance_ingester.py`

**Features**:
- Fetch data using yfinance library
- Support configurable asset list (from `config/assets.yaml`)
- Extract fields:
  - OHLCV (Open, High, Low, Close, Volume)
  - Market Cap
  - 24h Change %
  - P/E ratio (for stocks)
- Retry logic with exponential backoff
- Data validation (schema, nulls, ranges)
- Save to S3 Bronze layer in Parquet format
- Partition by: `year/month/day/hour/symbol`

**Configuration**:
- Asset list in `config/assets.yaml`
- Cron schedule in `config/pipeline_config.yaml`
- Historical backfill: configurable period (e.g., 1-3 months)

#### 2.2 Airflow DAG: Data Ingestion
**File**: `dags/data_ingestion_dag.py`

**Tasks**:
1. Fetch stock data
2. Fetch crypto data
3. Validate ingested data
4. Log ingestion metrics

**Schedule**: Configurable cron (default: hourly)

---

### Phase 3: ETL Pipeline (Week 3-5)

#### 3.1 Bronze to Silver Transformation
**File**: `src/transformation/bronze_to_silver.py`

**Transformations**:
- Data cleaning:
  - Remove duplicates
  - Handle null values
  - Standardize data types
  - Validate business rules (e.g., price > 0, volume >= 0)
- Schema standardization
- Data quality checks
- Save to S3 Silver layer

#### 3.2 Silver to Gold Transformation
**File**: `src/transformation/silver_to_gold.py`

**Transformations**:
- Calculate technical indicators:
  - **RSI (Relative Strength Index)**: 14-period
  - **MACD (Moving Average Convergence Divergence)**: 12, 26, 9 periods
  - **Bollinger Bands**: 20-period, 2 standard deviations
  - Moving averages (SMA, EMA): 7, 14, 30, 50, 200 days
- Calculate price changes:
  - Hourly change %
  - Daily change %
  - Weekly change %
- Correlation analysis:
  - Inter-asset correlations
  - Correlation matrices
- Aggregations:
  - Hourly summaries
  - Daily summaries
- Save to S3 Gold layer

**File**: `src/transformation/technical_indicators.py`
- Implement RSI calculation
- Implement MACD calculation
- Implement Bollinger Bands calculation
- Implement moving averages

**File**: `src/transformation/correlation_analysis.py`
- Calculate correlation matrices
- Store correlation data

#### 3.3 Airflow DAG: ETL Pipeline
**File**: `dags/etl_pipeline_dag.py`

**Tasks**:
1. Bronze to Silver transformation (Spark job)
2. Silver to Gold transformation (Spark job)
3. Data quality validation
4. Log transformation metrics

**Dependencies**: Runs after data ingestion

---

### Phase 4: Data Warehouse Loading (Week 5-6)

#### 4.1 Databricks Delta Lake Schema Design
**File**: `sql/databricks_schema.sql` and `src/warehouse/delta_schema.py`

**Delta Lake Star Schema Structure**:

**Fact Table**: `fact_market_data` (Delta table)
- Primary key: `market_data_id`
- Foreign keys: `symbol_id`, `date_id`, `time_id`
- Measures: `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `market_cap`, `change_pct`
- Technical indicators: `rsi`, `macd`, `macd_signal`, `bollinger_upper`, `bollinger_lower`, `sma_7`, `sma_14`, `sma_30`, `sma_50`, `sma_200`
- Partition columns: `date`, `symbol` (for query optimization)
- Delta table location: `s3://your-bucket/delta/fact_market_data/` or DBFS

**Dimension Tables** (Delta tables):
- `dim_symbol`: symbol_id, symbol, name, asset_type (stock/crypto), sector, exchange
- `dim_date`: date_id, date, year, month, day, quarter, week
- `dim_time`: time_id, hour, minute, market_hours_flag
- `dim_correlation`: correlation_id, symbol_1_id, symbol_2_id, correlation_value, period

**Sub-dimensions** (Star schema normalization):
- `dim_symbol_sector`: sector_id, sector_name, industry
- `dim_exchange`: exchange_id, exchange_name, country

**Delta Lake Features**:
- ACID transactions
- Time travel (versioning)
- Schema evolution
- Optimized file layout (Z-ordering, partitioning)
- Vacuum for data retention management

#### 4.2 Kafka Producer
**File**: `src/warehouse/kafka_producer.py`

**Features**:
- Read from S3 Gold layer
- Publish to Kafka topics:
  - `market-data-stocks`
  - `market-data-crypto`
- Message format: JSON with schema
- Error handling and retry logic

#### 4.3 Databricks Loader
**File**: `src/warehouse/databricks_loader.py`

**Features**:
- Consume from Kafka topics using Spark Structured Streaming
- Transform data for Delta Lake schema
- Load into Delta tables using `MERGE` operations (upserts)
- Leverage Delta Lake features:
  - ACID transactions
  - Schema evolution
  - Optimize and Z-order for query performance
- Error handling and dead letter queue
- Use Databricks SQL or PySpark for data loading

#### 4.4 Airflow DAG: Warehouse Loading
**File**: `dags/warehouse_load_dag.py`

**Tasks**:
1. Trigger Kafka producer (Spark job)
2. Load data to Databricks Delta Lake tables
3. Optimize Delta tables (Z-order, compact small files)
4. Validate warehouse data
5. Update dimension tables if needed
6. Run VACUUM on Delta tables (optional, for old versions)

**Dependencies**: Runs after ETL pipeline

**Note**: Can use Databricks Jobs API or run Spark jobs via Databricks Connect

---

### Phase 5: Data Quality & Validation (Week 6)

#### 5.1 Data Quality Framework
**File**: `src/quality/validators.py`

**Validations**:
- Schema validation (Pydantic models)
- Null checks
- Data type validation
- Business rule validation:
  - Price > 0
  - Volume >= 0
  - Market cap >= 0
  - RSI between 0-100
  - Valid date ranges
- Duplicate detection
- Range checks (e.g., price change % reasonable)

**File**: `src/quality/data_profiling.py`
- Generate data quality reports
- Track data quality metrics
- Alert on anomalies

---

### Phase 6: Dashboard Development (Week 7-8)

#### 6.1 Grafana Dashboard Configuration
**File**: `grafana/dashboards/market_analytics.json`

**Dashboard Panels**:

1. **Overview KPIs**:
   - Total assets tracked
   - Latest update time
   - Data freshness indicator

2. **Price Trends**:
   - Multi-asset price comparison (line chart)
   - Individual asset price history
   - Volume trends

3. **Technical Indicators**:
   - RSI heatmap (all assets)
   - MACD signals
   - Bollinger Bands visualization
   - Moving averages overlay

4. **Performance Analysis**:
   - Top gainers/losers (table)
   - Performance rankings
   - 24h/7d/30d change comparisons

5. **Correlation Analysis**:
   - Correlation heatmap (all assets)
   - Correlation matrix table
   - Asset relationship network

6. **Market Cap Trends**:
   - Market cap over time
   - Market cap distribution
   - Sector/type breakdown

7. **Filters**:
   - Date range selector
   - Asset type (stocks/crypto)
   - Individual asset selector
   - Time period selector

#### 6.2 Grafana Data Source
**File**: `grafana/datasources/databricks_datasource.json`
- Configure Databricks SQL Warehouse connection
- Use Databricks SQL connector or JDBC/ODBC connection
- Set up query templates for Delta Lake tables
- Alternative: Use Grafana's PostgreSQL/MySQL connector if using Databricks SQL endpoint

#### 6.3 SQL Queries
**File**: `sql/queries/dashboard_queries.sql`
- Pre-written queries for each dashboard panel
- Optimized for performance

---

### Phase 7: Error Handling & Monitoring (Week 8-9)

#### 7.1 Retry Logic
**File**: `src/utils/retry_handler.py`

**Features**:
- Exponential backoff retry
- Configurable max retries
- Retry for specific exceptions (API errors, network issues)
- Logging of retry attempts

#### 7.2 Error Handling
- Try-catch blocks in all components
- Error logging to files/CloudWatch
- Dead letter queue for failed records
- Alert notifications (optional: email/Slack)

#### 7.3 Monitoring
- Airflow task monitoring
- Spark job monitoring
- Kafka consumer lag monitoring
- Data quality metrics tracking
- Pipeline health checks

---

### Phase 8: Historical Data Backfill (Week 9)

#### 8.1 Backfill Script
**File**: `scripts/backfill_historical_data.py`

**Features**:
- Configurable date range
- Batch processing for large date ranges
- Respects API rate limits
- Progress tracking
- Resume capability (if interrupted)

**Usage**:
```bash
python scripts/backfill_historical_data.py --start-date 2024-01-01 --end-date 2024-03-01
```

---

### Phase 9: Testing & Documentation (Week 10)

#### 9.1 Unit Tests
- Test data transformations
- Test technical indicator calculations
- Test data validators
- Test utility functions

#### 9.2 Integration Tests
- Test end-to-end pipeline
- Test S3 → Spark → Kafka → Databricks Delta Lake flow
- Test Delta Lake MERGE operations
- Test error scenarios

#### 9.3 Documentation
- Architecture documentation
- Setup guide
- API documentation
- Dashboard user guide
- Troubleshooting guide

---

## Configuration Files

### config/assets.yaml
```yaml
stocks:
  - symbol: AAPL
    name: Apple Inc.
  - symbol: MSFT
    name: Microsoft Corporation
  # ... more stocks

crypto:
  - symbol: BTC-USD
    name: Bitcoin
  - symbol: ETH-USD
    name: Ethereum
  # ... more crypto
```

### config/pipeline_config.yaml
```yaml
ingestion:
  schedule: "0 * * * *"  # Hourly (cron syntax)
  retry_attempts: 3
  retry_delay: 60  # seconds

backfill:
  enabled: true
  default_period_days: 90

data_quality:
  strict_mode: true
  alert_on_failures: true
```

---

## Key Implementation Details

### Data Formats
- **Bronze**: Raw JSON/Parquet (as received from API)
- **Silver**: Cleaned Parquet (validated, standardized)
- **Gold**: Enriched Parquet (with calculated metrics)
- **Warehouse**: Delta Lake tables (Databricks)

### Partitioning Strategy
- S3: `year=YYYY/month=MM/day=DD/hour=HH/symbol=SYMBOL/`
- Enables efficient querying by time range and symbol
- Spark can leverage partition pruning
- Delta Lake tables: Partition by `date` and `symbol` for optimal query performance

### Databricks-Specific Considerations
- **Delta Lake Storage**: Store Delta tables in S3 or DBFS
- **Table Management**: Use Unity Catalog (recommended) or Hive metastore
- **Data Loading**: Use `MERGE` statements for upserts (ACID transactions)
- **Optimization**: 
  - Run `OPTIMIZE` to compact small files
  - Use `ZORDER BY` on frequently queried columns (e.g., symbol, date)
  - Schedule `VACUUM` to remove old file versions
- **Query Performance**: 
  - Use Databricks SQL Warehouse for dashboard queries
  - Leverage Delta Lake caching and predicate pushdown
- **Integration Options**:
  - Databricks Jobs API (for Airflow integration)
  - Databricks Connect (for local development)
  - Spark Structured Streaming (for Kafka consumption)

### Technical Indicators Formulas
- **RSI**: 14-period RSI using Wilder's smoothing
- **MACD**: EMA(12) - EMA(26), Signal = EMA(9) of MACD
- **Bollinger Bands**: SMA(20) ± 2*StdDev(20)

### Error Handling Strategy
- Retry with exponential backoff: 1s, 2s, 4s, 8s, 16s
- Max retries: 3-5 attempts
- Dead letter queue for permanently failed records
- Comprehensive logging

---

## Success Criteria

- ✅ Data successfully ingested from Yahoo Finance API
- ✅ Data stored in S3 with proper partitioning
- ✅ ETL pipeline processes data through Bronze → Silver → Gold
- ✅ Technical indicators calculated correctly
- ✅ Data loaded into Databricks Delta Lake with proper schema
- ✅ Kafka integration working (S3 → Kafka → Databricks)
- ✅ Delta Lake optimizations (Z-order, compaction) working
- ✅ Grafana dashboard displays all metrics
- ✅ Pipeline runs on schedule (hourly)
- ✅ Error handling and retries working
- ✅ Data quality checks passing
- ✅ Historical backfill functional
- ✅ Code well-documented and tested

---

## Next Steps

1. Review and approve this plan
2. Set up development environment (Docker, AWS credentials, Databricks workspace)
3. Begin Phase 1: Infrastructure Setup
4. Iterate through phases sequentially
5. Test each phase before moving to next

---

## Future Enhancements (Post-Capstone)

- Add CoinGecko integration
- Real-time streaming with Spark Structured Streaming
- Machine learning predictions
- Alert system for price movements
- Additional data sources
- Cloud deployment (Databricks on AWS, MSK, MWAA)
- Use Databricks Workflows for orchestration (alternative to Airflow)

