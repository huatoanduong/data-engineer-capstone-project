# Stock & Crypto Market Data Pipeline

An end-to-end data engineering pipeline for ingesting, processing, and analyzing stock and cryptocurrency market data. This project demonstrates modern data engineering practices including data lake architecture, ETL processing, real-time streaming, and data warehousing.

## 🚀 Quick Start

### Prerequisites

- Docker Desktop or Docker Engine
- Docker Compose v2.0+
- AWS Account with S3 access
- Python 3.10+
- Databricks account (Community Edition or Trial)

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd DECapstoneProject
   ```

2. **Set up environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your AWS credentials and configuration
   ```

3. **Start Docker services:**
   ```bash
   cd docker
   docker-compose up -d
   ```

4. **Set up S3 bucket:**
   ```bash
   python scripts/setup_s3_bucket.py
   ```

5. **Access services:**
   - **Airflow UI**: http://localhost:8080 (airflow/airflow)
   - **Spark Master UI**: http://localhost:8081
   - **Kafka**: localhost:9092

For detailed setup instructions, see [Setup Guide](docs/setup_guide.md).

## 📋 Project Overview

This pipeline ingests market data from Yahoo Finance, processes it through multiple layers (Bronze → Silver → Gold), streams it via Kafka, and stores it in Databricks Delta Lake for analytics and visualization.

### Key Features

- **Data Ingestion**: Automated hourly ingestion from Yahoo Finance API
- **Multi-Layer Architecture**: Bronze (raw), Silver (cleaned), Gold (enriched) data layers
- **ETL Processing**: Apache Spark-based transformations with technical indicators
- **Real-Time Streaming**: Kafka-based event streaming
- **Data Warehouse**: Databricks Delta Lake with star schema
- **Data Quality**: Comprehensive validation and profiling
- **Monitoring**: Grafana dashboards for visualization
- **Historical Backfill**: Script for backfilling historical data

## 🏗️ Architecture

```
┌─────────────────┐
│  Yahoo Finance  │ (yfinance API)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Data Ingestion │ (Python + Airflow)
│   Hourly DAG    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   AWS S3        │ (Data Lake)
│  Bronze Layer   │ Partitioned Parquet
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
│  Apache Kafka   │ (Event Streaming)
│  Topics: stocks, │ crypto
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Databricks    │ (Data Warehouse)
│  Delta Lake     │ Star Schema
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Grafana      │ (Dashboards)
│  Visualizations │
└─────────────────┘
```

For detailed architecture documentation, see [Architecture Documentation](docs/architecture.md).

## 📁 Project Structure

```
DECapstoneProject/
├── src/                    # Source code
│   ├── ingestion/         # Data ingestion modules
│   ├── transformation/     # ETL transformations
│   ├── warehouse/         # Data warehouse loaders
│   ├── quality/           # Data quality checks
│   └── utils/             # Utility functions
├── dags/                   # Airflow DAGs
├── scripts/                # Setup and utility scripts
├── config/                 # Configuration files
├── tests/                  # Test suite
├── docs/                   # Documentation
├── sql/                    # SQL queries and schemas
└── grafana/                # Grafana dashboards
```

## 🔧 Configuration

### Assets Configuration

Edit `config/assets.yaml` to configure which stocks and cryptocurrencies to track:

```yaml
assets:
  stocks:
    - symbol: AAPL
      name: Apple Inc.
      sector: Technology
  crypto:
    - symbol: BTC-USD
      name: Bitcoin
```

### Pipeline Configuration

Edit `config/pipeline_config.yaml` to configure schedules, retry policies, and batch sizes.

### AWS Configuration

Edit `config/aws_config.yaml` to configure S3 bucket and paths.

## 📊 Data Flow

1. **Ingestion**: Yahoo Finance API → S3 Bronze (hourly)
2. **Transformation**: Bronze → Silver (cleaning) → Gold (enrichment)
3. **Streaming**: Gold layer → Kafka topics
4. **Warehouse**: Kafka → Databricks Delta Lake
5. **Visualization**: Databricks → Grafana dashboards

## 🧪 Testing

Run the test suite:

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test categories
pytest -m unit
pytest -m integration
```

## 📚 Documentation

- [Setup Guide](docs/setup_guide.md) - Detailed setup instructions
- [Architecture Documentation](docs/architecture.md) - System architecture and design
- [API Documentation](docs/api_documentation.md) - Module and function documentation
- [Backfill Guide](docs/backfill_guide.md) - Historical data backfill instructions
- [Troubleshooting](docs/troubleshooting.md) - Common issues and solutions
- [Databricks Setup](docs/databricks_setup.md) - Databricks configuration
- [Grafana Setup](docs/grafana_setup.md) - Grafana dashboard setup

## 🛠️ Usage Examples

### Run Data Ingestion

```bash
# Manual ingestion
python -m src.ingestion.yahoo_finance_ingester

# Or via Airflow DAG
# Access Airflow UI and trigger data_ingestion_dag
```

### Backfill Historical Data

```bash
python scripts/backfill_historical_data.py \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

### Run ETL Pipeline

```bash
# Via Airflow DAG
# Trigger etl_pipeline_dag in Airflow UI
```

## 🧩 Technology Stack

- **Data Lake**: AWS S3
- **ETL Processing**: Apache Spark (PySpark)
- **Orchestration**: Apache Airflow
- **Message Queue**: Apache Kafka
- **Data Warehouse**: Databricks (Delta Lake)
- **Visualization**: Grafana
- **Data Source**: Yahoo Finance (yfinance)
- **Development**: Docker-based local environment

## 📈 Features

### Data Ingestion
- Configurable asset list
- Retry logic with exponential backoff
- Data validation
- Partitioned storage (year/month/day/hour/symbol)

### ETL Processing
- Bronze to Silver: Data cleaning and standardization
- Silver to Gold: Technical indicators (RSI, MACD, Bollinger Bands)
- Correlation analysis
- Data quality checks

### Data Warehouse
- Star schema design
- Delta Lake tables
- Partitioned by date and asset type
- Optimized for analytics queries

### Data Quality
- Schema validation
- Business rule validation
- Data profiling
- Alerting on failures

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is part of a Data Engineering capstone project.

## 🆘 Support

For issues and questions:
- Check [Troubleshooting Guide](docs/troubleshooting.md)
- Review [API Documentation](docs/api_documentation.md)
- Open an issue on GitHub

## 🔗 Related Documentation

- [Setup Guide](docs/setup_guide.md)
- [Architecture Documentation](docs/architecture.md)
- [API Documentation](docs/api_documentation.md)
- [Backfill Guide](docs/backfill_guide.md)
- [Troubleshooting](docs/troubleshooting.md)
