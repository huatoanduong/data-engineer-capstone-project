"""Airflow DAG for market data ingestion."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion import YahooFinanceIngester
from utils import get_logger
from ingestion.config import load_ingestion_config, load_assets

logger = get_logger(__name__)

# Load configuration
ingestion_config = load_ingestion_config()
schedule = ingestion_config.get('schedule', '0 * * * *')

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': ingestion_config.get('retry_attempts', 3),
    'retry_delay': timedelta(seconds=ingestion_config.get('retry_delay_seconds', 60)),
}

# Create DAG
dag = DAG(
    'market_data_ingestion',
    default_args=default_args,
    description='Ingest stock and crypto market data from Yahoo Finance',
    schedule=schedule,  # Use 'schedule' for Airflow 2.4+, 'schedule_interval' for older versions
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ingestion', 'market-data'],
)


def fetch_stock_data(**context):
    """Fetch stock market data."""
    logger.info("Starting stock data ingestion")
    ingester = YahooFinanceIngester()
    assets = load_assets()
    
    results = {}
    for stock in assets['stocks']:
        symbol = stock['symbol']
        logger.info(f"Fetching data for stock: {symbol}")
        results[symbol] = ingester.ingest_asset(symbol, 'stocks')
    
    successful = sum(1 for v in results.values() if v)
    total = len(results)
    logger.info(f"Stock ingestion complete: {successful}/{total} successful")
    
    # Push results to XCom for downstream tasks
    context['ti'].xcom_push(key='stock_results', value=results)
    return results


def fetch_crypto_data(**context):
    """Fetch crypto market data."""
    logger.info("Starting crypto data ingestion")
    ingester = YahooFinanceIngester()
    assets = load_assets()
    
    results = {}
    for crypto in assets['crypto']:
        symbol = crypto['symbol']
        logger.info(f"Fetching data for crypto: {symbol}")
        results[symbol] = ingester.ingest_asset(symbol, 'crypto')
    
    successful = sum(1 for v in results.values() if v)
    total = len(results)
    logger.info(f"Crypto ingestion complete: {successful}/{total} successful")
    
    # Push results to XCom
    context['ti'].xcom_push(key='crypto_results', value=results)
    return results


def validate_ingested_data(**context):
    """Validate all ingested data."""
    logger.info("Validating ingested data")
    
    # Pull results from upstream tasks
    stock_results = context['ti'].xcom_pull(task_ids='fetch_stock_data', key='stock_results')
    crypto_results = context['ti'].xcom_pull(task_ids='fetch_crypto_data', key='crypto_results')
    
    # Check for failures
    stock_failures = [k for k, v in stock_results.items() if not v] if stock_results else []
    crypto_failures = [k for k, v in crypto_results.items() if not v] if crypto_results else []
    
    if stock_failures or crypto_failures:
        logger.warning(f"Some assets failed ingestion: Stocks: {stock_failures}, Crypto: {crypto_failures}")
        # Don't fail the task, just log warnings
    else:
        logger.info("All assets ingested successfully")
    
    return {
        'stock_failures': stock_failures,
        'crypto_failures': crypto_failures
    }


def log_ingestion_metrics(**context):
    """Log ingestion metrics."""
    logger.info("Logging ingestion metrics")
    
    validation_results = context['ti'].xcom_pull(task_ids='validate_ingested_data')
    
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'stock_failures': validation_results.get('stock_failures', []) if validation_results else [],
        'crypto_failures': validation_results.get('crypto_failures', []) if validation_results else [],
    }
    
    logger.info(f"Ingestion metrics: {metrics}")
    
    # Could also send to monitoring system (CloudWatch, Prometheus, etc.)
    return metrics


# Define tasks
fetch_stock_task = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_stock_data,
    dag=dag,
)

fetch_crypto_task = PythonOperator(
    task_id='fetch_crypto_data',
    python_callable=fetch_crypto_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_ingested_data',
    python_callable=validate_ingested_data,
    dag=dag,
)

log_metrics_task = PythonOperator(
    task_id='log_ingestion_metrics',
    python_callable=log_ingestion_metrics,
    dag=dag,
)

# Set task dependencies
[fetch_stock_task, fetch_crypto_task] >> validate_task >> log_metrics_task
