"""Airflow DAG for warehouse loading."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'warehouse_load',
    default_args=default_args,
    description='Load data to Databricks Delta Lake from Kafka',
    schedule='10 * * * *',  # 10 minutes after each hour
    start_date=days_ago(1),
    catchup=False,
    tags=['warehouse', 'databricks', 'kafka'],
)


def trigger_kafka_producer(**context):
    """Trigger Kafka producer to read from S3 Gold and publish to Kafka."""
    from warehouse.kafka_producer import MarketDataProducer
    from utils import setup_logger
    
    logger = setup_logger(__name__)
    
    try:
        # Get execution date from context
        execution_date = context.get('execution_date')
        date_filter = None
        
        if execution_date:
            # Format date filter for S3 path
            date_filter = f"year={execution_date.year}/month={execution_date.month:02d}/day={execution_date.day:02d}"
        
        logger.info(f"Triggering Kafka producer with date filter: {date_filter}")
        
        # Create producer and run
        producer = MarketDataProducer(use_internal_broker=True)
        results = producer.produce_market_data(date_filter=date_filter)
        
        logger.info(f"Kafka producer completed. Results: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Error in Kafka producer: {e}", exc_info=True)
        raise


def load_to_databricks(**context):
    """Load data from Kafka to Databricks Delta Lake."""
    from warehouse.databricks_loader import run_main_loader
    from warehouse.kafka_producer import load_kafka_config
    from utils import setup_logger
    from ingestion.config import load_aws_config
    
    logger = setup_logger(__name__)
    
    try:
        # Load configurations
        kafka_config = load_kafka_config()
        aws_config = load_aws_config()
        
        kafka_broker = kafka_config['broker']['internal']
        topics = [t['name'] for t in kafka_config['topics']]
        bucket_name = aws_config['s3']['bucket_name']
        delta_path = f"s3a://{bucket_name}/delta/fact_market_data"
        
        logger.info(f"Loading to Databricks from Kafka topics: {topics}")
        
        # Run loader
        run_main_loader(
            kafka_broker=kafka_broker,
            topics=topics,
            delta_table_path=delta_path,
            checkpoint_base="/tmp/checkpoints/warehouse_load",
            starting_offsets="latest"
        )
        
        logger.info("Databricks loader completed")
        
    except Exception as e:
        logger.error(f"Error in Databricks loader: {e}", exc_info=True)
        raise


def optimize_delta_tables_task(**context):
    """Optimize Delta tables (Z-order, compaction)."""
    from pyspark.sql import SparkSession
    from warehouse.databricks_loader import create_spark_session, optimize_delta_tables
    from ingestion.config import load_aws_config
    from utils import setup_logger
    
    logger = setup_logger(__name__)
    
    try:
        aws_config = load_aws_config()
        bucket_name = aws_config['s3']['bucket_name']
        
        # Create Spark session
        spark = create_spark_session("DeltaOptimization")
        
        # Define tables to optimize
        delta_tables = [
            f"s3a://{bucket_name}/delta/fact_market_data",
            f"s3a://{bucket_name}/delta/dim_symbol",
            f"s3a://{bucket_name}/delta/dim_date",
            f"s3a://{bucket_name}/delta/dim_time",
            f"s3a://{bucket_name}/delta/dim_correlation",
        ]
        
        # Z-order columns for fact table
        z_order_cols = {
            f"s3a://{bucket_name}/delta/fact_market_data": ['symbol_id', 'timestamp']
        }
        
        logger.info("Optimizing Delta tables...")
        optimize_delta_tables(spark, delta_tables, z_order_cols)
        
        logger.info("Delta table optimization completed")
        spark.stop()
        
    except Exception as e:
        logger.error(f"Error optimizing Delta tables: {e}", exc_info=True)
        raise


def vacuum_delta_tables_task(**context):
    """Run VACUUM on Delta tables to remove old files."""
    from warehouse.databricks_loader import create_spark_session, vacuum_delta_tables
    from ingestion.config import load_aws_config
    from utils import setup_logger
    
    logger = setup_logger(__name__)
    
    try:
        aws_config = load_aws_config()
        bucket_name = aws_config['s3']['bucket_name']
        
        # Create Spark session
        spark = create_spark_session("DeltaVacuum")
        
        # Define tables to vacuum
        delta_tables = [
            f"s3a://{bucket_name}/delta/fact_market_data",
            f"s3a://{bucket_name}/delta/dim_symbol",
            f"s3a://{bucket_name}/delta/dim_date",
            f"s3a://{bucket_name}/delta/dim_time",
            f"s3a://{bucket_name}/delta/dim_correlation",
        ]
        
        logger.info("Running VACUUM on Delta tables...")
        vacuum_delta_tables(spark, delta_tables, retention_hours=168)  # 7 days
        
        logger.info("VACUUM completed")
        spark.stop()
        
    except Exception as e:
        logger.error(f"Error running VACUUM: {e}", exc_info=True)
        raise


def validate_warehouse_data(**context):
    """Validate warehouse data quality."""
    from pyspark.sql import SparkSession
    from warehouse.databricks_loader import create_spark_session
    from ingestion.config import load_aws_config
    from utils import setup_logger
    
    logger = setup_logger(__name__)
    
    try:
        aws_config = load_aws_config()
        bucket_name = aws_config['s3']['bucket_name']
        
        # Create Spark session
        spark = create_spark_session("DataValidation")
        
        # Read fact table
        fact_path = f"s3a://{bucket_name}/delta/fact_market_data"
        df = spark.read.format("delta").load(fact_path)
        
        # Basic validations
        total_count = df.count()
        null_symbols = df.filter(df.symbol_id.isNull()).count()
        null_timestamps = df.filter(df.timestamp.isNull()).count()
        
        logger.info(f"Validation results:")
        logger.info(f"  Total records: {total_count}")
        logger.info(f"  Null symbol_id: {null_symbols}")
        logger.info(f"  Null timestamps: {null_timestamps}")
        
        # Check for recent data
        from pyspark.sql.functions import max as spark_max
        latest_timestamp = df.agg(spark_max("timestamp")).collect()[0][0]
        logger.info(f"  Latest timestamp: {latest_timestamp}")
        
        # Validation checks
        if total_count == 0:
            raise ValueError("No data in fact table")
        
        if null_symbols > 0:
            logger.warning(f"Found {null_symbols} records with null symbol_id")
        
        if null_timestamps > 0:
            raise ValueError(f"Found {null_timestamps} records with null timestamp")
        
        logger.info("Data validation passed")
        spark.stop()
        
    except Exception as e:
        logger.error(f"Data validation failed: {e}", exc_info=True)
        raise


# Task definitions
trigger_producer_task = PythonOperator(
    task_id='trigger_kafka_producer',
    python_callable=trigger_kafka_producer,
    dag=dag,
)

load_to_databricks_task = PythonOperator(
    task_id='load_to_databricks',
    python_callable=load_to_databricks,
    dag=dag,
)

optimize_tables_task = PythonOperator(
    task_id='optimize_delta_tables',
    python_callable=optimize_delta_tables_task,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_warehouse_data',
    python_callable=validate_warehouse_data,
    dag=dag,
)

vacuum_tables_task = PythonOperator(
    task_id='vacuum_delta_tables',
    python_callable=vacuum_delta_tables_task,
    dag=dag,
)

# Task dependencies
trigger_producer_task >> load_to_databricks_task >> optimize_tables_task >> validate_data_task >> vacuum_tables_task
