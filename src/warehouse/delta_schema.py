"""Delta Lake schema definitions and table creation functions."""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType,
    TimestampType, BooleanType, LongType, DateType
)
from delta import configure_spark_with_delta_pip
import logging
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import get_logger

logger = get_logger(__name__)


def get_fact_table_schema() -> StructType:
    """Get schema for fact_market_data table."""
    return StructType([
        StructField("market_data_id", LongType(), True),
        StructField("symbol_id", IntegerType(), False),
        StructField("date_id", IntegerType(), False),
        StructField("time_id", IntegerType(), False),
        StructField("open_price", DecimalType(18, 4), True),
        StructField("high_price", DecimalType(18, 4), True),
        StructField("low_price", DecimalType(18, 4), True),
        StructField("close_price", DecimalType(18, 4), True),
        StructField("volume", LongType(), True),
        StructField("market_cap", LongType(), True),
        StructField("change_pct", DecimalType(10, 4), True),
        StructField("hourly_change_pct", DecimalType(10, 4), True),
        StructField("daily_change_pct", DecimalType(10, 4), True),
        StructField("weekly_change_pct", DecimalType(10, 4), True),
        StructField("rsi", DecimalType(10, 4), True),
        StructField("macd", DecimalType(18, 4), True),
        StructField("macd_signal", DecimalType(18, 4), True),
        StructField("bollinger_upper", DecimalType(18, 4), True),
        StructField("bollinger_lower", DecimalType(18, 4), True),
        StructField("sma_7", DecimalType(18, 4), True),
        StructField("sma_14", DecimalType(18, 4), True),
        StructField("sma_30", DecimalType(18, 4), True),
        StructField("sma_50", DecimalType(18, 4), True),
        StructField("sma_200", DecimalType(18, 4), True),
        StructField("timestamp", TimestampType(), False),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
    ])


def get_dim_symbol_schema() -> StructType:
    """Get schema for dim_symbol table."""
    return StructType([
        StructField("symbol_id", IntegerType(), False),
        StructField("symbol", StringType(), False),
        StructField("name", StringType(), True),
        StructField("asset_type", StringType(), False),
        StructField("sector", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])


def get_dim_date_schema() -> StructType:
    """Get schema for dim_date table."""
    return StructType([
        StructField("date_id", IntegerType(), False),
        StructField("date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("week", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), True),
        StructField("month_name", StringType(), True),
        StructField("is_weekend", BooleanType(), True),
        StructField("is_holiday", BooleanType(), True),
    ])


def get_dim_time_schema() -> StructType:
    """Get schema for dim_time table."""
    return StructType([
        StructField("time_id", IntegerType(), False),
        StructField("hour", IntegerType(), False),
        StructField("minute", IntegerType(), False),
        StructField("second", IntegerType(), False),
        StructField("time_of_day", StringType(), True),
        StructField("period_of_day", StringType(), True),
    ])


def get_dim_correlation_schema() -> StructType:
    """Get schema for dim_correlation table."""
    return StructType([
        StructField("correlation_id", LongType(), True),
        StructField("symbol_id_1", IntegerType(), False),
        StructField("symbol_id_2", IntegerType(), False),
        StructField("correlation_value", DecimalType(10, 6), False),
        StructField("period", StringType(), False),
        StructField("calculated_at", TimestampType(), False),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
    ])


def get_dimension_table_schemas() -> dict:
    """Get all dimension table schemas."""
    return {
        'dim_symbol': get_dim_symbol_schema(),
        'dim_date': get_dim_date_schema(),
        'dim_time': get_dim_time_schema(),
        'dim_correlation': get_dim_correlation_schema(),
    }


def create_spark_session_for_schema() -> SparkSession:
    """Create Spark session with Delta Lake configuration."""
    builder = SparkSession.builder \
        .appName("DeltaSchemaCreation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def create_fact_table(spark: SparkSession, bucket_name: str, database_name: str = "market_data") -> None:
    """Create fact_market_data Delta table."""
    delta_path = f"s3a://{bucket_name}/delta/fact_market_data"
    
    try:
        # Create database if it doesn't exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        spark.sql(f"USE {database_name}")
        
        # Create table using SQL
        create_sql = f"""
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
        LOCATION '{delta_path}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        spark.sql(create_sql)
        logger.info(f"Created fact_market_data table at {delta_path}")
        
    except Exception as e:
        logger.error(f"Error creating fact table: {e}", exc_info=True)
        raise


def create_dimension_tables(spark: SparkSession, bucket_name: str, database_name: str = "market_data") -> None:
    """Create all dimension tables."""
    try:
        # Create database if it doesn't exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        spark.sql(f"USE {database_name}")
        
        # Create dim_symbol
        dim_symbol_path = f"s3a://{bucket_name}/delta/dim_symbol"
        spark.sql(f"""
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
        LOCATION '{dim_symbol_path}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """)
        logger.info(f"Created dim_symbol table at {dim_symbol_path}")
        
        # Create dim_date
        dim_date_path = f"s3a://{bucket_name}/delta/dim_date"
        spark.sql(f"""
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
        LOCATION '{dim_date_path}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """)
        logger.info(f"Created dim_date table at {dim_date_path}")
        
        # Create dim_time
        dim_time_path = f"s3a://{bucket_name}/delta/dim_time"
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dim_time (
            time_id INT NOT NULL,
            hour INT NOT NULL,
            minute INT NOT NULL,
            second INT NOT NULL,
            time_of_day STRING,
            period_of_day STRING
        )
        USING DELTA
        LOCATION '{dim_time_path}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """)
        logger.info(f"Created dim_time table at {dim_time_path}")
        
        # Create dim_correlation
        dim_correlation_path = f"s3a://{bucket_name}/delta/dim_correlation"
        spark.sql(f"""
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
        LOCATION '{dim_correlation_path}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """)
        logger.info(f"Created dim_correlation table at {dim_correlation_path}")
        
    except Exception as e:
        logger.error(f"Error creating dimension tables: {e}", exc_info=True)
        raise


def create_all_tables(bucket_name: str, database_name: str = "market_data") -> None:
    """Create all Delta Lake tables (fact and dimensions)."""
    spark = create_spark_session_for_schema()
    
    try:
        create_fact_table(spark, bucket_name, database_name)
        create_dimension_tables(spark, bucket_name, database_name)
        logger.info("All Delta Lake tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    from utils import setup_logger
    
    # Setup logging
    setup_logger(__name__)
    
    bucket = sys.argv[1] if len(sys.argv) > 1 else None
    database = sys.argv[2] if len(sys.argv) > 2 else "market_data"
    
    if not bucket:
        # Try to load from config
        try:
            from ingestion.config import load_aws_config
            aws_config = load_aws_config()
            bucket = aws_config['s3']['bucket_name']
        except Exception as e:
            logger.error(f"Could not determine bucket name: {e}")
            sys.exit(1)
    
    create_all_tables(bucket, database)
