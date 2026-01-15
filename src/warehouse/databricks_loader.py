"""Databricks loader using Spark Structured Streaming."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, lit, year, month, dayofmonth, hour,
    minute, second, to_date, date_format, when, isnan, isnull
)
from pyspark.sql.types import StructType, StringType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging
from pathlib import Path
import sys
from typing import Optional, Dict, Any
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import get_logger
from ingestion.config import load_aws_config

logger = get_logger(__name__)


def load_databricks_config() -> Dict[str, Any]:
    """Load Databricks configuration from config file."""
    import yaml
    config_path = Path(__file__).parent.parent.parent / "config" / "databricks_config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config['databricks']


def create_spark_session(app_name: str = "DatabricksLoader") -> SparkSession:
    """
    Create Spark session with Delta and Kafka support.
    
    Args:
        app_name: Application name for Spark session
    
    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .config("spark.sql.streaming.schemaInference", "true")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def load_from_kafka(
    spark: SparkSession,
    topic: str,
    kafka_broker: str,
    starting_offsets: str = "latest"
) -> DataFrame:
    """
    Consume from Kafka topic using Spark Structured Streaming.
    
    Args:
        spark: SparkSession
        topic: Kafka topic name
        kafka_broker: Kafka broker address
        starting_offsets: Starting offset (earliest, latest, or JSON string)
    
    Returns:
        Streaming DataFrame
    """
    logger.info(f"Loading from Kafka topic: {topic}, broker: {kafka_broker}")
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .option("failOnDataLoss", "false") \
        .option("kafka.group.id", f"databricks-loader-{topic}") \
        .load()
    
    return df


def parse_kafka_json(df: DataFrame, schema: Optional[StructType] = None) -> DataFrame:
    """
    Parse JSON messages from Kafka.
    
    Args:
        df: Streaming DataFrame from Kafka
        schema: Optional schema for JSON parsing
    
    Returns:
        DataFrame with parsed JSON data
    """
    # Extract value as string
    df = df.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("json_value"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    )
    
    # Parse JSON
    if schema:
        df = df.select(
            "*",
            from_json(col("json_value"), schema).alias("data")
        )
        # Flatten data columns
        data_cols = [col(f"data.{field.name}").alias(field.name) for field in schema.fields]
        df = df.select("message_key", "kafka_timestamp", "partition", "offset", *data_cols)
    else:
        # For streaming, we'll use a generic approach
        # In production, provide a proper schema
        logger.warning("No schema provided, using generic JSON parsing")
        df = df.select(
            "*",
            from_json(col("json_value"), "json_value STRING").alias("data")
        )
        df = df.select("message_key", "kafka_timestamp", "partition", "offset", col("data.*"))
    
    return df


def transform_for_delta_schema(df: DataFrame) -> DataFrame:
    """
    Transform streaming data to match Delta Lake schema.
    
    Args:
        df: Streaming DataFrame with parsed Kafka messages
    
    Returns:
        Transformed DataFrame
    """
    # Add partition columns if not present
    if 'timestamp' in df.columns:
        df = df.withColumn("year", year(col("timestamp"))) \
               .withColumn("month", month(col("timestamp"))) \
               .withColumn("day", dayofmonth(col("timestamp"))) \
               .withColumn("hour", hour(col("timestamp")))
    
    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", lit(datetime.utcnow()))
    
    # Map columns to Delta schema
    # Note: This is a simplified mapping - adjust based on actual Gold layer schema
    column_mapping = {
        'open': 'open_price',
        'high': 'high_price',
        'low': 'low_price',
        'close': 'close_price',
    }
    
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns and new_col not in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
    
    # Handle missing columns with nulls
    required_cols = [
        'symbol_id', 'date_id', 'time_id', 'open_price', 'high_price',
        'low_price', 'close_price', 'volume', 'market_cap', 'change_pct',
        'hourly_change_pct', 'daily_change_pct', 'weekly_change_pct',
        'rsi', 'macd', 'macd_signal', 'bollinger_upper', 'bollinger_lower',
        'sma_7', 'sma_14', 'sma_30', 'sma_50', 'sma_200'
    ]
    
    for col_name in required_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast("double"))
    
    return df


def merge_to_delta(
    df: DataFrame,
    delta_table_path: str,
    merge_keys: list,
    spark: SparkSession
) -> None:
    """
    Merge data into Delta table using upsert operation.
    
    Args:
        df: DataFrame to merge
        delta_table_path: Path to Delta table
        merge_keys: List of column names to use for merge condition
        spark: SparkSession
    """
    try:
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        
        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Perform merge
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        logger.info(f"Merged data into Delta table at {delta_table_path}")
        
    except Exception as e:
        logger.error(f"Error merging to Delta table: {e}", exc_info=True)
        raise


def write_stream_to_delta(
    df: DataFrame,
    delta_table_path: str,
    checkpoint_location: str,
    output_mode: str = "append"
) -> None:
    """
    Write streaming DataFrame to Delta table.
    
    Args:
        df: Streaming DataFrame
        delta_table_path: Path to Delta table
        checkpoint_location: Checkpoint location for streaming
        output_mode: Output mode (append, complete, update)
    """
    logger.info(f"Writing stream to Delta table at {delta_table_path}")
    
    query = df.writeStream \
        .format("delta") \
        .outputMode(output_mode) \
        .option("checkpointLocation", checkpoint_location) \
        .option("mergeSchema", "true") \
        .start(delta_table_path)
    
    return query


def optimize_delta_tables(spark: SparkSession, delta_table_paths: list, z_order_cols: Optional[Dict[str, list]] = None) -> None:
    """
    Optimize Delta tables (Z-order, compaction).
    
    Args:
        spark: SparkSession
        delta_table_paths: List of Delta table paths to optimize
        z_order_cols: Optional dict mapping table paths to columns for Z-ordering
    """
    for table_path in delta_table_paths:
        try:
            logger.info(f"Optimizing Delta table: {table_path}")
            
            # Read table
            df = spark.read.format("delta").load(table_path)
            
            # Z-order optimization if specified
            if z_order_cols and table_path in z_order_cols:
                cols = ", ".join(z_order_cols[table_path])
                spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({cols})")
                logger.info(f"Applied Z-order optimization on {cols}")
            
            # General optimization (compaction)
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            logger.info(f"Optimized table: {table_path}")
            
        except Exception as e:
            logger.error(f"Error optimizing table {table_path}: {e}", exc_info=True)


def vacuum_delta_tables(spark: SparkSession, delta_table_paths: list, retention_hours: int = 168) -> None:
    """
    Run VACUUM on Delta tables to remove old files.
    
    Args:
        spark: SparkSession
        delta_table_paths: List of Delta table paths
        retention_hours: Retention period in hours (default 168 = 7 days)
    """
    for table_path in delta_table_paths:
        try:
            logger.info(f"Running VACUUM on Delta table: {table_path}")
            spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS")
            logger.info(f"VACUUM completed for: {table_path}")
        except Exception as e:
            logger.error(f"Error running VACUUM on {table_path}: {e}", exc_info=True)


def run_main_loader(
    kafka_broker: str,
    topics: list,
    delta_table_path: str,
    checkpoint_base: str = "/tmp/checkpoints",
    starting_offsets: str = "latest"
) -> None:
    """
    Main function to run the Databricks loader.
    
    Args:
        kafka_broker: Kafka broker address
        topics: List of Kafka topics to consume
        delta_table_path: Path to Delta table
        checkpoint_base: Base path for checkpoints
        starting_offsets: Starting offset for Kafka
    """
    spark = create_spark_session()
    
    try:
        # Load from each topic
        streams = []
        for topic in topics:
            logger.info(f"Starting stream for topic: {topic}")
            
            # Load from Kafka
            kafka_df = load_from_kafka(spark, topic, kafka_broker, starting_offsets)
            
            # Parse JSON (simplified - in production, use proper schema)
            # For now, we'll use a generic approach
            parsed_df = kafka_df.select(
                col("key").cast("string").alias("message_key"),
                col("value").cast("string").alias("json_value"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Parse JSON - using a generic schema approach
            # In production, define a proper schema based on your Gold layer structure
            from pyspark.sql.types import (
                StructType, StructField, StringType, DoubleType, LongType, TimestampType
            )
            
            # Generic schema for market data (adjust based on actual Gold schema)
            market_data_schema = StructType([
                StructField("symbol", StringType(), True),
                StructField("asset_type", StringType(), True),
                StructField("open", DoubleType(), True),
                StructField("high", DoubleType(), True),
                StructField("low", DoubleType(), True),
                StructField("close", DoubleType(), True),
                StructField("volume", LongType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("rsi", DoubleType(), True),
                StructField("macd", DoubleType(), True),
                # Add more fields as needed
            ])
            
            json_df = parsed_df.select(
                "*",
                from_json(col("json_value"), market_data_schema).alias("data")
            ).select("data.*")
            
            # Transform for Delta schema
            transformed_df = transform_for_delta_schema(json_df)
            
            # Write to Delta (using batch processing for simplicity)
            # In production, use write_stream_to_delta for true streaming
            checkpoint_location = f"{checkpoint_base}/{topic}"
            
            query = write_stream_to_delta(
                transformed_df,
                delta_table_path,
                checkpoint_location,
                output_mode="append"
            )
            
            streams.append((topic, query))
        
        # Wait for all streams
        logger.info("Starting streaming queries...")
        for topic, query in streams:
            logger.info(f"Waiting for stream: {topic}")
            query.awaitTermination(timeout=60)  # Wait 60 seconds for testing
        
        logger.info("Streaming completed")
        
    except Exception as e:
        logger.error(f"Error in main loader: {e}", exc_info=True)
        raise
    finally:
        # Stop all streams
        for topic, query in streams:
            try:
                query.stop()
            except:
                pass
        
        spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Databricks Loader')
    parser.add_argument('--kafka-broker', type=str, help='Kafka broker address')
    parser.add_argument('--topics', type=str, nargs='+', help='Kafka topics to consume')
    parser.add_argument('--delta-path', type=str, help='Delta table path')
    parser.add_argument('--checkpoint', type=str, default='/tmp/checkpoints', help='Checkpoint location')
    
    args = parser.parse_args()
    
    # Setup logging
    from utils import setup_logger
    setup_logger(__name__)
    
    # Load configs if not provided
    if not args.kafka_broker:
        from warehouse.kafka_producer import load_kafka_config
        kafka_config = load_kafka_config()
        args.kafka_broker = kafka_config['broker']['internal']
    
    if not args.topics:
        kafka_config = load_kafka_config()
        args.topics = [t['name'] for t in kafka_config['topics']]
    
    if not args.delta_path:
        aws_config = load_aws_config()
        bucket_name = aws_config['s3']['bucket_name']
        args.delta_path = f"s3a://{bucket_name}/delta/fact_market_data"
    
    try:
        run_main_loader(
            args.kafka_broker,
            args.topics,
            args.delta_path,
            args.checkpoint
        )
    except Exception as e:
        logger.error(f"Loader failed: {e}", exc_info=True)
        sys.exit(1)
