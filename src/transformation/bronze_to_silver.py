"""Bronze to Silver data transformation."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, to_timestamp, lit, year, month, dayofmonth, hour, last, first
from pyspark.sql.types import DoubleType, LongType, TimestampType
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
import logging
import sys
from pathlib import Path

# Add config path for loading AWS config
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session with S3 and Delta Lake configuration."""
    builder = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def read_bronze_data(spark: SparkSession, bucket: str, date_filter: str = None) -> DataFrame:
    """Read data from S3 Bronze layer."""
    bronze_path = f"s3a://{bucket}/bronze"
    
    if date_filter:
        bronze_path = f"{bronze_path}/{date_filter}"
    
    logger.info(f"Reading from: {bronze_path}")
    df = spark.read.parquet(bronze_path)
    return df


def remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicate records."""
    initial_count = df.count()
    df = df.dropDuplicates(['timestamp', 'symbol'])
    final_count = df.count()
    logger.info(f"Removed {initial_count - final_count} duplicate records")
    return df


def handle_nulls(df: DataFrame) -> DataFrame:
    """Handle null values with forward fill for prices and 0 for volume."""
    window_forward = Window.partitionBy('symbol').orderBy('timestamp').rowsBetween(Window.unboundedPreceding, 0)
    window_backward = Window.partitionBy('symbol').orderBy('timestamp').rowsBetween(0, Window.unboundedFollowing)
    
    # Forward fill for price columns using last() with ignoreNulls
    price_cols = ['open', 'high', 'low', 'close']
    for col_name in price_cols:
        # Forward fill: use last non-null value from previous rows
        df = df.withColumn(
            f'{col_name}_ffill',
            last(col(col_name), ignorenulls=True).over(window_forward)
        )
        # Backward fill for remaining nulls (first rows): use first non-null value from following rows
        df = df.withColumn(
            f'{col_name}_bfill',
            first(col(col_name), ignorenulls=True).over(window_backward)
        )
        # Combine: use forward fill if available, otherwise backward fill
        df = df.withColumn(
            col_name,
            when(isnull(col(col_name)), 
                 when(isnull(col(f'{col_name}_ffill')), col(f'{col_name}_bfill'))
                 .otherwise(col(f'{col_name}_ffill'))
            ).otherwise(col(col_name))
        )
        # Drop temporary columns
        df = df.drop(f'{col_name}_ffill', f'{col_name}_bfill')
    
    # Fill volume with 0 if null
    df = df.withColumn('volume', when(isnull(col('volume')), lit(0)).otherwise(col('volume')))
    
    return df


def standardize_types(df: DataFrame) -> DataFrame:
    """Standardize data types."""
    df = df.withColumn('timestamp', to_timestamp(col('timestamp')))
    df = df.withColumn('open', col('open').cast(DoubleType()))
    df = df.withColumn('high', col('high').cast(DoubleType()))
    df = df.withColumn('low', col('low').cast(DoubleType()))
    df = df.withColumn('close', col('close').cast(DoubleType()))
    df = df.withColumn('volume', col('volume').cast(LongType()))
    
    return df


def validate_business_rules(df: DataFrame) -> DataFrame:
    """Validate and filter based on business rules."""
    initial_count = df.count()
    
    # Price > 0
    df = df.filter((col('open') > 0) & (col('high') > 0) & (col('low') > 0) & (col('close') > 0))
    
    # Volume >= 0
    df = df.filter(col('volume') >= 0)
    
    # Market cap >= 0 (if present)
    if 'market_cap' in df.columns:
        df = df.filter((col('market_cap').isNull()) | (col('market_cap') >= 0))
    
    final_count = df.count()
    logger.info(f"Filtered {initial_count - final_count} records that violated business rules")
    
    return df


def write_silver_data(df: DataFrame, bucket: str) -> None:
    """Write data to S3 Silver layer using Delta Lake."""
    silver_path = f"s3a://{bucket}/silver"
    
    df.write \
        .format("delta") \
        .mode('overwrite') \
        .partitionBy('year', 'month', 'day', 'hour', 'symbol') \
        .save(silver_path)
    
    logger.info(f"Written to: {silver_path}")


def transform_bronze_to_silver(bucket: str, date_filter: str = None) -> None:
    """Main transformation function."""
    spark = create_spark_session()
    
    try:
        # Read bronze data
        df = read_bronze_data(spark, bucket, date_filter)
        
        if df.count() == 0:
            logger.warning("No data found in Bronze layer")
            return
        
        # Transform
        df = remove_duplicates(df)
        df = handle_nulls(df)
        df = standardize_types(df)
        df = validate_business_rules(df)
        
        # Add partition columns
        df = df.withColumn('year', year(col('timestamp')))
        df = df.withColumn('month', month(col('timestamp')))
        df = df.withColumn('day', dayofmonth(col('timestamp')))
        df = df.withColumn('hour', hour(col('timestamp')))
        
        # Write silver data
        write_silver_data(df, bucket)
        
        logger.info("Bronze to Silver transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in Bronze to Silver transformation: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    from utils import setup_logger
    
    # Setup logging
    setup_logger(__name__)
    
    bucket = sys.argv[1] if len(sys.argv) > 1 else None
    date_filter = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not bucket:
        # Try to load from config
        try:
            from ingestion.config import load_aws_config
            aws_config = load_aws_config()
            bucket = aws_config['s3']['bucket_name']
        except Exception as e:
            logger.error(f"Could not determine bucket name: {e}")
            sys.exit(1)
    
    transform_bronze_to_silver(bucket, date_filter)
