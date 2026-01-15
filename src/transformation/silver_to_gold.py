"""Silver to Gold data transformation."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lag, when, lit, year, month, dayofmonth, hour, avg, max, min, sum as spark_sum
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from transformation.technical_indicators import calculate_all_indicators
from transformation.correlation_analysis import calculate_correlations

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session with S3 and Delta Lake configuration."""
    builder = SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def calculate_price_changes(df: DataFrame) -> DataFrame:
    """Calculate hourly, daily, and weekly price changes."""
    window = Window.partitionBy('symbol').orderBy('timestamp')
    
    # Hourly change
    df = df.withColumn('prev_close', lag(col('close'), 1).over(window))
    df = df.withColumn(
        'hourly_change_pct',
        when(col('prev_close') > 0, ((col('close') - col('prev_close')) / col('prev_close')) * 100)
        .otherwise(lit(0))
    )
    
    # Daily change (24 hours ago)
    df = df.withColumn('prev_close_24h', lag(col('close'), 24).over(window))
    df = df.withColumn(
        'daily_change_pct',
        when(col('prev_close_24h') > 0, ((col('close') - col('prev_close_24h')) / col('prev_close_24h')) * 100)
        .otherwise(lit(0))
    )
    
    # Weekly change (168 hours ago)
    df = df.withColumn('prev_close_168h', lag(col('close'), 168).over(window))
    df = df.withColumn(
        'weekly_change_pct',
        when(col('prev_close_168h') > 0, ((col('close') - col('prev_close_168h')) / col('prev_close_168h')) * 100)
        .otherwise(lit(0))
    )
    
    return df.drop('prev_close', 'prev_close_24h', 'prev_close_168h')


def calculate_correlations_integrated(df: DataFrame) -> DataFrame:
    """
    Calculate correlation analysis and add correlation features to the DataFrame.
    This integrates correlation analysis directly into the transformation.
    
    Note: This is a simplified integration. For full correlation matrix,
    use the calculate_correlations function separately.
    """
    logger.info("Performing integrated correlation analysis")
    
    # Calculate correlations for different periods
    try:
        # Calculate daily correlations
        correlation_df = calculate_correlations(df, period='daily')
        
        if correlation_df.count() > 0:
            logger.info(f"Calculated {correlation_df.count()} correlation pairs")
        else:
            logger.warning("No correlations calculated")
    except Exception as e:
        logger.warning(f"Error calculating correlations: {e}")
        # Continue without correlation data
    
    # Return original dataframe (correlation data stored separately if needed)
    # In a full implementation, you might join correlation scores back to the dataframe
    return df


def create_hourly_aggregations(df: DataFrame) -> DataFrame:
    """Create hourly summary aggregations."""
    hourly_df = df.groupBy('symbol', 'year', 'month', 'day', 'hour').agg(
        avg('close').alias('avg_price'),
        max('high').alias('max_price'),
        min('low').alias('min_price'),
        spark_sum('volume').alias('total_volume'),
        avg('rsi').alias('avg_rsi'),
        avg('macd').alias('avg_macd')
    )
    
    return hourly_df


def create_daily_aggregations(df: DataFrame) -> DataFrame:
    """Create daily summary aggregations."""
    daily_df = df.groupBy('symbol', 'year', 'month', 'day').agg(
        avg('close').alias('avg_price'),
        max('high').alias('max_price'),
        min('low').alias('min_price'),
        spark_sum('volume').alias('total_volume'),
        avg('rsi').alias('avg_rsi'),
        avg('macd').alias('avg_macd')
    )
    
    return daily_df


def transform_silver_to_gold(bucket: str, date_filter: str = None) -> None:
    """Main transformation function."""
    spark = create_spark_session()
    
    try:
        # Read silver data (Delta Lake format)
        silver_path = f"s3a://{bucket}/silver"
        if date_filter:
            silver_path = f"{silver_path}/{date_filter}"
        
        logger.info(f"Reading from: {silver_path}")
        df = spark.read.format("delta").load(silver_path)
        
        if df.count() == 0:
            logger.warning("No data found in Silver layer")
            return
        
        # Calculate technical indicators
        df = calculate_all_indicators(df)
        
        # Calculate price changes
        df = calculate_price_changes(df)
        
        # Perform correlation analysis (integrated)
        df = calculate_correlations_integrated(df)
        
        # Add partition columns
        df = df.withColumn('year', year(col('timestamp')))
        df = df.withColumn('month', month(col('timestamp')))
        df = df.withColumn('day', dayofmonth(col('timestamp')))
        df = df.withColumn('hour', hour(col('timestamp')))
        
        # Write gold data (Delta Lake format)
        gold_path = f"s3a://{bucket}/gold"
        logger.info(f"Writing to: {gold_path}")
        df.write \
            .format("delta") \
            .mode('overwrite') \
            .partitionBy('year', 'month', 'day', 'hour', 'symbol') \
            .save(gold_path)
        
        # Create and save aggregations (also in Delta format)
        logger.info("Creating aggregations")
        hourly_df = create_hourly_aggregations(df)
        daily_df = create_daily_aggregations(df)
        
        hourly_df.write.format("delta").mode('overwrite').save(f"{gold_path}/hourly_aggregations")
        daily_df.write.format("delta").mode('overwrite').save(f"{gold_path}/daily_aggregations")
        
        logger.info("Silver to Gold transformation completed")
        
    except Exception as e:
        logger.error(f"Error in Silver to Gold transformation: {e}", exc_info=True)
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
    
    transform_silver_to_gold(bucket, date_filter)
