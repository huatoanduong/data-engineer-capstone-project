"""Correlation analysis between assets."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, corr
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import numpy as np
import logging

logger = logging.getLogger(__name__)


def calculate_correlations(df: DataFrame, period: str = 'daily') -> DataFrame:
    """
    Calculate correlation matrix between assets.
    
    Args:
        df: DataFrame with price data
        period: Time period ('hourly', 'daily', 'weekly')
    
    Returns:
        DataFrame with correlation pairs
    """
    # Pivot data to have assets as columns
    pivot_df = df.select('timestamp', 'symbol', 'close') \
        .groupBy('timestamp') \
        .pivot('symbol') \
        .avg('close')
    
    # Calculate pairwise correlations
    symbols = [c for c in pivot_df.columns if c != 'timestamp']
    correlations = []
    
    # Get Spark session from DataFrame
    spark = df.sql_ctx.sparkSession
    
    for i, sym1 in enumerate(symbols):
        for sym2 in symbols[i+1:]:
            try:
                corr_value = pivot_df.select(corr(sym1, sym2)).collect()[0][0]
                if corr_value is not None:
                    correlations.append({
                        'symbol_1': sym1,
                        'symbol_2': sym2,
                        'correlation': float(corr_value),
                        'period': period
                    })
            except Exception as e:
                logger.warning(f"Error calculating correlation between {sym1} and {sym2}: {e}")
                continue
    
    if correlations:
        return spark.createDataFrame(correlations)
    else:
        # Return empty DataFrame with correct schema
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("symbol_1", StringType(), True),
            StructField("symbol_2", StringType(), True),
            StructField("correlation", DoubleType(), True),
            StructField("period", StringType(), True)
        ])
        return spark.createDataFrame([], schema)


def calculate_correlation_matrix(df: DataFrame) -> np.ndarray:
    """
    Calculate full correlation matrix.
    
    Args:
        df: DataFrame with pivoted price data (assets as columns)
    
    Returns:
        Correlation matrix as numpy array
    """
    # Implementation using PySpark ML
    # First, pivot the data if needed
    if 'symbol' in df.columns:
        pivot_df = df.select('timestamp', 'symbol', 'close') \
            .groupBy('timestamp') \
            .pivot('symbol') \
            .avg('close')
    else:
        pivot_df = df
    
    # Get numeric columns (exclude timestamp)
    numeric_cols = [c for c in pivot_df.columns if c != 'timestamp']
    
    if len(numeric_cols) < 2:
        logger.warning("Not enough columns for correlation matrix")
        return np.array([])
    
    # Use VectorAssembler to create feature vector
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol='features')
    vector_df = assembler.transform(pivot_df)
    
    # Calculate correlation matrix
    matrix = Correlation.corr(vector_df, 'features').head()[0]
    return matrix.toArray()
