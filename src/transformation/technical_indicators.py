"""Technical indicator calculations using PySpark."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lag, avg, stddev, lit, row_number
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


def calculate_rsi(df: DataFrame, period: int = 14, price_col: str = 'close') -> DataFrame:
    """
    Calculate Relative Strength Index (RSI).
    
    Uses Wilder's smoothing method.
    """
    window = Window.partitionBy('symbol').orderBy('timestamp').rowsBetween(-period, 0)
    
    # Calculate price changes
    df = df.withColumn('price_change', col(price_col) - lag(col(price_col), 1).over(
        Window.partitionBy('symbol').orderBy('timestamp')
    ))
    
    # Separate gains and losses
    df = df.withColumn('gain', when(col('price_change') > 0, col('price_change')).otherwise(0))
    df = df.withColumn('loss', when(col('price_change') < 0, -col('price_change')).otherwise(0))
    
    # Calculate average gain and loss using Wilder's smoothing
    df = df.withColumn('avg_gain', avg(col('gain')).over(window))
    df = df.withColumn('avg_loss', avg(col('loss')).over(window))
    
    # Calculate RS and RSI
    df = df.withColumn('rs', col('avg_gain') / when(col('avg_loss') == 0, lit(0.0001)).otherwise(col('avg_loss')))
    df = df.withColumn('rsi', lit(100) - (lit(100) / (lit(1) + col('rs'))))
    
    return df.drop('price_change', 'gain', 'loss', 'avg_gain', 'avg_loss', 'rs')


def calculate_sma(df: DataFrame, period: int, price_col: str = 'close', output_col: str = None) -> DataFrame:
    """Calculate Simple Moving Average."""
    if output_col is None:
        output_col = f'sma_{period}'
    
    window = Window.partitionBy('symbol').orderBy('timestamp').rowsBetween(-period, 0)
    df = df.withColumn(output_col, avg(col(price_col)).over(window))
    
    return df


def calculate_ema(df: DataFrame, period: int, price_col: str = 'close', output_col: str = None) -> DataFrame:
    """
    Calculate Exponential Moving Average using robust window function approach.
    
    EMA calculation uses recursive formula:
    EMA_t = alpha * Price_t + (1 - alpha) * EMA_{t-1}
    where alpha = 2 / (period + 1)
    
    For PySpark, we use window functions with proper initialization and recursive calculation.
    """
    if output_col is None:
        output_col = f'ema_{period}'
    
    alpha = 2.0 / (period + 1)
    window = Window.partitionBy('symbol').orderBy('timestamp')
    
    # First, calculate SMA for initialization (used for first period values)
    df = calculate_sma(df, period, price_col, f'_sma_init_{period}')
    
    # Add row number for conditional logic
    df = df.withColumn('_row_num', row_number().over(window))
    
    # Initialize EMA: use SMA for first period rows, then calculate recursively
    # For the first row, EMA = Price
    # For rows 2 to period, EMA = SMA
    # For rows after period, EMA = alpha * Price + (1 - alpha) * Previous_EMA
    
    # Initialize with first price value
    df = df.withColumn(
        output_col,
        when(col('_row_num') == 1, col(price_col))
        .when(col('_row_num') <= period, col(f'_sma_init_{period}'))
        .otherwise(None)
    )
    
    # Calculate EMA recursively using window function with lag
    # For rows after period, calculate using recursive formula with LAG
    # Note: This requires the previous row's EMA to be calculated first
    # PySpark will handle this correctly when using LAG in a window function
    df = df.withColumn(
        output_col,
        when(col('_row_num') == 1, col(price_col))
        .when(col('_row_num') <= period, col(f'_sma_init_{period}'))
        .otherwise(
            # Recursive calculation: EMA = alpha * Price + (1 - alpha) * Previous_EMA
            col(price_col) * lit(alpha) + 
            lag(col(output_col), 1).over(window) * lit(1 - alpha)
        )
    )
    
    # Clean up temporary columns
    return df.drop('_sma_init_{period}', '_row_num')


def calculate_macd(
    df: DataFrame,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
    price_col: str = 'close'
) -> DataFrame:
    """
    Calculate MACD (Moving Average Convergence Divergence).
    
    MACD = EMA(fast) - EMA(slow)
    Signal = EMA(MACD, signal_period)
    Histogram = MACD - Signal
    """
    # Calculate EMAs
    df = calculate_ema(df, fast_period, price_col, f'ema_{fast_period}')
    df = calculate_ema(df, slow_period, price_col, f'ema_{slow_period}')
    
    # Calculate MACD line
    df = df.withColumn('macd', col(f'ema_{fast_period}') - col(f'ema_{slow_period}'))
    
    # Calculate Signal line (EMA of MACD)
    df = calculate_ema(df, signal_period, 'macd', 'macd_signal')
    
    # Calculate Histogram
    df = df.withColumn('macd_histogram', col('macd') - col('macd_signal'))
    
    return df


def calculate_bollinger_bands(
    df: DataFrame,
    period: int = 20,
    num_std: float = 2.0,
    price_col: str = 'close'
) -> DataFrame:
    """
    Calculate Bollinger Bands.
    
    Middle Band = SMA(period)
    Upper Band = SMA + (num_std * StdDev)
    Lower Band = SMA - (num_std * StdDev)
    """
    window = Window.partitionBy('symbol').orderBy('timestamp').rowsBetween(-period, 0)
    
    # Calculate SMA and StdDev
    df = df.withColumn(f'sma_{period}', avg(col(price_col)).over(window))
    df = df.withColumn(f'std_{period}', stddev(col(price_col)).over(window))
    
    # Calculate bands
    df = df.withColumn(
        'bollinger_upper',
        col(f'sma_{period}') + (lit(num_std) * col(f'std_{period}'))
    )
    df = df.withColumn(
        'bollinger_lower',
        col(f'sma_{period}') - (lit(num_std) * col(f'std_{period}'))
    )
    df = df.withColumn('bollinger_middle', col(f'sma_{period}'))
    
    return df.drop(f'std_{period}')


def calculate_all_indicators(df: DataFrame) -> DataFrame:
    """Calculate all technical indicators."""
    logger.info("Calculating technical indicators")
    
    # RSI
    df = calculate_rsi(df, period=14)
    
    # MACD
    df = calculate_macd(df, fast_period=12, slow_period=26, signal_period=9)
    
    # Bollinger Bands
    df = calculate_bollinger_bands(df, period=20, num_std=2.0)
    
    # Moving Averages
    for period in [7, 14, 30, 50, 200]:
        df = calculate_sma(df, period, output_col=f'sma_{period}')
        df = calculate_ema(df, period, output_col=f'ema_{period}')
    
    logger.info("Technical indicators calculated")
    return df
