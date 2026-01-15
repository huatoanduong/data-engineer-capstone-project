"""Yahoo Finance data ingestion."""

import yfinance as yf
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import logging

from ..utils import setup_logger, retry_with_backoff, write_parquet_to_s3, generate_partition_path
from .config import load_assets, load_ingestion_config, load_aws_config

logger = setup_logger(__name__)


class YahooFinanceIngester:
    """Ingest market data from Yahoo Finance."""
    
    def __init__(self):
        self.assets_config = load_assets()
        self.ingestion_config = load_ingestion_config()
        self.aws_config = load_aws_config()
        self.bucket = self.aws_config['s3']['bucket_name']
        
    @retry_with_backoff(max_retries=3, initial_delay=1.0)
    def fetch_asset_data(self, symbol: str, asset_type: str) -> Optional[pd.DataFrame]:
        """
        Fetch data for a single asset.
        
        Args:
            symbol: Asset symbol (e.g., 'AAPL', 'BTC-USD')
            asset_type: 'stocks' or 'crypto'
        
        Returns:
            DataFrame with market data or None if failed
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # Get current market data - use 1d period with 1h interval for hourly data
            # For real-time data, we can also use period="1d" with interval="1m" for minute data
            data = ticker.history(period="1d", interval="1h")
            
            if data.empty:
                logger.warning(f"No data available for {symbol}")
                return None
            
            # Extract OHLCV - timestamp is already in the index
            df = pd.DataFrame({
                'timestamp': data.index,
                'symbol': symbol,
                'open': data['Open'].values,
                'high': data['High'].values,
                'low': data['Low'].values,
                'close': data['Close'].values,
                'volume': data['Volume'].values
            })
            
            # Ensure timestamp is a datetime column (not index)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Extract additional metrics from info
            market_cap = info.get('marketCap', None)
            pe_ratio = info.get('trailingPE', None) if asset_type == 'stocks' else None
            
            # Add metrics to all rows (they're the same for the asset)
            df['market_cap'] = market_cap
            df['pe_ratio'] = pe_ratio
            
            # Calculate change percentage
            if len(df) > 1:
                df['change_pct'] = ((df['close'] - df['close'].shift(1)) / df['close'].shift(1)) * 100
                # Fill first row change_pct with 0
                df.loc[0, 'change_pct'] = 0.0
            else:
                df['change_pct'] = 0.0
            
            # Add metadata
            df['asset_type'] = asset_type
            df['ingestion_timestamp'] = datetime.now()
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            raise
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate data schema and business rules."""
        # Check required columns
        required_cols = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            logger.error("Missing required columns")
            return False
        
        # Check for nulls in required fields
        if df[required_cols].isnull().any().any():
            logger.error("Null values in required fields")
            return False
        
        # Business rule validations
        if (df['open'] <= 0).any() or (df['high'] <= 0).any() or (df['low'] <= 0).any() or (df['close'] <= 0).any():
            logger.error("Invalid price values (must be > 0)")
            return False
        
        if (df['volume'] < 0).any():
            logger.error("Invalid volume values (must be >= 0)")
            return False
        
        if 'market_cap' in df.columns and df['market_cap'].notna().any():
            if (df['market_cap'].dropna() < 0).any():
                logger.error("Invalid market cap values")
                return False
        
        return True
    
    def save_to_s3(self, df: pd.DataFrame, symbol: str, asset_type: str, timestamp: datetime) -> bool:
        """Save DataFrame to S3 Bronze layer with partitioning."""
        try:
            # Generate partition path
            base_path = f"{self.aws_config['s3']['bronze_path']}/{asset_type}"
            partition_path = generate_partition_path(
                base_path,
                timestamp.year,
                timestamp.month,
                timestamp.day,
                timestamp.hour,
                symbol
            )
            
            # Generate S3 key
            s3_key = f"{partition_path}data.parquet"
            
            # Write to S3
            success = write_parquet_to_s3(df, self.bucket, s3_key)
            
            if success:
                logger.info(f"Saved data for {symbol} to s3://{self.bucket}/{s3_key}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving data for {symbol} to S3: {e}")
            return False
    
    def ingest_asset(self, symbol: str, asset_type: str) -> bool:
        """Ingest data for a single asset."""
        try:
            logger.info(f"Ingesting data for {symbol} ({asset_type})")
            
            # Fetch data
            df = self.fetch_asset_data(symbol, asset_type)
            if df is None or df.empty:
                return False
            
            # Validate data
            if not self.validate_data(df):
                logger.error(f"Data validation failed for {symbol}")
                return False
            
            # Save to S3 (save each row with its timestamp)
            for _, row in df.iterrows():
                # Convert row to DataFrame
                row_df = pd.DataFrame([row])
                # Convert timestamp if it's a Timestamp object
                row_timestamp = row['timestamp']
                if isinstance(row_timestamp, pd.Timestamp):
                    row_timestamp = row_timestamp.to_pydatetime()
                elif not isinstance(row_timestamp, datetime):
                    row_timestamp = pd.to_datetime(row_timestamp).to_pydatetime()
                
                if not self.save_to_s3(row_df, symbol, asset_type, row_timestamp):
                    return False
            
            logger.info(f"Successfully ingested data for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting {symbol}: {e}")
            return False
    
    def ingest_all_assets(self) -> Dict[str, bool]:
        """Ingest data for all configured assets."""
        results = {}
        
        # Ingest stocks
        for stock in self.assets_config['stocks']:
            symbol = stock['symbol']
            results[symbol] = self.ingest_asset(symbol, 'stocks')
        
        # Ingest crypto
        for crypto in self.assets_config['crypto']:
            symbol = crypto['symbol']
            results[symbol] = self.ingest_asset(symbol, 'crypto')
        
        return results


def main():
    """Main function for standalone execution."""
    ingester = YahooFinanceIngester()
    results = ingester.ingest_all_assets()
    
    successful = sum(1 for v in results.values() if v)
    total = len(results)
    
    print(f"Ingestion complete: {successful}/{total} assets successful")
    
    if successful < total:
        failed = [k for k, v in results.items() if not v]
        print(f"Failed assets: {', '.join(failed)}")


if __name__ == "__main__":
    main()
