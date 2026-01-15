"""Historical data backfill script.

This script backfills historical market data for all configured assets
within a specified date range. It supports batch processing, rate limiting,
progress tracking, and resume capability.
"""

import argparse
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import sys

import yfinance as yf
import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.config import load_assets, load_ingestion_config, load_aws_config
from src.ingestion.yahoo_finance_ingester import YahooFinanceIngester
from src.utils import setup_logger, retry_with_backoff, write_parquet_to_s3, generate_partition_path

logger = setup_logger(__name__)


class HistoricalBackfiller:
    """Backfill historical market data."""
    
    def __init__(self, batch_size: int = 7, rate_limit_delay: float = 1.0):
        """
        Initialize backfiller.
        
        Args:
            batch_size: Number of days to process in each batch
            rate_limit_delay: Delay in seconds between API calls
        """
        self.assets_config = load_assets()
        self.ingestion_config = load_ingestion_config()
        self.aws_config = load_aws_config()
        self.bucket = self.aws_config['s3']['bucket_name']
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        self.progress_file = Path('backfill_progress.json')
        
        # Get backfill config if available
        backfill_config = self.ingestion_config.get('backfill', {})
        if not batch_size:
            self.batch_size = backfill_config.get('batch_size', 7)
        if not rate_limit_delay:
            self.rate_limit_delay = backfill_config.get('rate_limit_delay', 1.0)
    
    @retry_with_backoff(max_retries=3, initial_delay=1.0)
    def fetch_historical_data(
        self, 
        symbol: str, 
        asset_type: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical data for a single asset.
        
        Args:
            symbol: Asset symbol (e.g., 'AAPL', 'BTC-USD')
            asset_type: 'stocks' or 'crypto'
            start_date: Start date for historical data
            end_date: End date for historical data
        
        Returns:
            DataFrame with historical market data or None if failed
        """
        try:
            ticker = yf.Ticker(symbol)
            
            # Fetch historical data
            # Note: yfinance uses date strings in 'YYYY-MM-DD' format
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            logger.info(f"Fetching historical data for {symbol} from {start_str} to {end_str}")
            data = ticker.history(start=start_str, end=end_str, interval='1h')
            
            if data.empty:
                logger.warning(f"No historical data available for {symbol} from {start_str} to {end_str}")
                return None
            
            # Extract OHLCV
            df = pd.DataFrame({
                'timestamp': data.index,
                'symbol': symbol,
                'open': data['Open'].values,
                'high': data['High'].values,
                'low': data['Low'].values,
                'close': data['Close'].values,
                'volume': data['Volume'].values
            })
            
            # Ensure timestamp is a datetime column
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Get info for additional metrics (only once per symbol)
            try:
                info = ticker.info
                market_cap = info.get('marketCap', None)
                pe_ratio = info.get('trailingPE', None) if asset_type == 'stocks' else None
            except Exception as e:
                logger.warning(f"Could not fetch info for {symbol}: {e}")
                market_cap = None
                pe_ratio = None
            
            # Add metrics to all rows
            df['market_cap'] = market_cap
            df['pe_ratio'] = pe_ratio
            
            # Calculate change percentage
            if len(df) > 1:
                df['change_pct'] = ((df['close'] - df['close'].shift(1)) / df['close'].shift(1)) * 100
                df.loc[0, 'change_pct'] = 0.0
            else:
                df['change_pct'] = 0.0
            
            # Add metadata
            df['asset_type'] = asset_type
            df['ingestion_timestamp'] = datetime.now()
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
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
    
    def backfill_asset(
        self, 
        symbol: str, 
        asset_type: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> bool:
        """
        Backfill data for a single asset.
        
        Args:
            symbol: Asset symbol
            asset_type: 'stocks' or 'crypto'
            start_date: Start date
            end_date: End date
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Backfilling {symbol} ({asset_type}) from {start_date.date()} to {end_date.date()}")
            
            # Fetch historical data
            df = self.fetch_historical_data(symbol, asset_type, start_date, end_date)
            if df is None or df.empty:
                logger.warning(f"No data to backfill for {symbol}")
                return True  # Not an error, just no data available
            
            # Validate data
            if not self.validate_data(df):
                logger.error(f"Data validation failed for {symbol}")
                return False
            
            # Save to S3 (save each row with its timestamp)
            success_count = 0
            for _, row in df.iterrows():
                # Convert row to DataFrame
                row_df = pd.DataFrame([row])
                # Convert timestamp if it's a Timestamp object
                row_timestamp = row['timestamp']
                if isinstance(row_timestamp, pd.Timestamp):
                    row_timestamp = row_timestamp.to_pydatetime()
                elif not isinstance(row_timestamp, datetime):
                    row_timestamp = pd.to_datetime(row_timestamp).to_pydatetime()
                
                if self.save_to_s3(row_df, symbol, asset_type, row_timestamp):
                    success_count += 1
                else:
                    logger.error(f"Failed to save data for {symbol} at {row_timestamp}")
                    return False
            
            logger.info(f"Successfully backfilled {success_count} records for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error backfilling {symbol}: {e}")
            return False
    
    def load_progress(self) -> Optional[Dict]:
        """Load progress from file."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading progress file: {e}")
        return None
    
    def save_progress(self, current_date: datetime, symbol: str, asset_type: str):
        """Save progress to file."""
        try:
            progress = {
                'last_date': current_date.strftime('%Y-%m-%d'),
                'last_symbol': symbol,
                'last_asset_type': asset_type,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
    
    def backfill_all_assets(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        resume: bool = False
    ) -> Dict[str, bool]:
        """
        Backfill data for all configured assets.
        
        Args:
            start_date: Start date for backfill
            end_date: End date for backfill
            resume: Whether to resume from last progress
        
        Returns:
            Dictionary mapping symbol to success status
        """
        results = {}
        
        # Load progress if resuming
        progress = None
        if resume:
            progress = self.load_progress()
            if progress:
                last_date_str = progress.get('last_date')
                last_symbol = progress.get('last_symbol')
                last_asset_type = progress.get('last_asset_type')
                if last_date_str:
                    try:
                        resume_date = datetime.strptime(last_date_str, '%Y-%m-%d')
                        logger.info(f"Resuming from {resume_date.date()}, last symbol: {last_symbol}")
                        start_date = resume_date
                    except ValueError:
                        logger.warning(f"Invalid date in progress file: {last_date_str}")
        
        # Process in batches by date
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        processed_days = 0
        
        # Get all assets
        all_assets = []
        for stock in self.assets_config['stocks']:
            all_assets.append((stock['symbol'], 'stocks'))
        for crypto in self.assets_config['crypto']:
            all_assets.append((crypto['symbol'], 'crypto'))
        
        logger.info(f"Starting backfill for {len(all_assets)} assets from {start_date.date()} to {end_date.date()}")
        logger.info(f"Batch size: {self.batch_size} days, Rate limit delay: {self.rate_limit_delay}s")
        
        while current_date <= end_date:
            batch_end = min(current_date + timedelta(days=self.batch_size - 1), end_date)
            
            logger.info(f"\nProcessing batch: {current_date.date()} to {batch_end.date()}")
            
            # Process each asset in the batch
            for symbol, asset_type in all_assets:
                # Skip if resuming and we haven't reached the last symbol yet
                if resume and progress:
                    last_symbol = progress.get('last_symbol')
                    last_asset_type = progress.get('last_asset_type')
                    if last_symbol and last_asset_type:
                        # Skip until we reach the last processed symbol
                        if (asset_type, symbol) < (last_asset_type, last_symbol):
                            continue
                        elif (asset_type, symbol) == (last_asset_type, last_symbol):
                            # Start from the next date after last_date
                            resume = False
                            continue
                
                # Backfill this asset for the current batch
                success = self.backfill_asset(symbol, asset_type, current_date, batch_end)
                results[f"{symbol}_{asset_type}"] = success
                
                # Save progress after each asset
                self.save_progress(batch_end, symbol, asset_type)
                
                # Rate limiting
                if self.rate_limit_delay > 0:
                    time.sleep(self.rate_limit_delay)
            
            # Move to next batch
            current_date = batch_end + timedelta(days=1)
            processed_days += (batch_end - current_date + timedelta(days=self.batch_size)).days
            
            # Progress update
            progress_pct = (processed_days / total_days) * 100 if total_days > 0 else 0
            logger.info(f"Progress: {processed_days}/{total_days} days ({progress_pct:.1f}%)")
        
        return results


def main():
    """Main function for CLI execution."""
    parser = argparse.ArgumentParser(
        description='Backfill historical market data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill last 30 days
  python scripts/backfill_historical_data.py --start-date 2024-01-01 --end-date 2024-01-31
  
  # Resume interrupted backfill
  python scripts/backfill_historical_data.py --start-date 2024-01-01 --end-date 2024-01-31 --resume
  
  # Custom batch size and rate limit
  python scripts/backfill_historical_data.py --start-date 2024-01-01 --end-date 2024-01-31 --batch-size 14 --delay 2.0
        """
    )
    
    parser.add_argument(
        '--start-date',
        required=True,
        type=str,
        help='Start date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--end-date',
        required=True,
        type=str,
        help='End date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from last progress checkpoint'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=None,
        help='Number of days to process per batch (default: from config or 7)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=None,
        help='Delay in seconds between API calls (default: from config or 1.0)'
    )
    
    args = parser.parse_args()
    
    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        logger.error(f"Invalid date format: {e}. Use YYYY-MM-DD format.")
        sys.exit(1)
    
    # Validate date range
    if start_date > end_date:
        logger.error("Start date must be before or equal to end date")
        sys.exit(1)
    
    if start_date > datetime.now():
        logger.error("Start date cannot be in the future")
        sys.exit(1)
    
    # Initialize backfiller
    backfiller = HistoricalBackfiller(
        batch_size=args.batch_size,
        rate_limit_delay=args.delay
    )
    
    # Run backfill
    logger.info("=" * 60)
    logger.info("Starting Historical Data Backfill")
    logger.info("=" * 60)
    
    results = backfiller.backfill_all_assets(start_date, end_date, resume=args.resume)
    
    # Summary
    successful = sum(1 for v in results.values() if v)
    total = len(results)
    
    logger.info("=" * 60)
    logger.info("Backfill Complete")
    logger.info("=" * 60)
    logger.info(f"Successfully processed: {successful}/{total} asset-date combinations")
    
    if successful < total:
        failed = [k for k, v in results.items() if not v]
        logger.warning(f"Failed asset-date combinations: {', '.join(failed)}")
        sys.exit(1)
    else:
        logger.info("All backfills completed successfully!")
        # Clean up progress file on success
        if backfiller.progress_file.exists():
            backfiller.progress_file.unlink()
            logger.info("Progress file cleaned up")


if __name__ == "__main__":
    main()
