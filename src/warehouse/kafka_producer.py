"""Kafka producer for market data."""

import json
import pandas as pd
from typing import Optional, Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from pathlib import Path
import sys
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import get_logger, read_parquet_from_s3
from ingestion.config import load_aws_config

logger = get_logger(__name__)


def load_kafka_config() -> Dict[str, Any]:
    """Load Kafka configuration from config file."""
    import yaml
    config_path = Path(__file__).parent.parent.parent / "config" / "kafka_config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config['kafka']


class MarketDataProducer:
    """Kafka producer for publishing market data from S3 Gold layer."""
    
    def __init__(self, use_internal_broker: bool = True):
        """
        Initialize Kafka producer.
        
        Args:
            use_internal_broker: If True, use internal Docker network address, else use external
        """
        self.kafka_config = load_kafka_config()
        self.aws_config = load_aws_config()
        
        # Select broker address
        broker = self.kafka_config['broker']['internal'] if use_internal_broker else self.kafka_config['broker']['external']
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True,
            compression_type='snappy'
        )
        
        logger.info(f"Initialized Kafka producer with broker: {broker}")
    
    def read_gold_data(self, date_filter: Optional[str] = None) -> pd.DataFrame:
        """
        Read data from S3 Gold layer.
        
        Args:
            date_filter: Optional date filter in format 'year=YYYY/month=MM/day=DD'
        
        Returns:
            DataFrame with market data
        """
        bucket_name = self.aws_config['s3']['bucket_name']
        gold_path = f"gold"
        
        if date_filter:
            gold_path = f"{gold_path}/{date_filter}"
        
        logger.info(f"Reading Gold data from s3://{bucket_name}/{gold_path}")
        
        try:
            # Read Parquet files from S3
            df = read_parquet_from_s3(bucket_name, gold_path)
            
            if df.empty:
                logger.warning(f"No data found in Gold layer at {gold_path}")
                return df
            
            logger.info(f"Read {len(df)} rows from Gold layer")
            return df
            
        except Exception as e:
            logger.error(f"Error reading Gold data: {e}", exc_info=True)
            raise
    
    def transform_row_to_message(self, row: pd.Series) -> Dict[str, Any]:
        """
        Transform DataFrame row to Kafka message format.
        
        Args:
            row: Pandas Series representing a row
        
        Returns:
            Dictionary with message data
        """
        # Convert row to dict and handle NaN values
        message = row.to_dict()
        
        # Replace NaN with None for JSON serialization
        for key, value in message.items():
            if pd.isna(value):
                message[key] = None
            elif isinstance(value, (pd.Timestamp, datetime)):
                message[key] = value.isoformat()
            elif isinstance(value, (pd.Int64Dtype, pd.Float64Dtype)):
                # Handle nullable integer/float types
                if pd.isna(value):
                    message[key] = None
                else:
                    message[key] = float(value) if 'float' in str(type(value)) else int(value)
        
        # Add schema metadata
        message['_schema_version'] = '1.0'
        message['_ingestion_timestamp'] = datetime.utcnow().isoformat()
        
        return message
    
    def publish_to_kafka(self, df: pd.DataFrame, topic: str, key_column: Optional[str] = None) -> int:
        """
        Publish DataFrame rows to Kafka topic.
        
        Args:
            df: DataFrame to publish
            topic: Kafka topic name
            key_column: Optional column name to use as message key
        
        Returns:
            Number of messages published
        """
        if df.empty:
            logger.warning(f"DataFrame is empty, nothing to publish to {topic}")
            return 0
        
        published_count = 0
        failed_count = 0
        
        try:
            for idx, row in df.iterrows():
                try:
                    # Transform row to message
                    message = self.transform_row_to_message(row)
                    
                    # Determine message key
                    key = None
                    if key_column and key_column in row:
                        key = str(row[key_column])
                    elif 'symbol' in row:
                        key = str(row['symbol'])
                    
                    # Send message
                    future = self.producer.send(topic, key=key, value=message)
                    
                    # Wait for send to complete (with timeout)
                    record_metadata = future.get(timeout=10)
                    published_count += 1
                    
                    if published_count % 100 == 0:
                        logger.debug(f"Published {published_count} messages to {topic}")
                    
                except KafkaError as e:
                    failed_count += 1
                    logger.error(f"Failed to send message {idx} to {topic}: {e}")
                    if failed_count > 10:
                        logger.error("Too many failures, stopping publish")
                        break
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Unexpected error publishing message {idx}: {e}")
            
            # Flush remaining messages
            self.producer.flush()
            
            logger.info(f"Published {published_count} messages to {topic} (failed: {failed_count})")
            return published_count
            
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}", exc_info=True)
            raise
    
    def produce_market_data(self, date_filter: Optional[str] = None) -> Dict[str, int]:
        """
        Main function to produce market data from Gold layer to Kafka.
        
        Args:
            date_filter: Optional date filter in format 'year=YYYY/month=MM/day=DD'
        
        Returns:
            Dictionary with counts of messages published per topic
        """
        try:
            # Read Gold data
            df = self.read_gold_data(date_filter)
            
            if df.empty:
                logger.warning("No data to produce")
                return {}
            
            # Get topic names from config
            topics = self.kafka_config['topics']
            stocks_topic = next((t['name'] for t in topics if t['name'] == 'market-data-stocks'), None)
            crypto_topic = next((t['name'] for t in topics if t['name'] == 'market-data-crypto'), None)
            
            if not stocks_topic or not crypto_topic:
                logger.error("Required topics not found in config")
                raise ValueError("Topics 'market-data-stocks' and 'market-data-crypto' must be configured")
            
            # Separate stocks and crypto
            # Check if asset_type column exists
            if 'asset_type' in df.columns:
                stocks_df = df[df['asset_type'] == 'stocks'].copy()
                crypto_df = df[df['asset_type'] == 'crypto'].copy()
            elif 'symbol' in df.columns:
                # Fallback: try to infer from symbol or use all data
                logger.warning("asset_type column not found, publishing all data to both topics")
                stocks_df = df.copy()
                crypto_df = df.copy()
            else:
                logger.warning("Cannot determine asset type, publishing all data to stocks topic")
                stocks_df = df.copy()
                crypto_df = pd.DataFrame()
            
            results = {}
            
            # Publish stocks data
            if not stocks_df.empty:
                count = self.publish_to_kafka(stocks_df, stocks_topic, key_column='symbol')
                results[stocks_topic] = count
            else:
                logger.info("No stocks data to publish")
                results[stocks_topic] = 0
            
            # Publish crypto data
            if not crypto_df.empty:
                count = self.publish_to_kafka(crypto_df, crypto_topic, key_column='symbol')
                results[crypto_topic] = count
            else:
                logger.info("No crypto data to publish")
                results[crypto_topic] = 0
            
            return results
            
        except Exception as e:
            logger.error(f"Error producing market data: {e}", exc_info=True)
            raise
        finally:
            # Close producer
            self.producer.close()
            logger.info("Kafka producer closed")
    
    def close(self):
        """Close the Kafka producer."""
        self.producer.close()
        logger.info("Kafka producer closed")


def main():
    """Main function for command-line execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Producer for Market Data')
    parser.add_argument('--date-filter', type=str, help='Date filter in format year=YYYY/month=MM/day=DD')
    parser.add_argument('--external-broker', action='store_true', help='Use external broker address')
    
    args = parser.parse_args()
    
    # Setup logging
    from utils import setup_logger
    setup_logger(__name__)
    
    # Create producer and run
    producer = MarketDataProducer(use_internal_broker=not args.external_broker)
    
    try:
        results = producer.produce_market_data(date_filter=args.date_filter)
        logger.info(f"Production complete. Results: {results}")
    except Exception as e:
        logger.error(f"Production failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
