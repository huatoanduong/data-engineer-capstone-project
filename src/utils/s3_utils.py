"""S3 utility functions for data operations."""

import boto3
import pandas as pd
from io import BytesIO
from typing import Optional, List
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_s3_client():
    """Get S3 client using environment variables."""
    return boto3.client('s3')


def generate_partition_path(
    base_path: str,
    year: int,
    month: int,
    day: int,
    hour: int,
    symbol: str,
    layer: str = "bronze"
) -> str:
    """
    Generate S3 partition path.
    
    Args:
        base_path: Base S3 path (e.g., 'bronze/stocks')
        year: Year (YYYY)
        month: Month (1-12)
        day: Day (1-31)
        hour: Hour (0-23)
        symbol: Asset symbol
        layer: Data layer (bronze, silver, gold)
    
    Returns:
        Partition path string
    """
    return (
        f"{base_path}/"
        f"year={year:04d}/"
        f"month={month:02d}/"
        f"day={day:02d}/"
        f"hour={hour:02d}/"
        f"symbol={symbol}/"
    )


def write_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    partition_cols: Optional[List[str]] = None
) -> bool:
    """
    Write DataFrame to S3 as Parquet file.
    
    Args:
        df: DataFrame to write
        bucket: S3 bucket name
        key: S3 key (path)
        partition_cols: Optional list of partition columns
    
    Returns:
        True if successful, False otherwise
    """
    try:
        s3_client = get_s3_client()
        
        # Convert DataFrame to Parquet bytes
        parquet_buffer = BytesIO()
        df.to_parquet(
            parquet_buffer,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        parquet_buffer.seek(0)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buffer.read()
        )
        
        logger.info(f"Successfully wrote Parquet file to s3://{bucket}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error writing Parquet to S3: {e}")
        return False


def read_parquet_from_s3(
    bucket: str,
    key: str,
    filters: Optional[List[tuple]] = None
) -> pd.DataFrame:
    """
    Read Parquet file from S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 key (path) or prefix
        filters: Optional list of filters for partitioned reads
    
    Returns:
        DataFrame
    """
    try:
        s3_client = get_s3_client()
        
        # If key is a prefix, read all files
        if key.endswith('/'):
            # List all objects with this prefix
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
            if 'Contents' not in response:
                logger.warning(f"No objects found with prefix: {key}")
                return pd.DataFrame()
            
            # Read all Parquet files
            dfs = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    obj_response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                    df = pd.read_parquet(BytesIO(obj_response['Body'].read()))
                    dfs.append(df)
            
            if dfs:
                return pd.concat(dfs, ignore_index=True)
            return pd.DataFrame()
        else:
            # Read single file
            response = s3_client.get_object(Bucket=bucket, Key=key)
            return pd.read_parquet(BytesIO(response['Body'].read()))
    
    except Exception as e:
        logger.error(f"Error reading Parquet from S3: {e}")
        raise


def list_s3_partitions(
    bucket: str,
    prefix: str
) -> List[str]:
    """
    List all partition paths in S3.
    
    Args:
        bucket: S3 bucket name
        prefix: Prefix to search under
    
    Returns:
        List of partition paths
    """
    try:
        s3_client = get_s3_client()
        partitions = []
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
        
        for page in pages:
            if 'CommonPrefixes' in page:
                partitions.extend([p['Prefix'] for p in page['CommonPrefixes']])
        
        return partitions
    except Exception as e:
        logger.error(f"Error listing S3 partitions: {e}")
        return []


def file_exists(bucket: str, key: str) -> bool:
    """Check if file exists in S3."""
    try:
        s3_client = get_s3_client()
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404' or error_code == 'NoSuchKey':
            return False
        # Re-raise for other client errors
        logger.error(f"Error checking file existence: {e}")
        return False
    except Exception as e:
        logger.error(f"Error checking file existence: {e}")
        return False
