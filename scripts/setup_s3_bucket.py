#!/usr/bin/env python3
"""
S3 Bucket Setup Script

This script creates the S3 bucket structure for the data pipeline.
It sets up bronze, silver, and gold layers with proper partitioning.
"""

import boto3
import yaml
import os
import sys
from pathlib import Path
from typing import Dict, Any, List
from botocore.exceptions import ClientError

# Configuration
CONFIG_PATH = Path(__file__).parent.parent / "config" / "aws_config.yaml"

def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

def create_s3_client(region: str) -> boto3.client:
    """Create S3 client using environment variables or IAM role."""
    return boto3.client('s3', region_name=region)

def create_bucket(s3_client: boto3.client, bucket_name: str, region: str) -> bool:
    """Create S3 bucket if it doesn't exist."""
    try:
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print(f"✓ Bucket '{bucket_name}' created successfully")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyExists':
            print(f"✓ Bucket '{bucket_name}' already exists")
            return True
        elif e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"✓ Bucket '{bucket_name}' already owned by you")
            return True
        else:
            print(f"✗ Error creating bucket: {e}")
            return False

def create_directory_structure(s3_client: boto3.client, bucket_name: str, config: Dict) -> bool:
    """Create directory structure by creating placeholder files."""
    paths = [
        f"{config['aws']['s3']['bronze_path']}/stocks/.gitkeep",
        f"{config['aws']['s3']['bronze_path']}/crypto/.gitkeep",
        f"{config['aws']['s3']['silver_path']}/stocks/.gitkeep",
        f"{config['aws']['s3']['silver_path']}/crypto/.gitkeep",
        f"{config['aws']['s3']['gold_path']}/stocks/.gitkeep",
        f"{config['aws']['s3']['gold_path']}/crypto/.gitkeep",
        f"{config['aws']['s3']['delta_path']}/.gitkeep",
    ]
    
    for path in paths:
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=path,
                Body=b''
            )
            print(f"✓ Created directory: {path}")
        except ClientError as e:
            print(f"✗ Error creating directory {path}: {e}")
            return False
    
    return True

def setup_lifecycle_policies(s3_client: boto3.client, bucket_name: str, config: Dict) -> bool:
    """Configure lifecycle policies."""
    lifecycle_config = {
        'Rules': [
            {
                'Id': 'BronzeLayerLifecycle',
                'Status': 'Enabled',
                'Prefix': config['aws']['s3']['bronze_path'],
                'Expiration': {'Days': config['aws']['lifecycle']['bronze_retention_days']}
            },
            {
                'Id': 'SilverLayerLifecycle',
                'Status': 'Enabled',
                'Prefix': config['aws']['s3']['silver_path'],
                'Transitions': [
                    {
                        'Days': config['aws']['lifecycle']['transition_to_glacier_days'],
                        'StorageClass': 'GLACIER'
                    }
                ],
                'Expiration': {'Days': config['aws']['lifecycle']['silver_retention_days']}
            },
            {
                'Id': 'GoldLayerLifecycle',
                'Status': 'Enabled',
                'Prefix': config['aws']['s3']['gold_path'],
                'Transitions': [
                    {
                        'Days': config['aws']['lifecycle']['transition_to_glacier_days'],
                        'StorageClass': 'GLACIER'
                    }
                ],
                'Expiration': {'Days': config['aws']['lifecycle']['gold_retention_days']}
            }
        ]
    }
    
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        print("✓ Lifecycle policies configured")
        return True
    except ClientError as e:
        print(f"✗ Error configuring lifecycle policies: {e}")
        return False

def enable_versioning(s3_client: boto3.client, bucket_name: str) -> bool:
    """Enable versioning on bucket."""
    try:
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        print("✓ Versioning enabled")
        return True
    except ClientError as e:
        print(f"✗ Error enabling versioning: {e}")
        return False

def enable_encryption(s3_client: boto3.client, bucket_name: str) -> bool:
    """Enable server-side encryption."""
    try:
        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                'Rules': [{
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                }]
            }
        )
        print("✓ Encryption enabled")
        return True
    except ClientError as e:
        print(f"✗ Error enabling encryption: {e}")
        return False

def verify_setup(s3_client: boto3.client, bucket_name: str) -> bool:
    """Verify bucket setup."""
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        
        # List objects to verify structure
        response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        prefixes = [p['Prefix'] for p in response.get('CommonPrefixes', [])]
        
        expected_prefixes = ['bronze/', 'silver/', 'gold/', 'delta/']
        for prefix in expected_prefixes:
            if prefix not in prefixes:
                print(f"✗ Missing expected prefix: {prefix}")
                return False
        
        print("✓ Bucket setup verified")
        return True
    except ClientError as e:
        print(f"✗ Verification failed: {e}")
        return False

def main():
    """Main function."""
    print("Starting S3 bucket setup...")
    
    # Load configuration
    config = load_config()
    bucket_name = config['aws']['s3']['bucket_name']
    region = config['aws']['region']
    
    # Create S3 client
    s3_client = create_s3_client(region)
    
    # Execute setup steps
    steps = [
        ("Creating bucket", lambda: create_bucket(s3_client, bucket_name, region)),
        ("Creating directory structure", lambda: create_directory_structure(s3_client, bucket_name, config)),
        ("Configuring lifecycle policies", lambda: setup_lifecycle_policies(s3_client, bucket_name, config)),
        ("Enabling versioning", lambda: enable_versioning(s3_client, bucket_name)),
        ("Enabling encryption", lambda: enable_encryption(s3_client, bucket_name)),
        ("Verifying setup", lambda: verify_setup(s3_client, bucket_name)),
    ]
    
    for step_name, step_func in steps:
        print(f"\n{step_name}...")
        if not step_func():
            print(f"✗ Failed at step: {step_name}")
            sys.exit(1)
    
    print("\n✓ S3 bucket setup completed successfully!")
    print(f"Bucket name: {bucket_name}")
    print(f"Region: {region}")

if __name__ == "__main__":
    main()
