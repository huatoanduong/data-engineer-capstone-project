#!/usr/bin/env python3
"""
Databricks Setup Script

This script sets up initial Databricks configuration including:
- S3 mount configuration
- Database/schema creation
- Initial table structure (basic setup, full schema in Plan 5.1)
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.compute import DataSecurityMode, ClusterSpec
    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False
    print("Warning: databricks-sdk not installed. Some features may not work.")
    print("Install with: pip install databricks-sdk")

def load_config() -> Dict[str, Any]:
    """Load Databricks configuration."""
    config_path = Path(__file__).parent.parent / "config" / "databricks_config.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def create_databricks_client():
    """Create Databricks workspace client."""
    if not DATABRICKS_SDK_AVAILABLE:
        print("Error: databricks-sdk is required for this script")
        print("Install with: pip install databricks-sdk")
        return None
    
    token = os.getenv('DATABRICKS_TOKEN')
    workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL')
    
    if not token or not workspace_url:
        raise ValueError("DATABRICKS_TOKEN and DATABRICKS_WORKSPACE_URL must be set as environment variables")
    
    return WorkspaceClient(
        host=workspace_url,
        token=token
    )

def setup_s3_mount(client: WorkspaceClient, config: dict):
    """
    Set up S3 mount point (optional).
    
    Note: This typically needs to be done via Databricks notebook using dbutils.
    For CLI approach, you would need to use Databricks CLI or REST API.
    """
    print("Note: S3 mount setup should be done via Databricks notebook.")
    print("Use the following code in a Databricks notebook:")
    print("""
    dbutils.fs.mount(
        source="s3a://{bucket-name}",
        mount_point="/mnt/market-data",
        extra_configs={
            "fs.s3a.access.key": dbutils.secrets.get(scope="aws", key="access-key-id"),
            "fs.s3a.secret.key": dbutils.secrets.get(scope="aws", key="secret-access-key")
        }
    )
    """)
    pass

def create_database_sql(config: dict) -> str:
    """Generate SQL command to create database."""
    db_config = config['databricks']['hive_metastore']
    s3_config = config['databricks']['s3']
    
    # Replace environment variables in delta_path
    delta_path = s3_config['delta_path'].replace('${S3_BUCKET_NAME}', os.getenv('S3_BUCKET_NAME', ''))
    
    sql = f"""
    CREATE DATABASE IF NOT EXISTS {db_config['database_name']}
    LOCATION '{delta_path}'
    """
    return sql.strip()

def print_setup_instructions(config: dict):
    """Print instructions for manual setup."""
    print("\n" + "="*60)
    print("Databricks Setup Instructions")
    print("="*60)
    
    print("\n1. Create a Databricks notebook and run the following:")
    print("\n   # Set up S3 access")
    print("   spark.conf.set('spark.hadoop.fs.s3a.access.key', dbutils.secrets.get(scope='aws', key='access-key-id'))")
    print("   spark.conf.set('spark.hadoop.fs.s3a.secret.key', dbutils.secrets.get(scope='aws', key='secret-access-key'))")
    print("   spark.conf.set('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com')")
    
    print("\n   # Or use environment variables (if set in cluster config):")
    print("   # AWS credentials should be set as Spark environment variables")
    
    print("\n2. Create database:")
    sql = create_database_sql(config)
    print(f"   {sql}")
    
    print("\n3. Verify S3 access:")
    bucket_name = os.getenv('S3_BUCKET_NAME', config['databricks']['s3']['bucket_name'].replace('${S3_BUCKET_NAME}', ''))
    print(f"   dbutils.fs.ls('s3://{bucket_name}/')")
    
    print("\n4. Verify database creation:")
    db_name = config['databricks']['hive_metastore']['database_name']
    print(f"   spark.sql('SHOW DATABASES').show()")
    print(f"   spark.sql('USE {db_name}')")
    
    print("\n" + "="*60)
    print("Note: Full table schema will be created in Plan 5.1")
    print("="*60 + "\n")

def main():
    """Main setup function."""
    print("Databricks Setup Script")
    print("="*60)
    
    # Load configuration
    try:
        config = load_config()
    except FileNotFoundError:
        print("Error: config/databricks_config.yaml not found")
        return
    
    # Check environment variables
    token = os.getenv('DATABRICKS_TOKEN')
    workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL')
    
    if not token or not workspace_url:
        print("Warning: DATABRICKS_TOKEN and/or DATABRICKS_WORKSPACE_URL not set")
        print("These should be set in your .env file")
        print("\nProceeding with manual setup instructions...\n")
        print_setup_instructions(config)
        return
    
    # Try to create client and perform automated setup
    if DATABRICKS_SDK_AVAILABLE:
        try:
            client = create_databricks_client()
            if client:
                print("✓ Connected to Databricks workspace")
                print(f"  Workspace URL: {workspace_url}")
                
                # List clusters to verify connection
                try:
                    clusters = list(client.clusters.list())
                    print(f"✓ Found {len(clusters)} cluster(s)")
                except Exception as e:
                    print(f"  Note: Could not list clusters: {e}")
                
                print("\nNote: Database and table creation should be done via Databricks notebook")
                print("See instructions below:\n")
        except Exception as e:
            print(f"Error connecting to Databricks: {e}")
            print("\nProceeding with manual setup instructions...\n")
    else:
        print("databricks-sdk not available. Showing manual setup instructions...\n")
    
    # Print manual setup instructions
    print_setup_instructions(config)

if __name__ == "__main__":
    main()
