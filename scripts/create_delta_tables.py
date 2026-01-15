"""Script to create all Delta Lake tables in Databricks."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from warehouse.delta_schema import create_all_tables
from utils import setup_logger
from ingestion.config import load_aws_config

logger = setup_logger(__name__)


def main():
    """Main function to create all Delta tables."""
    try:
        # Load configuration
        aws_config = load_aws_config()
        bucket_name = aws_config['s3']['bucket_name']
        
        # Get database name from databricks config if available
        try:
            import yaml
            config_path = Path(__file__).parent.parent / "config" / "databricks_config.yaml"
            with open(config_path, 'r') as f:
                databricks_config = yaml.safe_load(f)
            database_name = databricks_config['databricks']['hive_metastore']['database_name']
        except Exception as e:
            logger.warning(f"Could not load databricks config, using default database name: {e}")
            database_name = "market_data"
        
        logger.info(f"Creating Delta Lake tables in bucket: {bucket_name}, database: {database_name}")
        
        # Create all tables
        create_all_tables(bucket_name, database_name)
        
        logger.info("Successfully created all Delta Lake tables")
        
    except Exception as e:
        logger.error(f"Error creating Delta tables: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
