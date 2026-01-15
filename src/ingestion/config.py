"""Configuration loader for data ingestion."""

import yaml
from pathlib import Path
from typing import Dict, List, Any


def load_assets() -> Dict[str, List[Dict]]:
    """Load asset list from config/assets.yaml."""
    config_path = Path(__file__).parent.parent.parent / "config" / "assets.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config['assets']


def load_ingestion_config() -> Dict[str, Any]:
    """Load ingestion configuration from pipeline_config.yaml."""
    config_path = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config['pipeline']['ingestion']


def load_aws_config() -> Dict[str, Any]:
    """Load AWS configuration."""
    config_path = Path(__file__).parent.parent.parent / "config" / "aws_config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config['aws']
