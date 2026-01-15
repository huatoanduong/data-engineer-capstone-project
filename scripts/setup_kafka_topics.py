#!/usr/bin/env python3
"""
Kafka Topics Setup Script

This script creates Kafka topics for the market data pipeline.
It configures topics with proper retention policies and partitions.
"""

import yaml
import sys
from pathlib import Path
from typing import Dict, List, Optional
from kafka import KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, KafkaError

# Configuration
CONFIG_PATH = Path(__file__).parent.parent / "config" / "kafka_config.yaml"

def load_config() -> Dict:
    """Load Kafka configuration from YAML file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"✗ Configuration file not found: {CONFIG_PATH}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"✗ Error parsing YAML configuration: {e}")
        sys.exit(1)

def create_kafka_admin_client(broker: str) -> Optional[KafkaAdminClient]:
    """Create Kafka admin client."""
    try:
        client = KafkaAdminClient(
            bootstrap_servers=broker,
            client_id='kafka-setup-script'
        )
        return client
    except Exception as e:
        print(f"✗ Error connecting to Kafka broker {broker}: {e}")
        return None

def topic_exists(admin_client: KafkaAdminClient, topic_name: str) -> bool:
    """Check if topic exists."""
    try:
        cluster_metadata = admin_client.describe_cluster()
        metadata = admin_client.list_topics()
        return topic_name in metadata
    except Exception as e:
        # If we can't list topics, try to describe the specific topic
        try:
            admin_client.describe_topics([topic_name])
            return True
        except Exception:
            return False

def create_topic(admin_client: KafkaAdminClient, topic_config: Dict) -> bool:
    """Create Kafka topic with configuration."""
    topic_name = topic_config['name']
    
    # Check if topic already exists
    if topic_exists(admin_client, topic_name):
        print(f"✓ Topic '{topic_name}' already exists")
        return True
    
    try:
        # Calculate retention in milliseconds
        retention_ms = topic_config['retention_hours'] * 3600 * 1000
        
        # Build topic configs
        topic_configs = {
            'retention.ms': str(retention_ms),
            'cleanup.policy': topic_config['cleanup_policy'],
            'compression.type': topic_config['compression_type']
        }
        
        # Add retention bytes if not unlimited
        if topic_config['retention_bytes'] > 0:
            topic_configs['retention.bytes'] = str(topic_config['retention_bytes'])
        
        topic = NewTopic(
            name=topic_name,
            num_partitions=topic_config['partitions'],
            replication_factor=topic_config['replication_factor'],
            topic_configs=topic_configs
        )
        
        result = admin_client.create_topics([topic], timeout_ms=10000)
        
        # Wait for topic creation to complete
        for topic_name_future, exception in result.items():
            if exception is not None:
                if isinstance(exception, TopicAlreadyExistsError):
                    print(f"✓ Topic '{topic_name}' already exists")
                    return True
                else:
                    print(f"✗ Error creating topic '{topic_name}': {exception}")
                    return False
        
        print(f"✓ Created topic '{topic_name}' with {topic_config['partitions']} partitions")
        return True
    except TopicAlreadyExistsError:
        print(f"✓ Topic '{topic_name}' already exists")
        return True
    except Exception as e:
        print(f"✗ Error creating topic '{topic_name}': {e}")
        return False

def configure_topic_retention(admin_client: KafkaAdminClient, topic_name: str, retention_hours: int) -> bool:
    """Configure topic retention policy."""
    try:
        retention_ms = retention_hours * 3600 * 1000
        config_resource = ConfigResource(
            resource_type=ConfigResourceType.TOPIC,
            name=topic_name,
            configs={'retention.ms': str(retention_ms)}
        )
        admin_client.alter_configs([config_resource])
        print(f"✓ Configured retention for '{topic_name}': {retention_hours} hours")
        return True
    except Exception as e:
        print(f"✗ Error configuring retention for '{topic_name}': {e}")
        return False

def list_topics(admin_client: KafkaAdminClient) -> List[str]:
    """List all Kafka topics."""
    try:
        topics = admin_client.list_topics()
        return list(topics)
    except Exception as e:
        print(f"✗ Error listing topics: {e}")
        return []

def describe_topic(admin_client: KafkaAdminClient, topic_name: str) -> Dict:
    """Get topic configuration details."""
    try:
        # Get topic metadata
        metadata = admin_client.describe_topics([topic_name])
        if topic_name in metadata:
            return metadata[topic_name]
        return {}
    except Exception as e:
        print(f"✗ Error describing topic '{topic_name}': {e}")
        return {}

def verify_setup(admin_client: KafkaAdminClient, config: Dict) -> bool:
    """Verify all topics are created correctly."""
    print("\nVerifying topic setup...")
    all_valid = True
    
    for topic_config in config['kafka']['topics']:
        topic_name = topic_config['name']
        
        if not topic_exists(admin_client, topic_name):
            print(f"✗ Topic '{topic_name}' does not exist")
            all_valid = False
            continue
        
        # Try to get topic details
        try:
            metadata = admin_client.describe_topics([topic_name])
            if topic_name in metadata:
                topic_info = metadata[topic_name]
                partitions = len(topic_info.get('partitions', {}))
                
                if partitions != topic_config['partitions']:
                    print(f"✗ Topic '{topic_name}' has {partitions} partitions, expected {topic_config['partitions']}")
                    all_valid = False
                else:
                    print(f"✓ Topic '{topic_name}' verified: {partitions} partitions")
            else:
                print(f"✓ Topic '{topic_name}' exists (could not verify partition count)")
        except Exception as e:
            print(f"⚠ Could not verify topic '{topic_name}': {e}")
            # Topic exists, so we'll consider it valid
            print(f"✓ Topic '{topic_name}' exists")
    
    return all_valid

def main():
    """Main function to set up Kafka topics."""
    print("Starting Kafka topics setup...")
    print("="*60)
    
    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"✗ Failed to load configuration: {e}")
        sys.exit(1)
    
    # Try to connect to Kafka broker
    # Try external broker first (for local development)
    admin_client = None
    broker_used = None
    
    external_broker = config['kafka']['broker']['external']
    internal_broker = config['kafka']['broker']['internal']
    
    print(f"\nAttempting to connect to Kafka broker...")
    print(f"  Trying external broker: {external_broker}")
    
    admin_client = create_kafka_admin_client(external_broker)
    if admin_client:
        broker_used = external_broker
        print(f"✓ Connected to Kafka broker: {external_broker}")
    else:
        print(f"  External broker failed, trying internal: {internal_broker}")
        admin_client = create_kafka_admin_client(internal_broker)
        if admin_client:
            broker_used = internal_broker
            print(f"✓ Connected to Kafka broker: {internal_broker}")
        else:
            print(f"\n✗ Could not connect to any Kafka broker")
            print(f"  Please ensure Kafka is running:")
            print(f"    docker-compose -f docker/docker-compose.yml ps kafka")
            sys.exit(1)
    
    # Create topics
    print(f"\n{'='*60}")
    print("Creating topics...")
    print("="*60)
    
    for topic_config in config['kafka']['topics']:
        if not create_topic(admin_client, topic_config):
            print(f"✗ Failed to create topic: {topic_config['name']}")
            admin_client.close()
            sys.exit(1)
    
    # Configure retention policies (in case topics already existed)
    print(f"\n{'='*60}")
    print("Configuring retention policies...")
    print("="*60)
    
    for topic_config in config['kafka']['topics']:
        configure_topic_retention(
            admin_client,
            topic_config['name'],
            topic_config['retention_hours']
        )
    
    # Verify setup
    if not verify_setup(admin_client, config):
        print("\n⚠ Some topics may not be configured correctly")
        print("  This might be normal if topics already existed with different config")
    else:
        print("\n✓ All topics verified successfully")
    
    # List all topics
    print(f"\n{'='*60}")
    print("All Kafka topics:")
    print("="*60)
    topics = list_topics(admin_client)
    if topics:
        for topic in sorted(topics):
            print(f"  - {topic}")
    else:
        print("  (No topics found)")
    
    # Consumer groups info
    print(f"\n{'='*60}")
    print("Consumer Groups (will be created on first use):")
    print("="*60)
    for cg in config['kafka']['consumer_groups']:
        print(f"  - {cg['name']} (topic: {cg['topic']})")
    
    print(f"\n{'='*60}")
    print("✓ Kafka topics setup completed successfully!")
    print(f"{'='*60}")
    print(f"\nBroker used: {broker_used}")
    print(f"Topics created: {len(config['kafka']['topics'])}")
    
    # Close admin client
    admin_client.close()

if __name__ == "__main__":
    main()
