import os
import sys
import json
import uuid
import logging  
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict

# Import the TopicConsumer from the kafka package
from kafka_consumer.topic_consumer import TopicConsumer


# Import the LoggerManager from the utils package
from utils.logger_manager import LoggerManager


def run_consumer(
    topic: str,
    group_id: str,
    num_messages: int,
    bootstrap_servers: str,
    ssl_config: Dict[str, str],
    schema_registry_url: str,
    azure_storage_account_name: str,
    azure_storage_account_key: str,
    azure_filesystem_name: str,
    reset_offsets: bool = False,
    starting_offsets: Optional[Dict[int, int]] = None,
    offset_increment: Optional[Dict[int, int]] = None,
    logger_manager: Optional[LoggerManager] = None,
    base_log_dir: str = 'logging',
) -> None:
    """
    Initializes and runs a Kafka topic consumer.

    Parameters:
        topic (str): The Kafka topic to consume from.
        group_id (str): The consumer group ID.
        num_messages (int): Number of messages to consume.
        bootstrap_servers (str): Kafka bootstrap servers.
        ssl_config (Dict[str, str]): SSL configuration for Kafka.
        schema_registry_url (str): URL for the Schema Registry.
        azure_storage_account_name (str): Azure storage account name.
        azure_storage_account_key (str): Azure storage account key.
        azure_filesystem_name (str): Azure filesystem name.
        reset_offsets (bool, optional): Whether to reset offsets to beginning. Defaults to False.
        starting_offsets (Optional[Dict[int, int]], optional): Starting offsets for partitions. Defaults to None.
        offset_increment (Optional[Dict[int, int]], optional): Offset increments to adjust offsets. Defaults to None.
        logger_manager (Optional[LoggerManager], optional): Instance of LoggerManager. Defaults to None.
        base_log_dir (str, optional): Base directory for logs. Defaults to 'logging'.
    """
    if logger_manager is None:
        logger_manager = LoggerManager(base_log_dir=base_log_dir)

    # Generate a unique identifier for this run
    run_id = str(uuid.uuid4())

    # Set up the topic-specific logger
    adapter = logger_manager.get_topic_logger(topic, run_id)

    try:
        adapter.info(f"Starting consumer for topic: {topic}, Run ID: {run_id}")
        # Initialize the TopicConsumer with provided configurations
        consumer = TopicConsumer(
            topic=topic,
            group_id=group_id,
            num_messages=num_messages,
            bootstrap_servers=bootstrap_servers,
            ssl_config=ssl_config,
            schema_registry_url=schema_registry_url,
            azure_storage_account_name=azure_storage_account_name,
            azure_storage_account_key=azure_storage_account_key,
            azure_filesystem_name=azure_filesystem_name,
            reset_offsets=reset_offsets,
            starting_offsets=starting_offsets,
            offset_increment=offset_increment,
            logger=adapter,
            run_id=run_id,
        )
        
        # Run the consumer process 
        consumer.run()

        adapter.info(f"Consumer for topic {topic} completed successfully.")
    except Exception as e:
        adapter.error(f"Error in run_consumer for topic {topic}: {e}.")


def main() -> None:
    """
    Main function to load configurations and start Kafka topic consumers in parallel.
    """
    # Get the absolute path to the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.abspath(os.path.join(script_dir, '..'))

    # Define base log directory
    base_log_dir = os.path.join(base_dir, 'logging')

    # Initialize LoggerManager
    logger_manager = LoggerManager(base_log_dir=base_log_dir)

    # Get the application logger (root logger)
    app_logger = logging.getLogger()
    app_logger.info("Application logger initialized.")

    # Add logging statements to debug paths
    app_logger.info(f"Script directory: {script_dir}")
    app_logger.info(f"Current working directory: {os.getcwd()}")
    app_logger.info(f"__file__: {__file__}")

    # Load configurations from files
    connections_config_path = os.path.join(base_dir, 'configurations', 'connections.json')
    topics_config_path = os.path.join(base_dir, 'configurations', 'topics.json')

    app_logger.info(f"Connections config path: {connections_config_path}")
    app_logger.info(f"Topics config path: {topics_config_path}")

    # Check if configuration files exist
    if not os.path.exists(connections_config_path):
        app_logger.error(f"Connections configuration file not found at {connections_config_path}")
        sys.exit(1)

    if not os.path.exists(topics_config_path):
        app_logger.error(f"Topics configuration file not found at {topics_config_path}")
        sys.exit(1)

    # Read connections config
    try:
        with open(connections_config_path, 'r') as f:
            connections_config = json.load(f)
    except Exception as e:
        app_logger.error(f"Failed to load connections configuration: {e}")
        sys.exit(1)

    # Resolve SSL certificate paths to absolute paths
    ssl_config = connections_config.get('ssl_config')
    if ssl_config:
        # Get the directory where connections_config.json is located
        connections_dir = os.path.dirname(connections_config_path)
        for key in ['ca', 'cert', 'key']:
            if ssl_config.get(key):
                # Resolve the path relative to the connections directory
                ssl_config[key] = os.path.abspath(os.path.join(connections_dir, ssl_config[key]))
                # Log the resolved path for debugging
                app_logger.info(f"Resolved {key} path: {ssl_config[key]}")

    # Update the connections_config with the resolved ssl_config
    connections_config['ssl_config'] = ssl_config

    # Extract connectivity configurations
    bootstrap_servers = connections_config.get('bootstrap_servers')
    azure_storage_account_name = connections_config.get('azure_storage_account_name')
    azure_storage_account_key = connections_config.get('azure_storage_account_key')
    azure_filesystem_name = connections_config.get('azure_filesystem_name')
    schema_registry_url = connections_config.get('schema_registry_url')

    # Validate configurations
    if not all(
        [
            bootstrap_servers,
            ssl_config.get('ca'),
            ssl_config.get('cert'),
            ssl_config.get('key'),
            schema_registry_url,
        ]
    ):
        app_logger.error("Kafka configurations are not properly set in configuration files.")
        sys.exit(1)

    # Read topics config
    try:
        with open(topics_config_path, 'r') as f:
            topics_config = json.load(f)
    except Exception as e:
        app_logger.error(f"Failed to load topics configuration: {e}")
        sys.exit(1)

    # Use a ThreadPoolExecutor to run multiple consumers in parallel
    while True:
        with ThreadPoolExecutor(max_workers=len(topics_config)) as executor:
            futures = []
            for config in topics_config:
                # Process starting_offsets and offset_increment to convert keys to integers
                starting_offsets = None
                offset_increment = None
                if config.get('starting_offsets'):
                    starting_offsets = {int(k): v for k, v in config['starting_offsets'].items()}
                if config.get('offset_increment'):
                    offset_increment = {int(k): v for k, v in config['offset_increment'].items()}

                # Submit the run_consumer function to the executor
                futures.append(
                    executor.submit(
                        run_consumer,
                        topic=config['topic'],
                        group_id=config['group_id'],
                        num_messages=config['num_messages'],
                        bootstrap_servers=bootstrap_servers,
                        ssl_config=ssl_config,
                        schema_registry_url=schema_registry_url,
                        azure_storage_account_name=azure_storage_account_name,
                        azure_storage_account_key=azure_storage_account_key,
                        azure_filesystem_name=azure_filesystem_name,
                        reset_offsets=config.get('reset_offsets', False),
                        starting_offsets=starting_offsets,
                        offset_increment=offset_increment,
                        logger_manager=logger_manager,
                        base_log_dir=base_log_dir,
                    )
                )

            # Wait for all futures to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    app_logger.error(f"Error occurred in future.result(): {e}")


if __name__ == "__main__":
    main()
