import logging 
import os
from typing import Optional, Dict

from kafka_consumer.kafka_consumer_manager import KafkaConsumerManager
from azure.parquet_handler import ParquetHandler
from azure.azure_uploader import AzureUploader


class TopicConsumer:
    """
    Manages the end-to-end process of consuming messages from a Kafka topic,
    converting them to Parquet format, and uploading to Azure Data Lake.

    This class integrates KafkaConsumerManager, ParquetHandler, and AzureUploader
    to perform the complete data pipeline for a specific topic.
    """

    def __init__(
        self,
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
        logger: Optional[logging.LoggerAdapter] = None,
        run_id: str = '',
    ) -> None:
        """
        Initializes the TopicConsumer with necessary configurations.

        Parameters:
            topic (str): The Kafka topic to consume.
            group_id (str): The consumer group ID.
            num_messages (int): Number of messages to consume.
            bootstrap_servers (str): Kafka bootstrap servers.
            ssl_config (Dict[str, str]): SSL configuration for Kafka.
            schema_registry_url (str): URL of the Schema Registry.
            azure_storage_account_name (str): Azure storage account name.
            azure_storage_account_key (str): Azure storage account key.
            azure_filesystem_name (str): Azure filesystem name.
            reset_offsets (bool, optional): Whether to reset offsets to beginning. Defaults to False.
            starting_offsets (Optional[Dict[int, int]], optional): Starting offsets per partition. Defaults to None.
            offset_increment (Optional[Dict[int, int]], optional): Offset increments per partition. Defaults to None.
            logger (Optional[logging.LoggerAdapter], optional): Logger instance to use. Defaults to None.
            run_id (str, optional): Unique identifier for the run. Defaults to ''.
        """
        self.topic = topic
        self.num_messages = num_messages
        self.azure_storage_account_name = azure_storage_account_name
        self.azure_storage_account_key = azure_storage_account_key
        self.azure_filesystem_name = azure_filesystem_name
        self.run_id = run_id

        # Use the passed logger or create a new one
        if logger is not None:
            self.logger = logger
        else:
            # Create a new logger adapter with topic and run_id
            self.logger = logging.getLogger(__name__)
            self.logger = logging.LoggerAdapter(self.logger, {'topic': topic, 'run_id': run_id})

        # Create KafkaConsumerManager instance
        self.kafka_consumer_manager = KafkaConsumerManager(
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            ssl_config=ssl_config,
            schema_registry_url=schema_registry_url,
            reset_offsets=reset_offsets,
            starting_offsets=starting_offsets,
            logger=self.logger,
            run_id=self.run_id,
        )

        # Create ParquetHandler instance
        self.parquet_handler = ParquetHandler(logger=self.logger)

        # Create AzureUploader instance
        self.azure_uploader = AzureUploader(
            storage_account_name=self.azure_storage_account_name,
            storage_account_key=self.azure_storage_account_key,
            filesystem_name=self.azure_filesystem_name,
            logger=self.logger,
        )

        # Store the offset increment
        self.offset_increment = offset_increment

    def run(self) -> None:
        """
        Executes the process of consuming Kafka messages, converting them to Parquet,
        and uploading the file to Azure Data Lake.

        The process includes:
            - Verifying Kafka connection.
            - Consuming messages from Kafka.
            - Applying offset increments if provided.
            - Saving messages to a Parquet file.
            - Uploading the Parquet file to Azure Data Lake.
            - Cleaning up local Parquet file.
        """
        try:
            # Verify Kafka connection
            if not self.kafka_consumer_manager.verify_connection():
                self.logger.error("Failed to verify Kafka connection.")
                return

            # Consume messages from the Kafka topic
            messages = self.kafka_consumer_manager.consume_messages(
                self.topic, self.num_messages
            )

            if not messages:
                self.logger.info("No messages consumed.")
                return

            # Apply offset increments if provided
            if self.offset_increment:
                for msg in messages:
                    partition = msg['partition']
                    increment = self.offset_increment.get(partition, 0)
                    msg['offset'] += increment

            # Determine from_offset and to_offset based on consumed messages
            from_offset = min(msg['offset'] for msg in messages)
            to_offset = max(msg['offset'] for msg in messages)

            # Define the Parquet file name using the topic, from_offset, and to_offset
            parquet_filename = f'{self.topic}_{from_offset}_{to_offset}_data.parquet'

            # Save the messages to Parquet
            parquet_path = self.parquet_handler.save_to_parquet(
                messages, self.topic, parquet_filename
            )

            # Upload to Azure and clean up local file
            if parquet_path:
                self.azure_uploader.upload_parquet(
                    self.topic,
                    os.path.basename(parquet_path),
                    parquet_path
                )
                self.azure_uploader.cleanup_local_file(parquet_path)

        except Exception as e:
            self.logger.error(f"Error during consumption or processing: {e}")
            raise

        finally:
            # Ensure the Kafka consumer is closed
            self.kafka_consumer_manager.close()