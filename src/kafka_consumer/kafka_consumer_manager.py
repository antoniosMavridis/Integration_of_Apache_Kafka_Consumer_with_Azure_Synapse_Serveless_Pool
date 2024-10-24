import struct
import io
import logging  
from typing import Optional, Dict, List, Any

from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from avro.io import BinaryDecoder, DatumReader

# Define MAGIC_BYTES for the Avro message format
MAGIC_BYTES = 0


class KafkaConsumerManager:
    """
    Manages Kafka consumer operations, including consuming messages and deserializing them using Avro schemas.

    This class handles the connection to Kafka, consumption of messages from a topic,
    and deserialization of message payloads using Avro schemas from a Schema Registry.
    """

    def __init__(
        self,
        group_id: str,
        bootstrap_servers: str,
        ssl_config: Dict[str, str],
        schema_registry_url: str,
        offset: str = 'earliest',
        reset_offsets: bool = False,
        starting_offsets: Optional[Dict[int, int]] = None,
        logger: Optional[logging.LoggerAdapter] = None,
        run_id: str = '',
    ) -> None:
        """
        Initializes the Kafka consumer with the provided configuration.

        Parameters:
            group_id (str): The consumer group ID.
            bootstrap_servers (str): Kafka bootstrap servers.
            ssl_config (Dict[str, str]): SSL configuration dictionary with keys 'ca', 'cert', 'key'.
            schema_registry_url (str): URL for the Schema Registry.
            offset (str, optional): Offset reset strategy ('earliest', 'latest'). Defaults to 'earliest'.
            reset_offsets (bool, optional): Whether to reset offsets to beginning. Defaults to False.
            starting_offsets (Optional[Dict[int, int]], optional): Starting offsets for partitions. Defaults to None.
            logger (Optional[logging.LoggerAdapter], optional): Logger instance to use. Defaults to None.
            run_id (str, optional): Unique identifier for the run. Defaults to ''.

        Raises:
            Exception: If initialization fails.
        """
        # Use the passed logger or create a new one
        if logger is not None:
            self.logger = logger
        else:
            # Create a new logger adapter with default topic and run_id
            self.logger = logging.getLogger(__name__)
            self.logger = logging.LoggerAdapter(self.logger, {'topic': 'N/A', 'run_id': run_id})

        try:
            # Initialize Schema Registry Client
            schema_registry_conf = {'url': schema_registry_url}
            self.schema_registry_client = CachedSchemaRegistryClient(schema_registry_conf)

            # Consumer configuration
            self.consumer_conf = {
                'bootstrap.servers': bootstrap_servers,
                'security.protocol': 'SSL',
                'ssl.ca.location': ssl_config['ca'],
                'ssl.certificate.location': ssl_config['cert'],
                'ssl.key.location': ssl_config['key'],
                'group.id': group_id,
                'auto.offset.reset': offset,
            }

            # Create the Consumer instance
            self.consumer = Consumer(self.consumer_conf)

            # Store reset_offsets and starting_offsets
            self.reset_offsets = reset_offsets
            self.starting_offsets = starting_offsets

            self.logger.info("KafkaConsumerManager initialized successfully.")

        except Exception as e:
            self.logger.error(f"Failed to initialize KafkaConsumerManager: {e}")
            raise

    def verify_connection(self) -> bool:
        """
        Verifies connection to Kafka by fetching available topics.

        Returns:
            bool: True if connection is successful, False otherwise.
        """
        try:
            # Retrieve metadata from Kafka
            self.consumer.list_topics(timeout=10)
            self.logger.info("Connected to Kafka successfully.")
            return True
        except KafkaException as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def unpack(self, payload: bytes) -> Optional[dict]:
        """
        Unpacks and deserializes a message payload using Avro and the schema registry.

        Parameters:
            payload (bytes): The raw message payload from Kafka.

        Returns:
            Optional[dict]: The deserialized message as a dictionary, or None if deserialization fails.
        """
        try:
            # Extract magic byte and schema ID from the payload
            # The first byte is the magic byte, the next 4 bytes are the schema ID
            magic, schema_id = struct.unpack('>bI', payload[:5])

            if magic == MAGIC_BYTES:
                # Fetch the schema using the schema ID from the registry
                schema = self.schema_registry_client.get_by_id(schema_id)

                # Use Avro DatumReader to read the payload
                reader = DatumReader(schema)
                decoder = BinaryDecoder(io.BytesIO(payload[5:]))  # Skip first 5 bytes
                decoded_data = reader.read(decoder)
                
                return decoded_data
            else:
                self.logger.warning("Unknown message format, skipping unpack.")
                return None

        except Exception as e:
            self.logger.error(f"Error during unpacking: {e}")
            return None

    def consume_messages(self, topic: str, num_messages: int) -> List[Dict[str, Any]]:
        """
        Consumes messages from a Kafka topic.

        Parameters:
            topic (str): The Kafka topic to consume from.
            num_messages (int): The number of messages to consume.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing message metadata and value.
        """
        try:
            # If starting_offsets is provided or reset_offsets is True, define an on_assign callback
            if self.starting_offsets or self.reset_offsets:
                self.logger.info(
                    f"Assigning specific offsets for consumer group {self.consumer_conf['group.id']} and topic {topic}"
                )

                def on_assign(consumer, partitions):
                    for p in partitions:
                        if self.starting_offsets and p.partition in self.starting_offsets:
                            # Set the starting offset for this partition
                            p.offset = self.starting_offsets[p.partition]
                            self.logger.info(f"Setting offset for partition {p.partition} to {p.offset}")
                        elif self.reset_offsets:
                            # Reset offset to the beginning
                            p.offset = OFFSET_BEGINNING
                            self.logger.info(f"Resetting offset for partition {p.partition} to beginning")
                        else:
                            self.logger.info(
                                f"No specific offset set for partition {p.partition}, using default assignment"
                            )
                    consumer.assign(partitions)

                # Subscribe with the on_assign callback
                self.consumer.subscribe([topic], on_assign=on_assign)
            else:
                # Subscribe normally
                self.consumer.subscribe([topic])

            self.logger.info(f"Subscribed to topic: {topic}")

            messages = []
            # Consume messages
            batch = self.consumer.consume(num_messages=num_messages, timeout=10.0)

            if not batch:
                self.logger.info(f"No messages fetched from topic {topic}.")
                return messages

            for msg in batch:
                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Manually deserialize the message using the unpack method
                try:
                    unpacked_value = self.unpack(msg.value())
                    if unpacked_value is None:
                        self.logger.warning(
                            f"Received a null or unknown value for message at offset {msg.offset()} "
                            f"in partition {msg.partition()}. Skipping message."
                        )
                    else:
                        messages.append(
                            {
                                "offset": msg.offset(),
                                "partition": msg.partition(),
                                "timestamp": msg.timestamp()[1],
                                "value": unpacked_value,
                            }
                        )
                except Exception as e:
                    self.logger.error(
                        f"Deserialization error at offset {msg.offset()} in partition {msg.partition()}: {e}"
                    )
                    continue

            return messages

        except Exception as e:
            self.logger.error(f"Error during consumption: {e}")
            raise

    def close(self) -> None:
        """
        Closes the Kafka consumer and releases any allocated resources.
        """
        self.consumer.close()
        self.logger.info("Kafka consumer closed.")