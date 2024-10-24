# Kafka to Azure Serveless Pool

## Overview

This application is designed to consume messages from Kafka topics, convert them into Parquet files, and upload them to Azure Data Lake. It is built to allow parallel consumption of multiple Kafka topics and customizable configurations.

### Key Components:

- **Kafka Consumer**: Consumes messages from specified Kafka topics.
- **Avro Deserialization**: Deserializes messages using schemas from a Schema Registry.
- **Parquet Conversion**: Converts consumed messages into Parquet files.
- **Azure Data Lake Uploader**: Uploads the Parquet files to Azure Data Lake Storage Gen2.
- **Logging Framework**: Provides detailed logging for troubleshooting and monitoring.
- **Flask Web Interface**: Allows manual, ad hoc Kafka data retrieval through a web interface.

## Application Workflow

1. **Configuration Loading**: The application reads configurations from `connections.json` and `topics.json`.
2. **Logger Initialization**: Sets up logging directories and loggers for the application and each topic.
3. **Consumer Initialization**: For each topic specified in `topics.json`, a `TopicConsumer` is initialized with the provided configurations.
4. **Message Consumption**: Messages are consumed from Kafka topics using the `KafkaConsumerManager`.
5. **Message Processing**: Consumed messages are deserialized from Avro format.
6. **Parquet File Creation**: Messages are saved locally as Parquet files using the `ParquetHandler`.
7. **File Upload**: The Parquet files are uploaded to Azure Data Lake using the `AzureUploader`.
8. **Cleanup**: Local Parquet files are deleted after successful upload.
9. **Parallel Execution**: Multiple topic consumers run in parallel using a thread pool.
10. **Manual Ad-Hoc Kafka Consumer**: The Flask web interface allows users to input Kafka topic details and fetch specific data manually, independent of the main scheduled process.

---

## File-by-File Explanation

### 1. **main.py**

#### Purpose:

The entry point of the application. It orchestrates the entire process by:

- Loading configurations.
- Setting up loggers.
- Initializing and running `TopicConsumer` instances in parallel.

#### Key Functions:

- **`run_consumer()`**: Initializes and runs a consumer for a specific topic.
- **`main()`**: Loads configurations, initializes loggers, and manages the parallel execution of consumers.

#### Logic:

- Loads connection configurations from `connections.json`.
- Loads topic configurations from `topics.json`.
- Resolves SSL certificate paths to absolute paths.
- Initializes `LoggerManager` for application-wide and topic-specific logging.
- Uses `ThreadPoolExecutor` to run multiple `run_consumer` tasks in parallel.

---

### 2. **kafka/topic_consumer.py**

#### Purpose:

Manages the end-to-end process for a single Kafka topic:

- Consumes messages.
- Converts messages to Parquet format.
- Uploads Parquet files to Azure Data Lake.

#### Key Components:

- **KafkaConsumerManager**: Manages Kafka consumer operations.
- **ParquetHandler**: Handles conversion of messages to Parquet files.
- **AzureUploader**: Manages file uploads to Azure Data Lake.

#### Logic:

- Verifies Kafka connection.
- Consumes messages from the specified topic.
- Applies offset increments if provided.
- Saves messages to a Parquet file.
- Uploads the Parquet file to Azure Data Lake.
- Cleans up the local Parquet file.
- Ensures the Kafka consumer is properly closed.

---

### 3. **kafka/kafka_consumer_manager.py**

#### Purpose:

Handles the connection to Kafka, consumption of messages, and deserialization using Avro schemas.

#### Key Functions:

- **`verify_connection()`**: Checks the connection to Kafka.
- **`unpack()`**: Deserializes messages using Avro schemas from the Schema Registry.
- **`consume_messages()`**: Consumes messages from a Kafka topic with optional offset management.
- **`close()`**: Closes the Kafka consumer.

#### Logic:

- Initializes the Kafka consumer with SSL configurations.
- Uses the Schema Registry to deserialize Avro messages.
- Manages offsets, including resetting and starting from specific offsets.
- Consumes a specified number of messages from a topic.

---

### 4. **azure/parquet_handler.py**

#### Purpose:

Converts consumed messages into Parquet files using PyArrow.

#### Key Functions:

- **`save_to_parquet()`**: Saves messages to a Parquet file.
- **`format_timestamp()`**: Formats timestamps for inclusion in the Parquet file.

#### Logic:

- Prepares data for Parquet conversion.
- Defines the schema for the Parquet file.
- Creates a PyArrow table and writes it to a Parquet file.

---

### 5. **azure/azure_uploader.py**

#### Purpose:

Uploads Parquet files to Azure Data Lake Storage Gen2 and handles cleanup.

#### Key Functions:

- **`upload_parquet()`**: Uploads a Parquet file to a specified directory in Azure Data Lake.
- **`cleanup_local_file()`**: Deletes the local Parquet file after upload.

#### Logic:

- Initializes the Data Lake service client.
- Ensures the target directory exists in Azure Data Lake.
- Uploads the Parquet file and overwrites if necessary.

---

### 6. **utils/logger_manager.py**

#### Purpose:

Manages logging configurations for the application and individual topics.

#### Key Functions:

- **`get_topic_logger()`**: Retrieves a logger adapter for a specific topic.
- **`_initialize_root_logger()`**: Sets up the root logger.
- **`_adjust_external_loggers()`**: Adjusts logging levels for external libraries.
- **`_apply_custom_filters()`**: Applies custom filters to exclude unwanted log messages.

#### Logic:

- Sets up logging directories and handlers.
- Applies custom formatting and filters to log messages.
- Allows topic-specific loggers to propagate logs to the root logger.

---

### 7. **utils/custom_formatter.py**

#### Purpose:

Provides a custom log formatter that includes additional context such as topic and run ID.

#### Logic:

- Extends the default logging formatter to include custom attributes.

---

### 8. **utils/custom_filters.py**

#### Purpose:

Defines a custom logging filter to exclude specific messages.

#### Logic:

- Filters out log records containing specified substrings.

---


### 9. **Flask App (app.py)**

#### Purpose:

Provides a web interface for manual Kafka data consumption. Users can provide Kafka topic details and retrieve messages on demand. This app operates independently of the main data pipeline, enabling real-time, ad hoc data queries.



- 

## Configuration Files

### 1. **connections.json**

#### Purpose:

Contains configuration parameters for connections to Kafka and Azure services.

#### Fields:

- **bootstrap_servers**: Kafka bootstrap servers.
- **ssl_config**: Paths to SSL certificate files.
- **azure_storage_account_name**: Azure storage account name.
- **azure_storage_account_key**: Azure storage account key.
- **azure_filesystem_name**: Azure filesystem name.
- **schema_registry_url**: URL of the Kafka Schema Registry.

#### Example:

```json
{
  "bootstrap_servers": "your.kafka.server:port",
 "ssl_config": {
        "ca": "../certifications/your_ca_file.pem",
        "cert": "../certifications/your_sslcert.pem",
        "key": "../certifications/your_rsa.pem"
 },
  "azure_storage_account_name": "your_storage_account",
  "azure_storage_account_key": "your_storage_account_key",
  "azure_filesystem_name": "your_filesystem_name",
  "schema_registry_url": "https://your.schema.registry"
}
```

### 2. **topics.json**

#### Purpose:

Specifies the topics to consume and their configurations.

#### Fields:

- **topic**: Name of the Kafka topic.
- **group_id**: Kafka consumer group ID.
- **num_messages**: Number of messages to consume.
- **reset_offsets**: Whether to reset offsets to the beginning.
- **starting_offsets**: Specific starting offsets for partitions.
- **offset_increment**: Offset increments to adjust message offsets.

#### Example:

```json
[
  {
    "topic": "example_topic_1",
    "group_id": "consumer_group_1",
    "num_messages": 100,
    "reset_offsets": false,
    "starting_offsets": null,
    "offset_increment": { "0": 0 }
  },
  {
    "topic": "example_topic_2",
    "group_id": "consumer_group_2",
    "num_messages": 50,
    "reset_offsets": true,
    "starting_offsets": { "0": 100, "1": 200 },
    "offset_increment": { "0": 10, "1": 20 }
  }
]
```

### Possible Senarios:

## **1. Fetch specific offsets for a specific partition:**

To fetch a specific number of messages starting from a specified offset for a partition, we create a new consumer group to avoid affecting other consumers.

```json
{
  "topic": "example_topic_1",
  "group_id": "new_consumer_group_1",
  "num_messages": 100,
  "reset_offsets": false,
  "starting_offsets": { "0": 5555 },
  "offset_increment": { "0": 0 }
}
```

In this example:

- We create a temporary consumer group new_consumer_group_1 to fetch 100 messages from partition 0 starting at offset 5555.
- The offset increment is set to 0, so the consumed offsets in the Parquet file will match the Kafka offsets

## **2. Fetch specific offsets for multiple partitions:**

To fetch messages starting from different offsets for multiple partitions within a topic.

```json
{
  "topic": "example_topic_1",
  "group_id": "new_consumer_group_1",
  "num_messages": 100,
  "reset_offsets": false,
  "starting_offsets": { "0": 5555, "1": 6666 },
  "offset_increment": { "0": 0, "1": 0 }
}
```

In this example:

- We consume messages from partition 0 starting from offset 5555 and from partition 1 starting from offset 6666.

## **3. Fetch specific offsets for multiple partitions:**

To reset the consumer group and fetch messages from the earliest available offset.

```json
{
  "topic": "example_topic_1",
  "group_id": "new_consumer_group_1",
  "num_messages": 100,
  "reset_offsets": true,
  "starting_offsets": null,
  "offset_increment": { "0": 0 }
}
```

In this case:

- We reset the offsets to the beginning of the topic, consuming the first 100 messages available in Kafka.

## **4. Increment offsets for a specific partition:**

To simulate processing offsets by adjusting the values in the Parquet file without changing the original Kafka offsets.

```json
{
  "topic": "example_topic_1",
  "group_id": "new_consumer_group_1",
  "num_messages": 100,
  "reset_offsets": true,
  "starting_offsets": null,
  "offset_increment": { "0": 200 }
}
```

## Notes:

### 1. Using the Same Consumer Group:

If you want the consumer group to continue from where it last left off, you can reuse the same `group_id` across topics or partitions.

- **Example**:  
  `"starting_offsets": null + "offset_increment": {"0": 0}` ensures that the consumer group resumes from the last committed offset without modifying the offset metadata in the final Parquet file.

### 2. Custom Offset Increment:

If you want the Parquet file to display adjusted offsets, use `"offset_increment"` to modify the offsets in the Parquet file while Kafka continues consuming from the actual offsets.

- **Example**:  
  `"offset_increment": {"0": 200}` will adjust the offsets in the Parquet file by 200, even though Kafka is consuming from the real offsets.

### 3. Resetting Offsets:

To reset the consumer group's offsets and start from the earliest available data, use `"reset_offsets": true`.

- **Example**:  
  `"reset_offsets": true + "starting_offsets": null` will reset the consumer group's offsets to the earliest available offsets in Kafka, allowing consumption from the beginning.

### 4. Temporary Consumer Groups:

When running one-time queries or fetching specific offset ranges, it's recommended to use a unique `group_id` to avoid affecting other consumers.
