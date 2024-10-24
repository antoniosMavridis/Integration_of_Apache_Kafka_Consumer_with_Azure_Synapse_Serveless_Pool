import os
import logging
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import json
from typing import List, Dict, Optional

class ParquetHandler:
    """
    Responsible for saving Kafka messages to Parquet files.
    """

    def __init__(self, output_dir: str = 'parquet_files', logger: logging.LoggerAdapter = None) -> None:
        """
        Initializes the Parquet handler with the output directory.
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # Use the passed logger or create a new one
        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)
            self.logger = logging.LoggerAdapter(self.logger, {'topic': 'N/A', 'run_id': 'N/A'})

    def format_timestamp(self, timestamp_ms: int) -> str:
        """
        Converts a timestamp from milliseconds to 'YYYY-MM-DD HH:MM:SS' format.
        """
        # Convert milliseconds to seconds, then format
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000)
        return timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')

    def save_to_parquet(self, messages: List[Dict], topic: str, parquet_filename: str) -> Optional[str]:
        """
        Saves Kafka messages to a Parquet file using PyArrow.

        Parameters:
            messages (List[Dict]): List of messages to save.
            topic (str): The Kafka topic name.
            parquet_filename (str): The name of the Parquet file to create.

        Returns:
            Optional[str]: The path to the saved Parquet file, or None if no messages to save.
        """
        if not messages:
            self.logger.info("No messages to save.")
            return None

        # Prepare data in the format Partition, Offset, Value, Timestamp
        partitions = []
        offsets = []
        values = []
        timestamps = []

        for msg in messages:
            # Format the timestamp if available
            formatted_timestamp = self.format_timestamp(msg['timestamp']) if msg.get('timestamp') else None

            partitions.append(msg['partition'])
            offsets.append(msg['offset'])

            # Convert the 'value' to a JSON string with double quotes
            json_value = json.dumps(msg['value'])

            values.append(json_value)
            timestamps.append(formatted_timestamp)

        # Define PyArrow schema
        schema = pa.schema([
            ('Partition', pa.int32()),
            ('Offset', pa.int64()),
            ('Value', pa.string()),
            ('Timestamp', pa.string()),
        ])

        # Create PyArrow table
        table = pa.Table.from_pydict({
            'Partition': partitions,
            'Offset': offsets,
            'Value': values,
            'Timestamp': timestamps,
        }, schema=schema)

        # Create the Parquet file using PyArrow
        parquet_path = os.path.join(self.output_dir, parquet_filename)
        pq.write_table(table, parquet_path)

        self.logger.info(f"Parquet file saved: {parquet_path}")
        return parquet_path