import os
import logging
from azure.storage.filedatalake import DataLakeServiceClient


class AzureUploader:
    """
    Handles uploading files to Azure Data Lake Storage Gen2.
    """

    def __init__(
        self,
        storage_account_name: str,
        storage_account_key: str,
        filesystem_name: str,
        logger: logging.LoggerAdapter = None,
        run_id: str = ''
    ) -> None:
        """
        Initializes the AzureUploader with storage account information.
        """
        if not filesystem_name:
            raise ValueError("Filesystem name must be provided.")

        # Use the passed logger or create a new one
        if logger is not None:
            self.logger = logger
        else:
            base_logger = logging.getLogger(__name__)
            self.logger = logging.LoggerAdapter(base_logger, {'topic': 'N/A', 'run_id': run_id})

        self.logger.info(f"Using filesystem: {filesystem_name}")

        # Initialize the Azure Data Lake client
        try:
            self.service_client = DataLakeServiceClient(
                account_url=f"https://{storage_account_name}.dfs.core.windows.net",
                credential=storage_account_key
            )
            self.filesystem_client = self.service_client.get_file_system_client(filesystem_name)
            self.logger.info("AzureUploader initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize AzureUploader: {e}")
            raise

    def upload_parquet(self, topic_name: str, file_name: str, local_file_path: str) -> None:
        """
        Uploads a Parquet file to Azure Data Lake.
        """
        try:
            dirname = f"your_azure_datalake_path/{topic_name}/inp"
            dest_path = f"{dirname}/{file_name}"

            self.logger.info(f"Uploading {file_name} to {dest_path} in Azure Data Lake...")

            # Ensure directory exists or create it
            directory_client = self.filesystem_client.get_directory_client(dirname)
            try:
                directory_client.create_directory()
                self.logger.info(f"Created directory: {dirname}")
            except Exception as e:
                # If the directory already exists, ignore the error
                if "exists" in str(e).lower():
                    self.logger.info(f"Directory already exists: {dirname}")
                else:
                    self.logger.error(f"Failed to create directory {dirname}: {e}")
                    raise

            # Get file client and upload the file
            file_client = directory_client.get_file_client(file_name)
            with open(local_file_path, "rb") as file_data:
                file_client.upload_data(file_data.read(), overwrite=True)

            self.logger.info(f"Upload of {file_name} completed.")

        except Exception as e:
            self.logger.error(f"Failed to upload file {file_name} to Azure: {e}")
            raise

    def cleanup_local_file(self, local_file_path: str) -> None:
        """
        Deletes the local Parquet file after it is uploaded to Azure.
        """
        try:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                self.logger.info(f"Local file {local_file_path} deleted.")
            else:
                self.logger.warning(f"File {local_file_path} not found, could not delete.")
        except Exception as e:
            self.logger.error(f"Failed to delete local file {local_file_path}: {e}")
            raise