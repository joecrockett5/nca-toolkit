import logging
import os
import shutil
from abc import ABC, abstractmethod

from config import validate_env_vars
from services.gcp_toolkit import upload_to_gcs
from services.s3_toolkit import download_from_s3, upload_to_s3

logger = logging.getLogger(__name__)


class CloudStorageProvider(ABC):
    @abstractmethod
    def upload_file(self, file_path: str) -> str:
        pass

    @abstractmethod
    def download_file(self, file_path: str) -> str:
        pass


class GCPStorageProvider(CloudStorageProvider):
    def __init__(self):
        self.bucket_name = os.getenv("GCP_BUCKET_NAME")

    def upload_file(self, file_path: str) -> str:
        return upload_to_gcs(file_path, self.bucket_name)

    def download_file(self, file_path: str) -> str:
        raise NotImplementedError("download_file not implemented for GCP")


class S3CompatibleProvider(CloudStorageProvider):
    def __init__(self):
        self.endpoint_url = os.getenv("S3_ENDPOINT_URL")
        self.access_key = os.getenv("S3_ACCESS_KEY")
        self.secret_key = os.getenv("S3_SECRET_KEY")

    def upload_file(self, file_path: str) -> str:
        cloud_file_url = upload_to_s3(
            file_path, self.endpoint_url, self.access_key, self.secret_key
        )
        logger.info(f"File uploaded successfully: {cloud_file_url}")

        return cloud_file_url

    def download_file(self, file_path: str) -> str:
        return download_from_s3(
            file_path, self.endpoint_url, self.access_key, self.secret_key
        )


def get_storage_provider() -> CloudStorageProvider:
    try:
        validate_env_vars("GCP")
        return GCPStorageProvider()
    except ValueError:
        validate_env_vars("S3")
        return S3CompatibleProvider()


def upload_file(file_path: str) -> str:
    provider = get_storage_provider()
    try:
        logger.info(f"Uploading file to cloud storage: {file_path}")
        url = provider.upload_file(file_path)
        logger.info(f"File uploaded successfully: {url}")
        return url
    except Exception as e:
        logger.error(f"Error uploading file to cloud storage: {e}")
        raise


def download_file(cloud_file_path: str, target_path: str) -> str:
    provider = get_storage_provider()
    try:
        logger.info(f"Downloading file from cloud storage: {cloud_file_path}")
        downloaded_file_path = provider.download_file(cloud_file_path)
        logger.info(f"File downloaded successfully: {downloaded_file_path}")
        shutil.move(downloaded_file_path, target_path)
        return target_path
    except Exception as e:
        logger.error(f"Error downloading file from cloud storage: {e}")
        raise
