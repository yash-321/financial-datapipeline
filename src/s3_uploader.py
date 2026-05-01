"""S3 uploader module for uploading parquet files to AWS S3."""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
import logging
import os
from pathlib import Path
import time
from typing import Optional

logger = logging.getLogger(__name__)


class S3Uploader:
    """Upload files to S3 with retry logic and multipart support."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "trades",
        region: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize S3 uploader.

        Args:
            bucket: S3 bucket name
            prefix: S3 key prefix (e.g., "trades" -> s3://bucket/trades/...)
            region: AWS region (defaults to AWS_REGION env var or us-east-1)
            max_retries: Maximum number of upload retry attempts
            retry_delay: Initial delay between retries (exponential backoff)
        """
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.region = region or os.environ.get("AWS_REGION", "us-east-1")
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Configure boto3 with retries
        config = Config(
            region_name=self.region,
            retries={"max_attempts": max_retries, "mode": "adaptive"},
        )

        self._client = boto3.client("s3", config=config)
        self._transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=8 * 1024 * 1024,  # 8MB
            max_concurrency=10,
            multipart_chunksize=8 * 1024 * 1024,  # 8MB
            use_threads=True,
        )

    def _build_s3_key(self, local_path: Path, base_dir: Path) -> str:
        """
        Build S3 key preserving Hive partition structure.

        Example:
            local_path: data/trades/date=2026-04-30/symbol=AAPL/part-123.parquet
            base_dir: data/trades
            -> trades/date=2026-04-30/symbol=AAPL/part-123.parquet
        """
        relative_path = local_path.relative_to(base_dir)
        return f"{self.prefix}/{relative_path}"

    def upload_file(
        self,
        local_path: Path,
        base_dir: Path,
        s3_key: Optional[str] = None,
        delete_after_upload: bool = False,
    ) -> tuple[bool, str]:
        """
        Upload a file to S3 with retry logic.

        Args:
            local_path: Path to local file
            base_dir: Base directory for computing relative S3 key
            s3_key: Optional explicit S3 key (overrides auto-generated)
            delete_after_upload: If True, delete local file after successful upload

        Returns:
            Tuple of (success: bool, s3_uri: str)
        """
        if s3_key is None:
            s3_key = self._build_s3_key(local_path, base_dir)

        s3_uri = f"s3://{self.bucket}/{s3_key}"

        for attempt in range(1, self.max_retries + 1):
            try:
                start_time = time.time()

                self._client.upload_file(
                    str(local_path),
                    self.bucket,
                    s3_key,
                    Config=self._transfer_config,
                    ExtraArgs={
                        "ContentType": "application/octet-stream",
                        "Metadata": {
                            "source": "trade_consumer",
                            "upload_timestamp": str(int(time.time() * 1000)),
                        },
                    },
                )

                elapsed = time.time() - start_time
                file_size = local_path.stat().st_size
                logger.info(
                    f"Uploaded {local_path.name} to {s3_uri} "
                    f"({file_size / 1024:.1f} KB in {elapsed:.2f}s)"
                )

                if delete_after_upload:
                    local_path.unlink()
                    logger.debug(f"Deleted local file: {local_path}")

                return True, s3_uri

            except NoCredentialsError:
                logger.error(
                    "AWS credentials not found. Set AWS_ACCESS_KEY_ID and "
                    "AWS_SECRET_ACCESS_KEY environment variables."
                )
                return False, s3_uri

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                logger.warning(
                    f"S3 upload attempt {attempt}/{self.max_retries} failed: "
                    f"{error_code} - {e}"
                )

                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** (attempt - 1))
                    logger.info(f"Retrying in {delay:.1f}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to upload {local_path} after {self.max_retries} attempts")
                    return False, s3_uri

            except Exception as e:
                logger.error(f"Unexpected error uploading {local_path}: {e}")
                return False, s3_uri

        return False, s3_uri

    def upload_files(
        self,
        file_paths: list[Path],
        base_dir: Path,
        delete_after_upload: bool = False,
    ) -> tuple[list[str], list[Path]]:
        """
        Upload multiple files to S3.

        Args:
            file_paths: List of local file paths to upload
            base_dir: Base directory for computing relative S3 keys
            delete_after_upload: If True, delete local files after successful upload

        Returns:
            Tuple of (successful_s3_uris, failed_local_paths)
        """
        successful = []
        failed = []

        for local_path in file_paths:
            success, s3_uri = self.upload_file(
                local_path, base_dir, delete_after_upload=delete_after_upload
            )
            if success:
                successful.append(s3_uri)
            else:
                failed.append(local_path)

        if successful:
            logger.info(f"Successfully uploaded {len(successful)} files to S3")
        if failed:
            logger.warning(f"Failed to upload {len(failed)} files to S3")

        return successful, failed

    def check_connection(self) -> bool:
        """Verify S3 bucket access."""
        try:
            self._client.head_bucket(Bucket=self.bucket)
            logger.info(f"Successfully connected to S3 bucket: {self.bucket}")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404":
                logger.error(f"S3 bucket not found: {self.bucket}")
            elif error_code == "403":
                logger.error(f"Access denied to S3 bucket: {self.bucket}")
            else:
                logger.error(f"Failed to connect to S3 bucket: {e}")
            return False
        except NoCredentialsError:
            logger.error("AWS credentials not configured")
            return False
