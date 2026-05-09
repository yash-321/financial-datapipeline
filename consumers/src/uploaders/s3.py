"""S3 uploader module for uploading files to AWS S3."""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
from dataclasses import dataclass
import logging
import os
from pathlib import Path
from queue import Queue, Empty
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class UploadTask:
    """Represents an S3 upload task."""
    local_path: Path
    base_dir: Path
    delete_after_upload: bool
    retry_count: int = 0


class S3Uploader:
    """Upload files to S3 with retry logic and multipart support."""

    MAX_UPLOAD_RETRIES = 3
    UPLOAD_QUEUE_MAX_SIZE = 1000

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
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
        self._transfer_config = boto3.s3.transfer.TransferConfig( # type: ignore
            multipart_threshold=8 * 1024 * 1024,  # 8MB
            max_concurrency=10,
            multipart_chunksize=8 * 1024 * 1024,  # 8MB
            use_threads=True,
        )

        # Async upload support
        self._upload_queue: Queue[Optional[UploadTask]] = Queue(maxsize=self.UPLOAD_QUEUE_MAX_SIZE)
        self._upload_workers: list[threading.Thread] = []
        self._uploads_in_flight: int = 0
        self._uploads_lock = threading.Lock()

    def check_connection(self) -> bool:
        """Verify S3 bucket is accessible."""
        try:
            self._client.head_bucket(Bucket=self.bucket)
            logger.info(f"S3 connection verified: s3://{self.bucket}")
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"S3 bucket check failed ({error_code}): {e}")
            return False
        except NoCredentialsError:
            logger.error("No AWS credentials found")
            return False

    def _build_s3_key(self, local_path: Path, base_dir: Path) -> str:
        """
        Build S3 key preserving Hive partition structure.

        Example:
            local_path: data/trades/date=2026-04-30/symbol=AAPL/part-123.parquet
            base_dir: data/trades
            -> trades/date=2026-04-30/symbol=AAPL/part-123.parquet
        """
        relative_path = local_path.relative_to(base_dir)
        if self.prefix:
            return f"{self.prefix}/{relative_path}"
        return str(relative_path)

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
                )

                elapsed = time.time() - start_time
                file_size = local_path.stat().st_size / 1024 / 1024  # MB
                logger.info(
                    f"Uploaded {local_path.name} ({file_size:.2f} MB) to {s3_uri} "
                    f"in {elapsed:.2f}s"
                )

                if delete_after_upload:
                    local_path.unlink()
                    logger.debug(f"Deleted local file: {local_path}")

                return True, s3_uri

            except ClientError as e:
                logger.warning(
                    f"Upload attempt {attempt}/{self.max_retries} failed for {local_path}: {e}"
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * (2 ** (attempt - 1)))

        logger.error(f"Failed to upload {local_path} after {self.max_retries} attempts")
        return False, s3_uri

    def start_async_workers(self, num_workers: int = 4) -> None:
        """Start background upload worker threads."""
        for i in range(num_workers):
            worker = threading.Thread(
                target=self._upload_worker,
                name=f"s3-upload-worker-{i}",
                daemon=True
            )
            worker.start()
            self._upload_workers.append(worker)
        logger.info(f"Started {num_workers} S3 upload workers")

    def _upload_worker(self) -> None:
        """Worker thread that processes uploads from the queue."""
        while True:
            try:
                task = self._upload_queue.get()

                # Shutdown sentinel
                if task is None:
                    self._upload_queue.task_done()
                    break

                with self._uploads_lock:
                    self._uploads_in_flight += 1

                try:
                    self._process_upload(task)
                finally:
                    with self._uploads_lock:
                        self._uploads_in_flight -= 1
                    self._upload_queue.task_done()

            except Exception as e:
                logger.error(f"Upload worker error: {e}")

    def _process_upload(self, task: UploadTask) -> None:
        """Process a single upload task."""
        try:
            success, s3_uri = self.upload_file(
                local_path=task.local_path,
                base_dir=task.base_dir,
                delete_after_upload=task.delete_after_upload,
            )
            if not success:
                self._handle_upload_failure(task, Exception("Upload returned failure"))
        except Exception as e:
            self._handle_upload_failure(task, e)

    def _handle_upload_failure(self, task: UploadTask, error: Exception) -> None:
        """Handle upload failure with exponential backoff retry."""
        if task.retry_count < self.MAX_UPLOAD_RETRIES:
            backoff_time = 2 ** task.retry_count
            logger.warning(
                f"Upload failed for {task.local_path} (attempt {task.retry_count + 1}), "
                f"retrying in {backoff_time}s: {error}"
            )
            time.sleep(backoff_time)

            retry_task = UploadTask(
                local_path=task.local_path,
                base_dir=task.base_dir,
                delete_after_upload=task.delete_after_upload,
                retry_count=task.retry_count + 1,
            )
            try:
                self._upload_queue.put(retry_task, timeout=5.0)
            except Exception:
                logger.error(f"Failed to re-queue upload for {task.local_path}")
        else:
            logger.error(
                f"Upload failed for {task.local_path} after {self.MAX_UPLOAD_RETRIES} retries, "
                f"file kept locally: {error}"
            )

    def queue_upload(
        self,
        local_path: Path,
        base_dir: Path,
        delete_after_upload: bool = False,
    ) -> bool:
        """Queue a file for async upload to S3.
        
        Returns True if queued successfully, False if queue is full.
        """
        task = UploadTask(
            local_path=local_path,
            base_dir=base_dir,
            delete_after_upload=delete_after_upload,
            retry_count=0,
        )
        try:
            self._upload_queue.put(task, timeout=5.0)
            logger.debug(f"Queued upload for {local_path}")
            return True
        except Exception:
            logger.warning(f"Upload queue full, cannot queue {local_path}")
            return False

    def wait_for_uploads(self, timeout: float = 60.0) -> None:
        """Wait for all pending uploads to complete."""
        if not self._upload_workers:
            return

        logger.info("Waiting for pending uploads to complete...")

        # Send shutdown sentinels
        for _ in self._upload_workers:
            try:
                self._upload_queue.put(None, timeout=5.0)
            except Exception:
                logger.warning("Failed to send shutdown sentinel")

        # Wait for queue to drain
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self._uploads_lock:
                in_flight = self._uploads_in_flight
            if self._upload_queue.empty() and in_flight == 0:
                break
            time.sleep(0.1)

        # Join workers
        for worker in self._upload_workers:
            worker.join(timeout=max(1.0, timeout - (time.time() - start_time)))

        if any(w.is_alive() for w in self._upload_workers):
            logger.warning("Some upload workers did not terminate in time")
        else:
            logger.info("All upload workers terminated")
