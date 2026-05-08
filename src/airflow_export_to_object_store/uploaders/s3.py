"""AWS S3 uploader using boto3 transfer (automatic multipart)."""
from __future__ import annotations

import logging
from typing import Any, Optional, Sequence, Tuple

from airflow.hooks.base import BaseHook

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    S3Hook = None  # type: ignore[assignment]


def _is_aws_generic_hook(storage_hook: Any) -> bool:
    """Detect AwsGenericHook even when the import path is unavailable."""
    try:
        from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

        if isinstance(storage_hook, AwsGenericHook):
            return True
    except Exception:
        pass

    cls_name = type(storage_hook).__name__.lower()
    if "aws" in cls_name and "hook" in cls_name:
        return True
    return "AwsGenericHook" in str(type(storage_hook))


class S3Uploader:
    name = "s3"

    def matches(self, storage_hook: Any) -> bool:
        if S3Hook is not None and isinstance(storage_hook, S3Hook):
            return True
        return _is_aws_generic_hook(storage_hook)

    def network_targets(self) -> Sequence[Tuple[str, int]]:
        return [("s3.amazonaws.com", 443)]

    def health_check(
        self,
        storage_hook: Any,
        *,
        container: Optional[str],
        bucket: Optional[str],
        log: logging.Logger,
    ) -> None:
        # Only the typed S3Hook exposes head_bucket cleanly; for the generic
        # AwsGenericHook we skip the explicit check and let upload surface errors.
        if S3Hook is None or not isinstance(storage_hook, S3Hook):
            log.info("S3 generic hook detected → skipping bucket head check.")
            return

        resolved = bucket or getattr(storage_hook, "bucket_name", None)
        if not resolved:
            raise ValueError("S3 bucket must be specified")
        conn = storage_hook.get_conn()
        conn.head_bucket(Bucket=resolved)
        log.info("S3 health check OK ✓ (head_bucket on %s)", resolved)

    def upload(
        self,
        storage_hook: Any,
        local_path: str,
        remote_path: str,
        *,
        container: Optional[str],
        bucket: Optional[str],
        overwrite: bool,
        storage_hook_id: str,
        log: logging.Logger,
    ) -> str:
        import boto3
        from boto3.s3.transfer import TransferConfig
        from botocore.config import Config

        log.info("S3 unified upload: %s → %s", local_path, remote_path)

        resolved = bucket or getattr(storage_hook, "bucket_name", None)
        if not resolved:
            raise ValueError("bucket must be set for S3 uploads")

        if S3Hook is not None and isinstance(storage_hook, S3Hook):
            s3 = storage_hook.get_conn()
        else:
            aws_conn = BaseHook.get_connection(storage_hook_id)
            session = boto3.session.Session(
                aws_access_key_id=aws_conn.login,
                aws_secret_access_key=aws_conn.password,
                region_name=aws_conn.extra_dejson.get("region_name"),
            )
            s3 = session.client(
                "s3",
                config=Config(
                    retries={"max_attempts": 10, "mode": "standard"},
                    connect_timeout=60,
                    read_timeout=60,
                ),
            )

        transfer_cfg = TransferConfig(
            multipart_threshold=64 * 1024 * 1024,
            multipart_chunksize=64 * 1024 * 1024,
            max_concurrency=8,
            use_threads=True,
        )
        s3.upload_file(Filename=local_path, Bucket=resolved, Key=remote_path, Config=transfer_cfg)
        return f"s3://{resolved}/{remote_path}"
