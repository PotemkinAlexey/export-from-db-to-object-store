"""AWS S3 uploader using boto3 transfer (automatic multipart)."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import quote

from airflow.hooks.base import BaseHook

from ..encryption import EncryptionOptions

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

    def network_targets(self) -> Sequence[tuple[str, int]]:
        return [("s3.amazonaws.com", 443)]

    def health_check(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
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

    def exists(
        self,
        storage_hook: Any,
        *,
        container: str | None,
        bucket: str | None,
        remote_path: str,
    ) -> bool:
        resolved = bucket or getattr(storage_hook, "bucket_name", None)
        if not resolved:
            return False
        try:
            if S3Hook is not None and isinstance(storage_hook, S3Hook):
                client = storage_hook.get_conn()
            else:
                import boto3

                aws_conn = BaseHook.get_connection(getattr(storage_hook, "aws_conn_id", None) or "aws_default")
                client = boto3.session.Session(
                    aws_access_key_id=aws_conn.login,
                    aws_secret_access_key=aws_conn.password,
                    region_name=aws_conn.extra_dejson.get("region_name"),
                ).client("s3")
            client.head_object(Bucket=resolved, Key=remote_path)
            return True
        except Exception:
            return False

    def upload(
        self,
        storage_hook: Any,
        local_path: str,
        remote_path: str,
        *,
        container: str | None,
        bucket: str | None,
        overwrite: bool,
        storage_hook_id: str,
        log: logging.Logger,
        encryption: EncryptionOptions | None = None,
        tags: Mapping[str, str] | None = None,
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
        extra_args = _build_extra_args(encryption=encryption, tags=tags)
        s3.upload_file(
            Filename=local_path,
            Bucket=resolved,
            Key=remote_path,
            Config=transfer_cfg,
            ExtraArgs=extra_args or None,
        )
        return f"s3://{resolved}/{remote_path}"


def _build_extra_args(*, encryption: EncryptionOptions | None, tags: Mapping[str, str] | None) -> dict[str, Any]:
    """Translate generic encryption + tags options into S3 ``ExtraArgs``.

    SSE-S3 (``"AES256"``) needs no key id; SSE-KMS (``"aws:kms"``)
    requires ``kms_key_id``. Object tags are URL-encoded as the
    ``"k1=v1&k2=v2"`` shape AWS expects.
    """
    extra: dict[str, Any] = {}
    if encryption is not None:
        if encryption.sse_algorithm:
            extra["ServerSideEncryption"] = encryption.sse_algorithm
        if encryption.kms_key_id:
            extra["SSEKMSKeyId"] = encryption.kms_key_id
            # SSE-KMS implies "aws:kms" if caller didn't set the algorithm.
            extra.setdefault("ServerSideEncryption", "aws:kms")
    if tags:
        extra["Tagging"] = "&".join(f"{quote(str(k), safe='')}={quote(str(v), safe='')}" for k, v in tags.items())
    return extra
