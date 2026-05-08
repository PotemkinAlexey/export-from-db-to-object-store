"""Pluggable uploader backends for object storage."""

from __future__ import annotations

from .azure import AzureBlobUploader
from .base import Uploader, get_registry, resolve_uploader
from .gcs import GCSUploader
from .s3 import S3Uploader

__all__ = [
    "Uploader",
    "get_registry",
    "resolve_uploader",
    "AzureBlobUploader",
    "GCSUploader",
    "S3Uploader",
]
