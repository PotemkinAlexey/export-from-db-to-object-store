"""Tests for the uploader registry and matching logic."""
from __future__ import annotations

import pytest

from airflow_export_to_object_store.uploaders import (
    AzureBlobUploader,
    S3Uploader,
    Uploader,
    get_registry,
    resolve_uploader,
)
from airflow_export_to_object_store.uploaders.s3 import _is_aws_generic_hook


class _NotAHook:
    pass


class _SomeAwsHook:
    """Custom hook whose class name happens to contain aws+hook."""


def test_registry_contains_known_backends():
    backends = get_registry()
    names = {b.name for b in backends}
    assert names == {"azure", "s3", "gcs"}


def test_uploaders_satisfy_protocol():
    assert isinstance(AzureBlobUploader(), Uploader)
    assert isinstance(S3Uploader(), Uploader)


def test_unknown_hook_raises_not_implemented():
    with pytest.raises(NotImplementedError):
        resolve_uploader(_NotAHook())


def test_aws_generic_detected_by_class_name():
    assert _is_aws_generic_hook(_SomeAwsHook()) is True


def test_aws_generic_rejects_unrelated_class():
    assert _is_aws_generic_hook(_NotAHook()) is False


def test_s3_uploader_matches_aws_named_hook():
    """Custom AWS-named hooks should resolve to the S3 backend."""
    uploader = resolve_uploader(_SomeAwsHook())
    assert uploader.name == "s3"


def test_network_targets_known():
    assert AzureBlobUploader().network_targets() == [("blob.core.windows.net", 443)]
    assert S3Uploader().network_targets() == [("s3.amazonaws.com", 443)]
