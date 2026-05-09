"""Server-side encryption options for the upload step.

Each cloud has its own knobs; the ``EncryptionOptions`` dataclass
carries all of them so the operator stays backend-agnostic. Each
uploader picks the fields it understands and ignores the rest:

* **AWS S3** uses ``sse_algorithm`` and (for SSE-KMS)
  ``kms_key_id``. ``sse_algorithm="AES256"`` selects SSE-S3
  (Amazon-managed keys); ``"aws:kms"`` requires ``kms_key_id``.
* **Azure Blob Storage** uses ``encryption_scope`` (an account-level
  reference to the Key Vault key your admin has wired up). Per-blob
  customer-provided keys are intentionally not supported here — they
  break server-side capabilities like blob index tags and have a
  worse operational story.
* **Google Cloud Storage** uses ``kms_key_name`` (full resource name
  of a Cloud KMS key, e.g. ``projects/.../keyRings/.../cryptoKeys/...``).

Compliance regimes (HIPAA, SOC 2, GDPR) typically want
*customer-managed* keys; with no encryption set, every backend
defaults to its provider-managed encryption (which is always on).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EncryptionOptions:
    # AWS S3
    kms_key_id: str | None = None
    sse_algorithm: str | None = None  # "AES256" | "aws:kms"

    # Azure Blob Storage
    encryption_scope: str | None = None

    # Google Cloud Storage
    kms_key_name: str | None = None
