"""Retry decorator with exponential backoff."""
from __future__ import annotations

import time
from functools import wraps


def with_retries(func):
    """Retry a method using ``self.retry_options`` for backoff configuration."""

    @wraps(func)
    def wrap(self, *args, **kwargs):
        attempt = 0
        while True:
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                attempt += 1
                if attempt > self.retry_options.upload_retries:
                    raise
                sleep = min(self.retry_options.backoff_base**attempt, self.retry_options.backoff_cap)
                self.log.warning(
                    "%s retry %d/%d in %.1fs: %s",
                    func.__name__,
                    attempt,
                    self.retry_options.upload_retries,
                    sleep,
                    e,
                )
                time.sleep(sleep)

    return wrap
