"""gnomon.telemetry — a dependency-free JSONL metrics sink.

This package is a skeleton: `Metrics` is standalone and is not yet
wired into `gnomon.store` or the server.
"""

from gnomon.telemetry.metrics import Metrics

__all__ = ["Metrics"]
