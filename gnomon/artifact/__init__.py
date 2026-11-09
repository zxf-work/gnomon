"""
Public API re-export only, over its guarded siblings (the IR's boundary table
— `record.py`, `registry.py`, `lookup.py`, `witness.py`; **not**
`refresh.py`, which deliberately does not join
as `gnomon.artifact.refresh` by whatever needs it).
"""

from __future__ import annotations

from gnomon.artifact.lookup import LookupResult, affected, affected_over
from gnomon.artifact.record import (
    REFRESH_KINDS, SCHEMA_NAME, SCHEMA_VERSION, ArtifactId, ArtifactRecord, StepDependency,
)
from gnomon.artifact.registry import Registry
from gnomon.artifact.witness import (
    ArtifactVerdict, ExemptReceipt, RefreshHandle, TermRef, check_artifact, render_verdict,
)

__all__ = [
    "REFRESH_KINDS", "SCHEMA_NAME", "SCHEMA_VERSION", "ArtifactId", "ArtifactRecord",
    "ArtifactVerdict", "ExemptReceipt", "LookupResult", "RefreshHandle", "Registry",
    "StepDependency", "TermRef", "affected", "affected_over", "check_artifact",
    "render_verdict",
]
