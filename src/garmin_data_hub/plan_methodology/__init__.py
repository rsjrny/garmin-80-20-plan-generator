"""Pure domain foundation for versioned training methodologies.

This package intentionally has no database, UI, Garmin, or AI dependencies.
"""

from .domain import (
    ApprovalState,
    Confidence,
    DataQuality,
    EvidenceClass,
    FitzgeraldCategory,
    FitzgeraldTarget,
    GoalIntent,
    LoadMode,
    MaffetoneTarget,
    MethodologyId,
    Metric,
    RevisionReason,
    SegmentKind,
    Sport,
)

__all__ = [
    "ApprovalState",
    "Confidence",
    "DataQuality",
    "EvidenceClass",
    "FitzgeraldCategory",
    "FitzgeraldTarget",
    "GoalIntent",
    "LoadMode",
    "MaffetoneTarget",
    "MethodologyId",
    "Metric",
    "RevisionReason",
    "SegmentKind",
    "Sport",
]
