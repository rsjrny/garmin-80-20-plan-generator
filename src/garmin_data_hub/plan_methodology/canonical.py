"""Deterministic canonical JSON and prescription content hashing."""

from __future__ import annotations

import hashlib
import json
from dataclasses import fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping

from .domain import DomainError, canonical_decimal


NON_CONTENT_KEYS = frozenset(
    {
        "activity_match",
        "activity_matches",
        "activities",
        "compliance",
        "compliance_cache",
        "display_overlays",
        "execution_duration",
        "log_path",
        "operational_telemetry",
        "observations",
        "runtime_matches",
        "runtime_observations",
        "runtime_state",
        "sensor_observations",
        "temporary_files",
        "ui_state",
    }
)


def _canonical_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DomainError("canonical datetimes must include a timezone")
    utc_value = value.astimezone(timezone.utc)
    rendered = utc_value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if "." in rendered:
        date_part, suffix = rendered[:-1].split(".", 1)
        rendered = f"{date_part}.{suffix.rstrip('0')}Z" if suffix.rstrip("0") else f"{date_part}Z"
    return rendered


def canonical_value(value: Any) -> Any:
    """Convert supported immutable domain content into JSON-native values."""
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        raise DomainError("binary floating-point values are not canonical domain content")
    if isinstance(value, Decimal):
        return canonical_decimal(value)
    if isinstance(value, Enum):
        return canonical_value(value.value)
    if isinstance(value, datetime):
        return _canonical_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value):
        return {field.name: canonical_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (Mapping, MappingProxyType)):
        if any(not isinstance(key, str) for key in value):
            raise DomainError("canonical object keys must be strings")
        return {key: canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        raise DomainError("unordered sets are not canonical domain content")
    raise DomainError(f"unsupported canonical value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonical_value(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def prescription_content(value: Any) -> Any:
    """Remove explicitly runtime/presentation-only content before hashing."""
    normalized = canonical_value(value)
    if isinstance(normalized, dict):
        return {
            key: prescription_content(item)
            for key, item in normalized.items()
            if key not in NON_CONTENT_KEYS
        }
    if isinstance(normalized, list):
        return [prescription_content(item) for item in normalized]
    return normalized


def content_sha256(value: Any) -> str:
    payload = canonical_json(prescription_content(value)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
