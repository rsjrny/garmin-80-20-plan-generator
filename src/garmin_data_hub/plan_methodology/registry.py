"""Small static registry of immutable methodology definitions."""

from __future__ import annotations

from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any, Mapping, Protocol

from .canonical import content_sha256
from .domain import DomainError, MethodologyId, Sport, parse_enum


class ParameterDeriver(Protocol):
    def derive(self, **inputs: Any) -> Any: ...


class MethodologyPolicy(Protocol):
    def validate(self, value: Any) -> Any: ...


class ComplianceEvaluator(Protocol):
    def evaluate(self, prescription: Any, observations: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class MethodologyManifest:
    methodology_id: MethodologyId
    methodology_version: int
    specification_version: str
    product_policy_version: str
    effective_manifest_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "methodology_id",
            parse_enum(MethodologyId, self.methodology_id, "methodology_id"),
        )
        if not isinstance(self.methodology_version, int) or isinstance(
            self.methodology_version, bool
        ):
            raise DomainError("methodology_version must be an integer")
        expected_version = 0 if self.methodology_id is MethodologyId.LEGACY_UNSPECIFIED else 1
        if self.methodology_version != expected_version:
            raise DomainError("methodology_version does not match methodology identity")
        if not self.specification_version or not self.product_policy_version:
            raise DomainError("manifest specification and policy versions are required")
        if not self.effective_manifest_hash.startswith("sha256:") or len(
            self.effective_manifest_hash
        ) <= len("sha256:"):
            raise DomainError("effective_manifest_hash must use sha256 identity")


@dataclass(frozen=True, slots=True)
class MethodologyDefinition:
    methodology_id: MethodologyId | str
    display_name: str
    sport: Sport
    version: str
    collaborators: tuple[str, ...]
    manifest: MethodologyManifest | None = None

    def __post_init__(self) -> None:
        if isinstance(self.methodology_id, str):
            try:
                method: MethodologyId | str = MethodologyId(self.methodology_id)
            except ValueError:
                method = self.methodology_id
            object.__setattr__(self, "methodology_id", method)
        object.__setattr__(self, "sport", parse_enum(Sport, self.sport, "sport"))
        object.__setattr__(self, "collaborators", tuple(self.collaborators))
        if not self.display_name or not self.version:
            raise DomainError("definition display_name and version are required")
        if len(self.collaborators) != 3:
            raise DomainError("a methodology definition requires exactly three collaborators")
        if len(set(self.collaborators)) != 3:
            raise DomainError("methodology collaborators must be distinct")

    @property
    def id_value(self) -> str:
        return (
            self.methodology_id.value
            if isinstance(self.methodology_id, MethodologyId)
            else self.methodology_id
        )


def _manifest(methodology_id: MethodologyId, version: int = 1) -> MethodologyManifest:
    payload = {
        "methodology_id": methodology_id.value,
        "methodology_version": version,
        "specification_version": "1.0.0",
        "product_policy_version": "1.0.0",
    }
    return MethodologyManifest(
        methodology_id=methodology_id,
        methodology_version=version,
        specification_version="1.0.0",
        product_policy_version="1.0.0",
        effective_manifest_hash=f"sha256:{content_sha256(payload)}",
    )


COLLABORATORS = ("ParameterDeriver", "MethodologyPolicy", "ComplianceEvaluator")

STATIC_DEFINITIONS = (
    MethodologyDefinition(
        MethodologyId.FITZGERALD_80_20_RUNNING_V1,
        "80/20 Running (Fitzgerald)",
        Sport.RUNNING,
        "1",
        COLLABORATORS,
        _manifest(MethodologyId.FITZGERALD_80_20_RUNNING_V1),
    ),
    MethodologyDefinition(
        MethodologyId.MAFFETONE_RUNNING_V1,
        "Maffetone",
        Sport.RUNNING,
        "1",
        COLLABORATORS,
        _manifest(MethodologyId.MAFFETONE_RUNNING_V1),
    ),
    MethodologyDefinition(
        MethodologyId.LEGACY_UNSPECIFIED,
        "legacy_unspecified",
        Sport.RUNNING,
        "0",
        COLLABORATORS,
        _manifest(MethodologyId.LEGACY_UNSPECIFIED, version=0),
    ),
)


@dataclass(frozen=True, slots=True)
class MethodologyRegistry:
    _definitions: Mapping[str, MethodologyDefinition]

    @classmethod
    def static(cls) -> "MethodologyRegistry":
        return cls(MappingProxyType({item.id_value: item for item in STATIC_DEFINITIONS}))

    def get(self, methodology_id: str) -> MethodologyDefinition:
        try:
            return self._definitions[methodology_id]
        except KeyError as exc:
            raise DomainError(f"unknown methodology: {methodology_id}") from exc

    def register(self, definition: MethodologyDefinition) -> "MethodologyRegistry":
        if definition.id_value in self._definitions:
            raise DomainError(f"methodology already registered: {definition.id_value}")
        extended = dict(self._definitions)
        extended[definition.id_value] = definition
        return replace(self, _definitions=MappingProxyType(extended))


REGISTRY = MethodologyRegistry.static()


def list_methodologies() -> dict[str, Any]:
    return {"ids": [definition.id_value for definition in STATIC_DEFINITIONS]}


def get_methodology(*, methodology_id: str) -> dict[str, Any]:
    try:
        definition = REGISTRY.get(methodology_id)
    except DomainError:
        return {"accepted": False, "reason": "UNKNOWN_METHODOLOGY"}
    return {
        "methodology_id": definition.id_value,
        "display_name": definition.display_name,
        "sport": definition.sport.value,
        "version": definition.version,
        "collaborators": list(definition.collaborators),
        "immutable": True,
    }


def validate_manifest(*, manifest: Mapping[str, Any]) -> dict[str, Any]:
    try:
        method = parse_enum(MethodologyId, manifest["methodology_id"], "methodology_id")
        parsed = MethodologyManifest(
            methodology_id=method,
            methodology_version=int(manifest["methodology_version"]),
            specification_version=str(manifest["specification_version"]),
            product_policy_version=str(manifest["product_policy_version"]),
            effective_manifest_hash=str(manifest["effective_manifest_hash"]),
        )
        expected_version = 0 if method is MethodologyId.LEGACY_UNSPECIFIED else 1
        valid = (
            parsed.methodology_version == expected_version
            and parsed.specification_version == "1.0.0"
            and parsed.product_policy_version == "1.0.0"
        )
    except (DomainError, KeyError, TypeError, ValueError):
        valid = False
    return {"valid": valid, "immutable": True}


def register_local_definition(*, definition: Mapping[str, Any]) -> dict[str, Any]:
    local = MethodologyDefinition(
        methodology_id=str(definition["methodology_id"]),
        display_name=str(definition.get("display_name", definition["methodology_id"])),
        sport=parse_enum(Sport, definition["sport"], "sport"),
        version=str(definition.get("version", "1")),
        collaborators=tuple(definition["collaborators"]),
    )
    REGISTRY.register(local)
    return {
        "registered": True,
        "modified_existing_methods": False,
        "modified_shared_repository": False,
    }
