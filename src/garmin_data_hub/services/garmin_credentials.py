"""Garmin Connect credentials backed by Windows Credential Manager.

The desktop application uses one Generic Credential for the current Windows
user.  The email is the credential username and the password is the protected
credential blob.  Credentials are copied into a child-process environment only
when a sync starts; they are never added to its command line.
"""

from __future__ import annotations

import ctypes
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol


GARMIN_EMAIL_ENV = "GARMIN_EMAIL"
GARMIN_PASSWORD_ENV = "GARMIN_PASSWORD"
WINDOWS_CREDENTIAL_TARGET = "GarminDataHub:GarminConnect"
WINDOWS_CREDENTIAL_BACKEND = "Windows Credential Manager"

_CRED_TYPE_GENERIC = 1
_CRED_PERSIST_LOCAL_MACHINE = 2
_ERROR_NOT_FOUND = 1168
_MAX_CREDENTIAL_BLOB_BYTES = 5 * 512
_MAX_USERNAME_CHARACTERS = 512


class CredentialStoreError(RuntimeError):
    """A credential-store operation could not be completed."""


class CredentialStoreUnavailable(CredentialStoreError):
    """The platform does not provide the configured credential store."""


@dataclass(frozen=True)
class GarminCredentials:
    """Validated Garmin Connect credentials.

    ``password`` is excluded from the generated representation so accidental
    logging of this object does not reveal it.
    """

    email: str
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        email = self.email.strip()
        if not email:
            raise ValueError("Garmin email is required")
        if not self.password:
            raise ValueError("Garmin password is required")
        if "\x00" in email or "\x00" in self.password:
            raise ValueError("Garmin credentials cannot contain null characters")
        if len(email) > _MAX_USERNAME_CHARACTERS:
            raise ValueError("Garmin email is too long for Windows Credential Manager")
        password_bytes = self.password.encode("utf-16-le")
        if len(password_bytes) > _MAX_CREDENTIAL_BLOB_BYTES:
            raise ValueError("Garmin password is too long for Windows Credential Manager")
        object.__setattr__(self, "email", email)


@dataclass(frozen=True)
class CredentialStatus:
    """Non-secret information suitable for display on the Sync page."""

    is_configured: bool
    email: str | None
    backend_name: str


class CredentialStore(Protocol):
    """Storage contract used by the UI and kept injectable for tests."""

    @property
    def backend_name(self) -> str: ...

    def load(self) -> GarminCredentials | None: ...

    def save(self, credentials: GarminCredentials) -> None: ...

    def delete(self) -> bool: ...


class _CredentialApi(Protocol):
    def read(self, target_name: str) -> GarminCredentials | None: ...

    def write(self, target_name: str, credentials: GarminCredentials) -> None: ...

    def delete(self, target_name: str) -> bool: ...


class _FileTime(ctypes.Structure):
    _fields_ = [
        ("low_date_time", ctypes.c_uint32),
        ("high_date_time", ctypes.c_uint32),
    ]


class _CredentialW(ctypes.Structure):
    _fields_ = [
        ("flags", ctypes.c_uint32),
        ("type", ctypes.c_uint32),
        ("target_name", ctypes.c_wchar_p),
        ("comment", ctypes.c_wchar_p),
        ("last_written", _FileTime),
        ("credential_blob_size", ctypes.c_uint32),
        ("credential_blob", ctypes.c_void_p),
        ("persist", ctypes.c_uint32),
        ("attribute_count", ctypes.c_uint32),
        ("attributes", ctypes.c_void_p),
        ("target_alias", ctypes.c_wchar_p),
        ("user_name", ctypes.c_wchar_p),
    ]


_CredentialPointer = ctypes.POINTER(_CredentialW)


class _WindowsCredentialApi:
    """Small, private wrapper around the WinCred API."""

    def __init__(self) -> None:
        if sys.platform != "win32":
            raise CredentialStoreUnavailable(
                "Windows Credential Manager is only available on Windows"
            )

        try:
            advapi32 = ctypes.WinDLL("Advapi32.dll", use_last_error=True)
        except (AttributeError, OSError) as exc:
            raise CredentialStoreUnavailable(
                "Windows Credential Manager could not be loaded"
            ) from exc

        advapi32.CredReadW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.POINTER(_CredentialPointer),
        ]
        advapi32.CredReadW.restype = ctypes.c_int
        advapi32.CredWriteW.argtypes = [
            ctypes.POINTER(_CredentialW),
            ctypes.c_uint32,
        ]
        advapi32.CredWriteW.restype = ctypes.c_int
        advapi32.CredDeleteW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
        ]
        advapi32.CredDeleteW.restype = ctypes.c_int
        advapi32.CredFree.argtypes = [ctypes.c_void_p]
        advapi32.CredFree.restype = None
        self._advapi32 = advapi32

    def read(self, target_name: str) -> GarminCredentials | None:
        credential_pointer = _CredentialPointer()
        succeeded = self._advapi32.CredReadW(
            target_name,
            _CRED_TYPE_GENERIC,
            0,
            ctypes.byref(credential_pointer),
        )
        if not succeeded:
            error_code = ctypes.get_last_error()
            if error_code == _ERROR_NOT_FOUND:
                return None
            raise _wincred_error("read credentials", error_code)

        try:
            credential = credential_pointer.contents
            password_bytes = ctypes.string_at(
                credential.credential_blob,
                credential.credential_blob_size,
            )
            try:
                password = password_bytes.decode("utf-16-le")
            except UnicodeDecodeError as exc:
                raise CredentialStoreError(
                    "Windows Credential Manager returned an invalid Garmin credential"
                ) from exc
            return GarminCredentials(
                email=credential.user_name or "",
                password=password,
            )
        except ValueError as exc:
            raise CredentialStoreError(
                "Windows Credential Manager returned an invalid Garmin credential"
            ) from exc
        finally:
            self._advapi32.CredFree(credential_pointer)

    def write(self, target_name: str, credentials: GarminCredentials) -> None:
        password_bytes = credentials.password.encode("utf-16-le")
        blob_buffer = (ctypes.c_ubyte * len(password_bytes)).from_buffer_copy(
            password_bytes
        )
        credential = _CredentialW()
        credential.type = _CRED_TYPE_GENERIC
        credential.target_name = target_name
        credential.credential_blob_size = len(password_bytes)
        credential.credential_blob = ctypes.cast(blob_buffer, ctypes.c_void_p)
        credential.persist = _CRED_PERSIST_LOCAL_MACHINE
        credential.user_name = credentials.email

        if not self._advapi32.CredWriteW(ctypes.byref(credential), 0):
            raise _wincred_error("save credentials", ctypes.get_last_error())

    def delete(self, target_name: str) -> bool:
        succeeded = self._advapi32.CredDeleteW(
            target_name,
            _CRED_TYPE_GENERIC,
            0,
        )
        if succeeded:
            return True
        error_code = ctypes.get_last_error()
        if error_code == _ERROR_NOT_FOUND:
            return False
        raise _wincred_error("delete credentials", error_code)


def _wincred_error(operation: str, error_code: int) -> CredentialStoreError:
    try:
        detail = ctypes.FormatError(error_code).strip()
    except (AttributeError, OSError):
        detail = ""
    suffix = f": {detail}" if detail else ""
    return CredentialStoreError(
        f"Windows Credential Manager could not {operation} "
        f"(error {error_code}){suffix}"
    )


class WindowsCredentialStore:
    """Store Garmin credentials in the current user's Windows credential vault."""

    backend_name = WINDOWS_CREDENTIAL_BACKEND

    def __init__(self, *, api: _CredentialApi | None = None) -> None:
        self._api = api if api is not None else _WindowsCredentialApi()

    def load(self) -> GarminCredentials | None:
        return self._api.read(WINDOWS_CREDENTIAL_TARGET)

    def save(self, credentials: GarminCredentials) -> None:
        self._api.write(WINDOWS_CREDENTIAL_TARGET, credentials)

    def delete(self) -> bool:
        return self._api.delete(WINDOWS_CREDENTIAL_TARGET)


def default_credential_store() -> CredentialStore:
    """Return the native credential store for this platform."""
    return WindowsCredentialStore()


def load_credentials(
    *, store: CredentialStore | None = None
) -> GarminCredentials | None:
    """Load saved Garmin credentials, or ``None`` when none have been saved."""
    return (store or default_credential_store()).load()


def save_credentials(
    email: str,
    password: str,
    *,
    store: CredentialStore | None = None,
) -> GarminCredentials:
    """Validate and save Garmin credentials, returning the normalized values."""
    credentials = GarminCredentials(email=email, password=password)
    (store or default_credential_store()).save(credentials)
    return credentials


def delete_credentials(*, store: CredentialStore | None = None) -> bool:
    """Delete saved Garmin credentials; return whether an entry existed."""
    return (store or default_credential_store()).delete()


def credential_status(*, store: CredentialStore | None = None) -> CredentialStatus:
    """Return non-secret configuration status for display by the UI."""
    selected_store = store or default_credential_store()
    credentials = selected_store.load()
    return CredentialStatus(
        is_configured=credentials is not None,
        email=credentials.email if credentials is not None else None,
        backend_name=selected_store.backend_name,
    )


def build_sync_environment(
    credentials: GarminCredentials,
    *,
    base_environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Copy an environment and overlay credentials for the sync child process.

    This function never mutates ``os.environ`` or the provided mapping.  The
    returned dictionary should be passed directly to ``subprocess.Popen``.
    """
    environment = dict(os.environ if base_environment is None else base_environment)
    environment[GARMIN_EMAIL_ENV] = credentials.email
    environment[GARMIN_PASSWORD_ENV] = credentials.password
    return environment
