"""Security-sensitive application launch settings."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

from starlette.responses import Response

from garmin_data_hub.ui_nicegui import app as nicegui_app


def test_browser_mode_is_bound_to_loopback(monkeypatch, tmp_path: Path) -> None:
    recorded: dict[str, object] = {}

    class FakeApp:
        @staticmethod
        def add_middleware(middleware, **kwargs) -> None:
            recorded.setdefault("middleware", []).append((middleware, kwargs))

        @staticmethod
        def on_shutdown(callback) -> None:
            recorded["shutdown"] = callback

    class FakeUi:
        @staticmethod
        def run(**kwargs) -> None:
            recorded["run"] = kwargs

    fake_engine = SimpleNamespace(cors_allowed_origins="*")
    fake_core = SimpleNamespace(sio=SimpleNamespace(eio=fake_engine))
    monkeypatch.setitem(
        sys.modules,
        "nicegui",
        SimpleNamespace(app=FakeApp(), core=fake_core, ui=FakeUi()),
    )
    monkeypatch.setattr(
        nicegui_app,
        "_prepare_database",
        lambda _args: (tmp_path / "garmin.db", False),
    )
    monkeypatch.setattr(nicegui_app, "create_ui", lambda *_args, **_kwargs: None)

    nicegui_app.main(["--browser", "--port", "8123"])

    assert recorded["run"] == {
        "title": "Garmin Data Hub",
        "host": "127.0.0.1",
        "native": False,
        "reload": False,
        "port": 8123,
        "show": True,
    }
    assert recorded["middleware"] == [
        (
            nicegui_app.TrustedHostMiddleware,
            {"allowed_hosts": ["127.0.0.1", "localhost"]},
        ),
        (nicegui_app._LocalSecurityHeadersMiddleware, {}),
    ]
    assert fake_engine.cors_allowed_origins == [
        "http://127.0.0.1:8123",
        "http://localhost:8123",
    ]


def test_local_security_headers_prevent_browser_embedding() -> None:
    middleware = nicegui_app._LocalSecurityHeadersMiddleware(lambda *_args: None)

    async def call_next(_request) -> Response:
        return Response("ok")

    response = asyncio.run(middleware.dispatch(None, call_next))

    assert response.headers["content-security-policy"] == "frame-ancestors 'none'"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "no-referrer"
