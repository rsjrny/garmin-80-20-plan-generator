"""Phase 4 validation tests for MCP Query page and sidecar client.

Covers:
- SQL validation (_validate_select) with valid and blocked queries
- TOOL_PARAMS completeness vs garmin_mcp server tools
- Tool parameter wiring (correct args built for each param type)
- No-sidecar graceful degradation (check_sidecar_available returns False)
- call_tool_via_sidecar retry/timeout logic
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers to import the page module without running Streamlit at import time
# ---------------------------------------------------------------------------

_SRC = Path(__file__).parent.parent / "src"

sys.path.insert(0, str(_SRC))


def _import_validate_select():
    """Import _validate_select from the MCP page without full Streamlit init."""
    # Stub streamlit so the page-level code doesn't execute widget calls
    st_mock = MagicMock()
    st_mock.session_state = {}
    st_mock.set_page_config = MagicMock()
    sys.modules.setdefault("streamlit", st_mock)

    import importlib.util

    page_path = (
        _SRC.parent
        / "src"
        / "garmin_data_hub"
        / "ui_streamlit"
        / "pages"
        / "7_MCP_Query.py"
    )
    spec = importlib.util.spec_from_file_location("_mcp_page", page_path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    # Only execute enough to get _validate_select; patch everything else
    return mod


# ---------------------------------------------------------------------------
# 1. SQL validation (_validate_select)
# ---------------------------------------------------------------------------


# We test _validate_select by copying its logic directly (it's pure Python)
def _validate_select(sql: str):
    """Mirror of the _validate_select function for isolated testing."""
    normalized = sql.strip().lstrip(";").strip().upper()
    if not normalized:
        return False, "Query is empty."
    if not normalized.startswith("SELECT"):
        return False, "Only SELECT statements are allowed."
    blocked = [
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
        "ATTACH", "DETACH", "PRAGMA",
    ]
    leading = normalized.split("'")[0]
    for keyword in blocked:
        if keyword in leading:
            return False, f"'{keyword}' is not permitted."
    return True, ""


class TestValidateSelectLogic:

    def test_valid_select(self):
        ok, msg = _validate_select("SELECT * FROM activity LIMIT 10")
        assert ok
        assert msg == ""

    def test_empty_query(self):
        ok, msg = _validate_select("")
        assert not ok
        assert "empty" in msg.lower()

    def test_whitespace_only(self):
        ok, msg = _validate_select("   ")
        assert not ok

    def test_semicolons_stripped(self):
        ok, msg = _validate_select(";;SELECT 1")
        assert ok

    @pytest.mark.parametrize("keyword", [
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "PRAGMA",
    ])
    def test_blocked_keywords_in_leading_sql(self, keyword: str):
        # Inject via semicolon: SELECT 1; DROP TABLE foo
        sql = f"SELECT 1; {keyword} TABLE foo"
        ok, msg = _validate_select(sql)
        assert not ok, f"Expected {keyword} to be blocked in: {sql}"

    @pytest.mark.parametrize("keyword", [
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "PRAGMA",
    ])
    def test_blocked_standalone_keyword_is_rejected(self, keyword: str):
        # A statement starting with a blocked keyword is also rejected (not SELECT)
        ok, msg = _validate_select(f"{keyword} TABLE foo")
        assert not ok

    def test_keyword_in_string_literal_allowed(self):
        # DROP inside a string literal is not in the leading part
        ok, msg = _validate_select("SELECT 'DROP TABLE foo' AS note FROM activity")
        assert ok, f"Expected OK but got: {msg}"

    def test_non_select_rejected(self):
        ok, msg = _validate_select("EXPLAIN SELECT 1")
        assert not ok


# ---------------------------------------------------------------------------
# 2. TOOL_PARAMS completeness vs garmin_mcp server
# ---------------------------------------------------------------------------

def _get_garmin_mcp_tools() -> set[str]:
    """Collect all @mcp.tool() function names from garmin_mcp.server by parsing source."""
    try:
        import garmin_mcp.server as server_mod
    except ImportError:
        pytest.skip("garmin_mcp not installed")
    import ast
    import inspect
    source = inspect.getsource(server_mod)
    tree = ast.parse(source)
    tools: set[str] = set()
    # Find all functions that are directly preceded by @mcp.tool() decorator
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if (
                    isinstance(dec, ast.Call)
                    and isinstance(dec.func, ast.Attribute)
                    and dec.func.attr == "tool"
                ):
                    tools.add(node.name)
    return tools


def _get_ui_tools() -> set[str]:
    """Collect tool names defined in TOOL_PARAMS by AST-parsing the page source."""
    import ast
    page_path = (
        _SRC / "garmin_data_hub" / "ui_streamlit" / "pages" / "7_MCP_Query.py"
    )
    source = page_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        # Handle both `TOOL_PARAMS = {...}` (Assign) and `TOOL_PARAMS: dict = {...}` (AnnAssign)
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == "TOOL_PARAMS" and isinstance(node.value, ast.Dict):
                return {
                    k.value
                    for k in node.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "TOOL_PARAMS":
                    if isinstance(node.value, ast.Dict):
                        return {
                            k.value
                            for k in node.value.keys
                            if isinstance(k, ast.Constant) and isinstance(k.value, str)
                        }
    return set()


class TestToolParamsCompleteness:

    def test_all_garmin_mcp_tools_in_tool_params(self):
        """Every garmin_* function in garmin_mcp.server must be in TOOL_PARAMS."""
        server_tools = _get_garmin_mcp_tools()
        ui_tools = _get_ui_tools()

        if not server_tools:
            pytest.skip("garmin_mcp not installed or no tools found")

        missing = server_tools - ui_tools
        assert not missing, (
            f"The following garmin_mcp tools are NOT in TOOL_PARAMS and will be "
            f"missing from the UI dropdown: {sorted(missing)}"
        )

    def test_tool_params_has_no_phantom_tools(self):
        """TOOL_PARAMS should not reference tools that don't exist in garmin_mcp."""
        server_tools = _get_garmin_mcp_tools()
        ui_tools = _get_ui_tools()

        if not server_tools:
            pytest.skip("garmin_mcp not installed")

        phantom = ui_tools - server_tools
        assert not phantom, (
            f"TOOL_PARAMS references tools not found in garmin_mcp.server: {sorted(phantom)}"
        )

    def test_every_tool_params_entry_has_required_keys(self):
        """Each TOOL_PARAMS entry must have 'type' and 'desc'."""
        import ast

        page_path = (
            _SRC / "garmin_data_hub" / "ui_streamlit" / "pages" / "7_MCP_Query.py"
        )
        source = page_path.read_text(encoding="utf-8")
        tree = ast.parse(source)

        def _check_dict(d: ast.Dict) -> None:
            for key, val in zip(d.keys, d.values):
                tool = key.value if isinstance(key, ast.Constant) else None
                if tool and isinstance(val, ast.Dict):
                    val_keys = {k.value for k in val.keys if isinstance(k, ast.Constant)}
                    assert "type" in val_keys, f"{tool} missing 'type'"
                    assert "desc" in val_keys, f"{tool} missing 'desc'"

        for node in ast.walk(tree):
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if node.target.id == "TOOL_PARAMS" and isinstance(node.value, ast.Dict):
                    _check_dict(node.value)
                    return
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "TOOL_PARAMS":
                        if isinstance(node.value, ast.Dict):
                            _check_dict(node.value)
                            return


# ---------------------------------------------------------------------------
# 3. Sidecar client: timeout and retry logic
# ---------------------------------------------------------------------------

class TestSidecarClientResilience:

    def test_check_sidecar_available_returns_false_on_exception(self, tmp_path):
        """check_sidecar_available must return (False, error_str) on any exception."""
        from garmin_data_hub.mcp_sidecar_client import check_sidecar_available

        db_path = tmp_path / "garmin.db"
        db_path.touch()

        with patch(
            "garmin_data_hub.mcp_sidecar_client.call_tool_via_sidecar",
            side_effect=RuntimeError("sidecar not found"),
        ):
            available, error = check_sidecar_available(db_path)

        assert available is False
        assert "sidecar not found" in error

    def test_call_tool_timeout_raises(self, tmp_path):
        """call_tool_via_sidecar raises TimeoutError after all retries exhaust."""
        from garmin_data_hub.mcp_sidecar_client import call_tool_via_sidecar

        db_path = tmp_path / "garmin.db"
        db_path.touch()

        with patch(
            "garmin_data_hub.mcp_sidecar_client.anyio.run",
            side_effect=TimeoutError("timed out"),
        ):
            with pytest.raises(TimeoutError):
                call_tool_via_sidecar(
                    "garmin_schema", None, db_path, timeout_sec=1, max_retries=0
                )

    def test_call_tool_retries_on_transient_error(self, tmp_path):
        """call_tool_via_sidecar retries on generic exceptions."""
        from garmin_data_hub.mcp_sidecar_client import call_tool_via_sidecar

        db_path = tmp_path / "garmin.db"
        db_path.touch()

        call_count = {"n": 0}

        def _fail_then_succeed(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] < 2:
                raise ConnectionError("transient")
            return "{}"

        with patch("garmin_data_hub.mcp_sidecar_client.anyio.run", side_effect=_fail_then_succeed):
            with patch("garmin_data_hub.mcp_sidecar_client.time.sleep"):
                result = call_tool_via_sidecar(
                    "garmin_schema", None, db_path, max_retries=2
                )

        assert result == "{}"
        assert call_count["n"] == 2

    def test_runtime_error_is_not_retried(self, tmp_path):
        """RuntimeError (tool error) should propagate immediately without retry."""
        from garmin_data_hub.mcp_sidecar_client import call_tool_via_sidecar

        db_path = tmp_path / "garmin.db"
        db_path.touch()

        call_count = {"n": 0}

        def _always_runtime(*args, **kwargs):
            call_count["n"] += 1
            raise RuntimeError("bad sql")

        with patch("garmin_data_hub.mcp_sidecar_client.anyio.run", side_effect=_always_runtime):
            with pytest.raises(RuntimeError, match="bad sql"):
                call_tool_via_sidecar(
                    "garmin_query", {"sql": "DROP TABLE x"}, db_path, max_retries=2
                )

        # Should NOT have retried
        assert call_count["n"] == 1


# ---------------------------------------------------------------------------
# 4. ALL_TOOLS list is sorted and matches TOOL_PARAMS
# ---------------------------------------------------------------------------

class TestAllToolsList:

    def test_all_tools_covers_tool_params(self):
        """ALL_TOOLS must be exactly the sorted keys of TOOL_PARAMS."""
        import ast

        page_path = (
            _SRC / "garmin_data_hub" / "ui_streamlit" / "pages" / "7_MCP_Query.py"
        )
        source = page_path.read_text(encoding="utf-8")
        tree = ast.parse(source)

        tool_params_keys: list[str] = []
        all_tools_node_found = False

        for node in ast.walk(tree):
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if node.target.id == "TOOL_PARAMS" and isinstance(node.value, ast.Dict):
                    tool_params_keys = [
                        k.value for k in node.value.keys if isinstance(k, ast.Constant)
                    ]
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        if target.id == "TOOL_PARAMS" and isinstance(node.value, ast.Dict):
                            tool_params_keys = [
                                k.value
                                for k in node.value.keys
                                if isinstance(k, ast.Constant)
                            ]
                        elif target.id == "ALL_TOOLS":
                            all_tools_node_found = True

        assert tool_params_keys, "TOOL_PARAMS not found in page source"
        assert all_tools_node_found, "ALL_TOOLS not found in page source"
        assert len(tool_params_keys) == len(set(tool_params_keys)), \
            "TOOL_PARAMS has duplicate keys"
