import os
import subprocess
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest

from tools.execution_backends import (
    PbQueryExecutionBackend,
    SnowflakePbQueryStrategy,
)


@pytest.fixture(autouse=True)
def clear_schema_version_cache():
    PbQueryExecutionBackend._schema_version_cache = None
    yield
    PbQueryExecutionBackend._schema_version_cache = None


def _make_backend() -> PbQueryExecutionBackend:
    backend = PbQueryExecutionBackend("snowflake")
    backend._strategy = SnowflakePbQueryStrategy()
    backend._connection_name = "snowflake_conn"
    backend._siteconfig_path = backend._default_siteconfig_path()
    backend._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_test_")
    os.makedirs(os.path.join(backend._stub_project_path, "output"), exist_ok=True)
    backend._session = True
    backend._pb_initialized = True
    return backend


class TestSchemaVersion:
    def test_parses_schema_version(self):
        result = subprocess.CompletedProcess(
            args=["pb", "version"],
            returncode=0,
            stdout="Profiles v0.1\nNative schema version: 91\n",
            stderr="",
        )

        with patch("tools.execution_backends.subprocess.run", return_value=result):
            version = PbQueryExecutionBackend._get_schema_version()

        assert version == 91

    def test_caches_schema_version(self):
        result = subprocess.CompletedProcess(
            args=["pb", "version"],
            returncode=0,
            stdout="Native schema version: 90\n",
            stderr="",
        )

        with patch(
            "tools.execution_backends.subprocess.run", return_value=result
        ) as mock_run:
            v1 = PbQueryExecutionBackend._get_schema_version()
            v2 = PbQueryExecutionBackend._get_schema_version()

        assert v1 == 90
        assert v2 == 90
        mock_run.assert_called_once()


class TestRawQuery:
    def test_maps_subprocess_timeout_to_concise_error(self):
        backend = _make_backend()

        with patch(
            "tools.execution_backends.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="pb query", timeout=540),
        ):
            with pytest.raises(RuntimeError, match="timed out"):
                backend.raw_query("SELECT 1")

        backend.cleanup()

    def test_maps_subprocess_error_to_concise_first_line(self):
        backend = _make_backend()
        result = subprocess.CompletedProcess(
            args=["pb", "query"],
            returncode=1,
            stdout="",
            stderr="\x1b[31mERROR: bad SQL\x1b[0m\nstack trace details",
        )

        with patch("tools.execution_backends.subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="ERROR: bad SQL"):
                backend.raw_query("SELECT broken")

        backend.cleanup()

    def test_raises_when_csv_not_written(self):
        backend = _make_backend()
        result = subprocess.CompletedProcess(
            args=["pb", "query"], returncode=0, stdout="ok", stderr=""
        )

        with patch("tools.execution_backends.subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="no output file"):
                backend.raw_query("SELECT 1")

        backend.cleanup()

    def test_pandas_mode_converts_nil_to_null_string(self):
        backend = _make_backend()

        def fake_run(cmd, **kwargs):
            csv_name = cmd[cmd.index("-f") + 1]
            csv_path = os.path.join(backend._stub_project_path, "output", csv_name)
            with open(csv_path, "w") as handle:
                handle.write("COL_A,COL_B\nfoo,<nil>\nbar,baz\n")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.execution_backends.subprocess.run", side_effect=fake_run):
            df = backend.raw_query("SELECT * FROM T", response_type="pandas")

        assert isinstance(df, pd.DataFrame)
        assert df.iloc[0]["COL_B"] == "Null"
        backend.cleanup()

    def test_list_mode_keeps_nil_as_nan(self):
        backend = _make_backend()

        def fake_run(cmd, **kwargs):
            csv_name = cmd[cmd.index("-f") + 1]
            csv_path = os.path.join(backend._stub_project_path, "output", csv_name)
            with open(csv_path, "w") as handle:
                handle.write("COL_A,COL_B\nfoo,<nil>\n")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.execution_backends.subprocess.run", side_effect=fake_run):
            rows = backend.raw_query("SELECT * FROM T", response_type="list")

        assert len(rows) == 1
        assert pd.isna(rows[0]["COL_B"])
        backend.cleanup()

    def test_empty_csv_returns_empty_rows_in_list_mode(self):
        backend = _make_backend()

        def fake_run(cmd, **kwargs):
            csv_name = cmd[cmd.index("-f") + 1]
            csv_path = os.path.join(backend._stub_project_path, "output", csv_name)
            # Write an empty file to emulate no tabular result payload.
            with open(csv_path, "w") as handle:
                handle.write("")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.execution_backends.subprocess.run", side_effect=fake_run):
            rows = backend.raw_query("SELECT * FROM T", response_type="list")

        assert rows == []
        backend.cleanup()


class TestInitialization:
    def test_runs_pb_run_before_first_query(self):
        backend = PbQueryExecutionBackend("snowflake")
        backend._strategy = SnowflakePbQueryStrategy()
        backend._connection_name = "snowflake_conn"
        backend._siteconfig_path = backend._default_siteconfig_path()
        backend._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_test_")
        os.makedirs(os.path.join(backend._stub_project_path, "output"), exist_ok=True)
        backend._session = True

        def fake_run(cmd, **kwargs):
            if cmd[:2] == ["pb", "run"]:
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

            csv_name = cmd[cmd.index("-f") + 1]
            csv_path = os.path.join(backend._stub_project_path, "output", csv_name)
            with open(csv_path, "w") as handle:
                handle.write("COL_A\n1\n")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.execution_backends.subprocess.run", side_effect=fake_run) as mock_run:
            rows = backend.raw_query("SELECT 1")

        assert rows == [{"COL_A": 1}]
        first_call = mock_run.call_args_list[0].args[0]
        assert first_call[:2] == ["pb", "run"]
        assert backend._pb_initialized is True
        backend.cleanup()

    def test_pb_commands_use_stub_project_as_cwd(self):
        backend = PbQueryExecutionBackend("snowflake")
        backend._strategy = SnowflakePbQueryStrategy()
        backend._connection_name = "snowflake_conn"
        backend._siteconfig_path = backend._default_siteconfig_path()
        backend._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_test_")
        os.makedirs(os.path.join(backend._stub_project_path, "output"), exist_ok=True)
        backend._session = True

        def fake_run(cmd, **kwargs):
            if cmd[:2] == ["pb", "run"]:
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

            csv_name = cmd[cmd.index("-f") + 1]
            csv_path = os.path.join(backend._stub_project_path, "output", csv_name)
            with open(csv_path, "w") as handle:
                handle.write("COL_A\n1\n")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.execution_backends.subprocess.run", side_effect=fake_run) as mock_run:
            backend.raw_query("SELECT 1")

        pb_calls = [c for c in mock_run.call_args_list if c.args[0][0] == "pb"]
        assert pb_calls
        assert all(c.kwargs.get("cwd") == backend._stub_project_path for c in pb_calls)
        backend.cleanup()


class TestHelperMethods:
    def test_describe_table_returns_fallback_on_empty_rows(self):
        backend = _make_backend()

        with patch.object(backend, "raw_query", return_value=[]):
            result = backend.describe_table("DB", "SCHEMA", "TABLE")

        assert result == ["Failed to describe table: empty schema metadata returned"]
        backend.cleanup()

    def test_input_table_suggestions_returns_matches(self):
        backend = _make_backend()

        def fake_raw_query(query, response_type="list"):
            if query.startswith("SHOW TABLES IN"):
                return [{"name": "EVENTS_TRACKS"}, {"name": "EVENTS_PAGES"}]
            if query.startswith("SELECT event"):
                return [{"event": "pages"}]
            return []

        with patch.object(backend, "raw_query", side_effect=fake_raw_query):
            suggestions = backend.input_table_suggestions("DB", "PUBLIC")

        assert "DB.PUBLIC.EVENTS_TRACKS" in suggestions
        assert "DB.PUBLIC.EVENTS_PAGES" in suggestions
        backend.cleanup()

    def test_input_table_suggestions_handles_top_events_failure(self):
        backend = _make_backend()

        def fake_raw_query(query, response_type="list"):
            if query.startswith("SHOW TABLES IN"):
                return [{"name": "EVENTS_TRACKS"}, {"name": "EVENTS_PAGES"}]
            if query.startswith("SELECT event"):
                raise RuntimeError("top events query failed")
            return []

        with patch.object(backend, "raw_query", side_effect=fake_raw_query):
            suggestions = backend.input_table_suggestions("DB", "PUBLIC")

        # Should still return matches from default table matching even though
        # the top-events query failed.
        assert "DB.PUBLIC.EVENTS_TRACKS" in suggestions
        assert "DB.PUBLIC.EVENTS_PAGES" in suggestions
        backend.cleanup()

    def test_input_table_suggestions_handles_list_tables_failure(self):
        backend = _make_backend()

        with patch.object(backend, "raw_query", side_effect=RuntimeError("connection lost")):
            suggestions = backend.input_table_suggestions("DB", "PUBLIC")

        assert suggestions == []
        backend.cleanup()


class TestBuildStrategy:
    def test_raises_for_unsupported_warehouse_type(self):
        backend = PbQueryExecutionBackend("unsupported_wh")
        with pytest.raises(ValueError, match="Unsupported warehouse type"):
            backend._build_strategy({})


@pytest.mark.parametrize(
    "warehouse_type",
    ["snowflake", "bigquery", "databricks", "redshift"],
)
class TestInitializeConnectionAllWarehouses:
    def test_initialize_connection_runs_full_flow(self, warehouse_type):
        """Verify initialize_connection: stub project setup, pb run, SELECT 1 health check."""
        version_result = subprocess.CompletedProcess(
            args=["pb", "version"],
            returncode=0,
            stdout="Native schema version: 91\n",
            stderr="",
        )

        stub_project_path = [None]

        def fake_run(cmd, **kwargs):
            if cmd[:2] == ["pb", "version"]:
                return version_result
            if cmd[:2] == ["pb", "run"]:
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
            if cmd[:2] == ["pb", "query"]:
                # Capture stub project path from the -p argument
                p_idx = cmd.index("-p") + 1
                stub_project_path[0] = cmd[p_idx]
                csv_name = cmd[cmd.index("-f") + 1]
                csv_path = os.path.join(cmd[p_idx], "output", csv_name)
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                with open(csv_path, "w") as handle:
                    handle.write("result\n1\n")
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        connection_details = {
            "type": warehouse_type,
            "connection_name": f"{warehouse_type}_conn",
        }
        if warehouse_type == "databricks":
            connection_details["catalog"] = "main"

        with patch("tools.execution_backends.subprocess.run", side_effect=fake_run) as mock_run:
            backend = PbQueryExecutionBackend(warehouse_type)
            backend.initialize_connection(connection_details)

        # Verify pb run was called (bootstrap)
        run_calls = [c for c in mock_run.call_args_list if c.args[0][:2] == ["pb", "run"]]
        assert len(run_calls) == 1

        # Verify pb query was called (SELECT 1 health check)
        query_calls = [c for c in mock_run.call_args_list if c.args[0][:2] == ["pb", "query"]]
        assert len(query_calls) == 1

        # Verify stub project was created with pb_project.yaml
        assert stub_project_path[0] is not None
        assert os.path.exists(os.path.join(stub_project_path[0], "pb_project.yaml"))

        assert backend._pb_initialized is True
        assert backend._strategy is not None
        backend.cleanup()
