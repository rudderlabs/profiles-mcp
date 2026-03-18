import os
import subprocess
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest

from tools.execution_backends import PbQueryExecutionBackend, SnowflakePbQueryStrategy


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
