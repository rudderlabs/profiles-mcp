import os
import subprocess
import tempfile

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, mock_open

from tools.pb_query_warehouse import PbQueryWarehouse


@pytest.fixture(autouse=True)
def clear_schema_version_cache():
    """Reset the class-level schema version cache before each test."""
    PbQueryWarehouse._schema_version_cache = None
    yield
    PbQueryWarehouse._schema_version_cache = None


@pytest.fixture
def pb_version_output():
    return "Profiles v0.15.2\nNative schema version: 89\n"


@pytest.fixture
def connection_details():
    return {
        "connection_name": "test_conn",
        "type": "snowflake",
        "account": "test_account",
        "user": "test_user",
    }


def _make_csv(content: str, stub_path: str):
    """Write a CSV file in the stub project's output dir and return the path."""
    output_dir = os.path.join(stub_path, "output")
    os.makedirs(output_dir, exist_ok=True)
    # We'll discover the actual filename from the command args
    return content


class TestGetSchemaVersion:
    def test_parses_version_from_stdout(self, pb_version_output):
        result = subprocess.CompletedProcess(
            args=["pb", "version"],
            returncode=0,
            stdout=pb_version_output,
            stderr="",
        )
        with patch("tools.pb_query_warehouse.subprocess.run", return_value=result):
            version = PbQueryWarehouse._get_schema_version()
            assert version == 89

    def test_parses_version_from_stderr(self):
        result = subprocess.CompletedProcess(
            args=["pb", "version"],
            returncode=0,
            stdout="",
            stderr="Native schema version: 42\n",
        )
        with patch("tools.pb_query_warehouse.subprocess.run", return_value=result):
            version = PbQueryWarehouse._get_schema_version()
            assert version == 42

    def test_caches_result(self, pb_version_output):
        result = subprocess.CompletedProcess(
            args=["pb", "version"],
            returncode=0,
            stdout=pb_version_output,
            stderr="",
        )
        with patch("tools.pb_query_warehouse.subprocess.run", return_value=result) as mock_run:
            v1 = PbQueryWarehouse._get_schema_version()
            v2 = PbQueryWarehouse._get_schema_version()
            assert v1 == v2 == 89
            mock_run.assert_called_once()

    def test_raises_on_unparseable_output(self):
        result = subprocess.CompletedProcess(
            args=["pb", "version"],
            returncode=0,
            stdout="Some other output",
            stderr="",
        )
        with patch("tools.pb_query_warehouse.subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="Could not parse schema version"):
                PbQueryWarehouse._get_schema_version()

    def test_raises_on_pb_not_found(self):
        with patch(
            "tools.pb_query_warehouse.subprocess.run",
            side_effect=FileNotFoundError(),
        ):
            with pytest.raises(RuntimeError, match="pb CLI not found"):
                PbQueryWarehouse._get_schema_version()

    def test_raises_on_timeout(self):
        with patch(
            "tools.pb_query_warehouse.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="pb version", timeout=30),
        ):
            with pytest.raises(RuntimeError, match="timed out"):
                PbQueryWarehouse._get_schema_version()


class TestInitializeConnection:
    @patch("tools.pb_query_warehouse.PbQueryWarehouse.raw_query")
    @patch("tools.pb_query_warehouse.PbQueryWarehouse._get_schema_version", return_value=89)
    def test_creates_stub_project(self, mock_version, mock_raw_query, connection_details):
        mock_raw_query.return_value = [{"1": 1}]

        wh = PbQueryWarehouse()
        wh.initialize_connection(connection_details)

        assert wh._connection_name == "test_conn"
        assert wh._stub_project_path is not None
        assert os.path.isdir(wh._stub_project_path)
        assert os.path.isdir(os.path.join(wh._stub_project_path, "models"))
        assert os.path.isdir(os.path.join(wh._stub_project_path, "output"))

        # Check pb_project.yaml content
        import yaml
        with open(os.path.join(wh._stub_project_path, "pb_project.yaml")) as f:
            project = yaml.safe_load(f)
        assert project["name"] == "pb_mcp_stub"
        assert project["schema_version"] == 89
        assert project["connection"] == "test_conn"
        assert project["model_folders"] == ["models"]

        # Verify SELECT 1 was called for connectivity check
        mock_raw_query.assert_called_once_with("SELECT 1")

        # Cleanup
        wh.cleanup()

    @patch("tools.pb_query_warehouse.PbQueryWarehouse.raw_query")
    @patch("tools.pb_query_warehouse.PbQueryWarehouse._get_schema_version", return_value=89)
    def test_cleans_up_on_connectivity_failure(self, mock_version, mock_raw_query, connection_details):
        mock_raw_query.side_effect = RuntimeError("connection failed")

        wh = PbQueryWarehouse()
        with pytest.raises(RuntimeError, match="Failed to validate pb query connectivity"):
            wh.initialize_connection(connection_details)

        # Stub dir should be cleaned up
        assert wh._stub_project_path is None

    def test_raises_without_connection_name(self):
        wh = PbQueryWarehouse()
        with pytest.raises(ValueError, match="connection_name"):
            wh.initialize_connection({"type": "snowflake"})


class TestRawQuery:
    def _setup_warehouse(self):
        """Create a PbQueryWarehouse with a real temp dir but skip init."""
        wh = PbQueryWarehouse()
        wh._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_test_")
        os.makedirs(os.path.join(wh._stub_project_path, "output"), exist_ok=True)
        wh._connection_name = "test_conn"
        wh._siteconfig_path = str(os.path.join(os.path.expanduser("~"), ".pb", "siteconfig.yaml"))
        return wh

    def test_raw_query_list(self):
        wh = self._setup_warehouse()
        csv_content = "NAME,TYPE\nalpha,VARCHAR\nbeta,INT\n"

        def fake_run(cmd, **kwargs):
            # Write CSV to the output dir based on -f arg
            f_idx = cmd.index("-f")
            csv_name = cmd[f_idx + 1]
            csv_path = os.path.join(wh._stub_project_path, "output", csv_name)
            with open(csv_path, "w") as f:
                f.write(csv_content)
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.pb_query_warehouse.subprocess.run", side_effect=fake_run):
            result = wh.raw_query("DESCRIBE TABLE db.schema.table", response_type="list")

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["NAME"] == "alpha"
        assert result[1]["TYPE"] == "INT"

        wh.cleanup()

    def test_raw_query_pandas(self):
        wh = self._setup_warehouse()
        csv_content = "COL_A,COL_B\nval1,<nil>\nval2,val3\n"

        def fake_run(cmd, **kwargs):
            f_idx = cmd.index("-f")
            csv_name = cmd[f_idx + 1]
            csv_path = os.path.join(wh._stub_project_path, "output", csv_name)
            with open(csv_path, "w") as f:
                f.write(csv_content)
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.pb_query_warehouse.subprocess.run", side_effect=fake_run):
            result = wh.raw_query("SELECT * FROM t", response_type="pandas")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        # <nil> should be converted to "Null" for object columns
        assert result.iloc[0]["COL_B"] == "Null"
        assert result.iloc[1]["COL_B"] == "val3"

        wh.cleanup()

    def test_raw_query_nil_handling(self):
        wh = self._setup_warehouse()
        csv_content = "ID,VALUE\n1,<nil>\n2,hello\n3,<nil>\n"

        def fake_run(cmd, **kwargs):
            f_idx = cmd.index("-f")
            csv_name = cmd[f_idx + 1]
            csv_path = os.path.join(wh._stub_project_path, "output", csv_name)
            with open(csv_path, "w") as f:
                f.write(csv_content)
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.pb_query_warehouse.subprocess.run", side_effect=fake_run):
            result = wh.raw_query("SELECT * FROM t", response_type="list")

        assert len(result) == 3
        # <nil> values should be NaN in the list (pandas converts to float NaN)
        assert pd.isna(result[0]["VALUE"])
        assert result[1]["VALUE"] == "hello"
        assert pd.isna(result[2]["VALUE"])

        wh.cleanup()

    def test_raw_query_error(self):
        wh = self._setup_warehouse()

        result = subprocess.CompletedProcess(
            args=["pb", "query"],
            returncode=1,
            stdout="",
            stderr="\x1B[31mError: connection refused\x1B[0m",
        )

        with patch("tools.pb_query_warehouse.subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="pb query failed"):
                wh.raw_query("SELECT 1")

        wh.cleanup()

    def test_raw_query_error_strips_ansi(self):
        wh = self._setup_warehouse()

        result = subprocess.CompletedProcess(
            args=["pb", "query"],
            returncode=1,
            stdout="",
            stderr="\x1B[31mError: bad query\x1B[0m",
        )

        with patch("tools.pb_query_warehouse.subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="Error: bad query"):
                wh.raw_query("SELECT 1")

        wh.cleanup()

    def test_raw_query_timeout(self):
        wh = self._setup_warehouse()

        with patch(
            "tools.pb_query_warehouse.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="pb query", timeout=540),
        ):
            with pytest.raises(RuntimeError, match="timed out"):
                wh.raw_query("SELECT 1")

        wh.cleanup()

    def test_raw_query_csv_cleanup(self):
        wh = self._setup_warehouse()
        csv_content = "A\n1\n"

        written_paths = []

        def fake_run(cmd, **kwargs):
            f_idx = cmd.index("-f")
            csv_name = cmd[f_idx + 1]
            csv_path = os.path.join(wh._stub_project_path, "output", csv_name)
            written_paths.append(csv_path)
            with open(csv_path, "w") as f:
                f.write(csv_content)
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.pb_query_warehouse.subprocess.run", side_effect=fake_run):
            wh.raw_query("SELECT 1")

        # CSV should be cleaned up after query
        assert len(written_paths) == 1
        assert not os.path.exists(written_paths[0])

        wh.cleanup()


class TestDescribeTable:
    def test_describe_table(self):
        wh = PbQueryWarehouse()
        wh._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_test_")
        os.makedirs(os.path.join(wh._stub_project_path, "output"), exist_ok=True)
        wh._connection_name = "test_conn"
        wh._siteconfig_path = str(os.path.join(os.path.expanduser("~"), ".pb", "siteconfig.yaml"))

        csv_content = "name,type\nid,NUMBER\nemail,VARCHAR\n"

        def fake_run(cmd, **kwargs):
            # Verify the SQL contains DESCRIBE TABLE
            assert "DESCRIBE TABLE mydb.myschema.mytable" in cmd
            f_idx = cmd.index("-f")
            csv_name = cmd[f_idx + 1]
            csv_path = os.path.join(wh._stub_project_path, "output", csv_name)
            with open(csv_path, "w") as f:
                f.write(csv_content)
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.pb_query_warehouse.subprocess.run", side_effect=fake_run):
            result = wh.describe_table("mydb", "myschema", "mytable")

        assert result == ["id: NUMBER", "email: VARCHAR"]
        wh.cleanup()

    def test_describe_table_uppercase_columns(self):
        wh = PbQueryWarehouse()
        wh._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_test_")
        os.makedirs(os.path.join(wh._stub_project_path, "output"), exist_ok=True)
        wh._connection_name = "test_conn"
        wh._siteconfig_path = str(os.path.join(os.path.expanduser("~"), ".pb", "siteconfig.yaml"))

        csv_content = "NAME,TYPE\nID,NUMBER\nEMAIL,VARCHAR\n"

        def fake_run(cmd, **kwargs):
            f_idx = cmd.index("-f")
            csv_name = cmd[f_idx + 1]
            csv_path = os.path.join(wh._stub_project_path, "output", csv_name)
            with open(csv_path, "w") as f:
                f.write(csv_content)
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.pb_query_warehouse.subprocess.run", side_effect=fake_run):
            result = wh.describe_table("mydb", "myschema", "mytable")

        assert result == ["ID: NUMBER", "EMAIL: VARCHAR"]
        wh.cleanup()

    def test_describe_table_validates_identifiers(self):
        wh = PbQueryWarehouse()
        wh._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_test_")
        wh._connection_name = "test_conn"

        result = wh.describe_table("mydb", "my schema; DROP TABLE", "mytable")
        assert len(result) == 1
        assert "Failed to describe table" in result[0]

        wh.cleanup()


class TestInputTableSuggestions:
    def test_input_table_suggestions(self):
        wh = PbQueryWarehouse()
        wh._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_test_")
        os.makedirs(os.path.join(wh._stub_project_path, "output"), exist_ok=True)
        wh._connection_name = "test_conn"
        wh._siteconfig_path = str(os.path.join(os.path.expanduser("~"), ".pb", "siteconfig.yaml"))

        call_count = [0]

        def fake_run(cmd, **kwargs):
            call_count[0] += 1
            query = cmd[2]  # pb query <SQL>
            f_idx = cmd.index("-f")
            csv_name = cmd[f_idx + 1]
            csv_path = os.path.join(wh._stub_project_path, "output", csv_name)

            if "SHOW TABLES" in query:
                with open(csv_path, "w") as f:
                    f.write("name\nRS_TRACKS\nRS_IDENTIFIES\nRS_PAGES\nORDERS\n")
            elif "SELECT event" in query.lower() or "select event" in query.lower():
                with open(csv_path, "w") as f:
                    f.write("EVENT,COUNT(*)\nproduct_viewed,100\npurchase,50\n")
            else:
                with open(csv_path, "w") as f:
                    f.write("result\nok\n")

            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        with patch("tools.pb_query_warehouse.subprocess.run", side_effect=fake_run):
            result = wh.input_table_suggestions("MYDB", "MYSCHEMA")

        # Should find tracks, identifies, pages via substring match
        assert "MYDB.MYSCHEMA.RS_TRACKS" in result
        assert "MYDB.MYSCHEMA.RS_IDENTIFIES" in result
        assert "MYDB.MYSCHEMA.RS_PAGES" in result
        # ORDERS doesn't match default tables
        assert "MYDB.MYSCHEMA.ORDERS" not in result

        wh.cleanup()


class TestCleanup:
    def test_cleanup_removes_temp_dir(self):
        wh = PbQueryWarehouse()
        wh._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_test_")
        temp_path = wh._stub_project_path
        assert os.path.isdir(temp_path)

        wh.cleanup()
        assert not os.path.exists(temp_path)
        assert wh._stub_project_path is None

    def test_cleanup_noop_when_no_path(self):
        wh = PbQueryWarehouse()
        wh._stub_project_path = None
        # Should not raise
        wh.cleanup()

    def test_cleanup_noop_when_path_already_removed(self):
        wh = PbQueryWarehouse()
        wh._stub_project_path = "/tmp/nonexistent_pb_mcp_test_dir"
        # Should not raise even if path doesn't exist
        wh.cleanup()


class TestSessionMethods:
    def test_create_session_returns_true(self):
        wh = PbQueryWarehouse()
        assert wh.create_session() is True

    def test_ensure_valid_session_is_noop(self):
        wh = PbQueryWarehouse()
        # Should not raise
        wh.ensure_valid_session()
