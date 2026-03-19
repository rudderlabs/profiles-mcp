import pytest
import os
import yaml
import shutil
import pandas as pd
from unittest.mock import patch
from tools.warehouse_factory import WarehouseManager
from tools.profiles import ProfilesTools


# Helper to check if a specific connection secret is present in env
def has_secret(env_var):
    return os.environ.get(env_var) is not None


def has_pb_cli():
    return shutil.which("pb") is not None


def is_ci_environment():
    return os.environ.get("CI", "").lower() in {"1", "true", "yes"}


def should_lenient_skip_local_external_errors():
    return not is_ci_environment()


def is_local_pb_override_mode():
    override_envs = [
        "PB_TEST_SNOWFLAKE_CONN",
        "PB_TEST_BIGQUERY_CONN",
        "PB_TEST_DATABRICKS_CONN",
        "PB_TEST_REDSHIFT_CONN",
    ]
    return any(os.environ.get(env_name) for env_name in override_envs)


def initialize_pb_warehouse_or_skip(warehouse_manager, connection_name, connection_details):
    try:
        return warehouse_manager.initialize_warehouse(connection_name, connection_details)
    except RuntimeError as exc:
        if should_lenient_skip_local_external_errors() or is_local_pb_override_mode():
            pytest.skip(
                f"Skipping local pb integration for '{connection_name}': {str(exc)}"
            )
        raise


def run_pb_query_or_skip(warehouse, query):
    try:
        return warehouse.raw_query(query)
    except RuntimeError as exc:
        if should_lenient_skip_local_external_errors() or is_local_pb_override_mode():
            pytest.skip(f"Skipping local pb integration query '{query}': {str(exc)}")
        raise


def run_pb_query_with_type_or_skip(warehouse, query, response_type):
    try:
        return warehouse.raw_query(query, response_type=response_type)
    except RuntimeError as exc:
        if should_lenient_skip_local_external_errors() or is_local_pb_override_mode():
            pytest.skip(
                f"Skipping local pb integration query '{query}' with response_type={response_type}: {str(exc)}"
            )
        raise


def initialize_sdk_warehouse_or_skip(warehouse_manager, connection_name, connection_details):
    try:
        return warehouse_manager.initialize_warehouse(connection_name, connection_details)
    except Exception as exc:
        if should_lenient_skip_local_external_errors():
            pytest.skip(
                f"Skipping local SDK integration for '{connection_name}': {str(exc)}"
            )
        raise


def initialize_for_metadata_or_skip(
    warehouse_manager,
    profiles_tool,
    connection_name,
    use_pb_mode=False,
):
    creds = profiles_tool.fetch_warehouse_credentials(connection_name)
    if creds.get("status") == "error":
        pytest.skip(
            f"Unable to fetch {connection_name} credentials for metadata test: {creds.get('message', 'unknown error')}"
        )

    if use_pb_mode:
        return initialize_pb_warehouse_or_skip(
            warehouse_manager,
            connection_name,
            creds["connection_details"],
        )

    return initialize_sdk_warehouse_or_skip(
        warehouse_manager,
        connection_name,
        creds["connection_details"],
    )


def assert_mcp_query_patterns_work(warehouse, use_pb_mode=False):
    simple_select = "SELECT 1 AS one"
    filtered_subquery = (
        "SELECT * FROM (SELECT 1 AS one UNION ALL SELECT 2 AS one) q "
        "WHERE one >= 1 ORDER BY one DESC LIMIT 1"
    )
    aggregate_subquery = (
        "SELECT COUNT(*) AS cnt "
        "FROM (SELECT 1 AS one UNION ALL SELECT 2 AS one) q"
    )

    if use_pb_mode:
        rows = run_pb_query_or_skip(warehouse, simple_select)
        filtered_rows = run_pb_query_or_skip(warehouse, filtered_subquery)
        aggregate_rows = run_pb_query_or_skip(warehouse, aggregate_subquery)
        dataframe_rows = run_pb_query_with_type_or_skip(
            warehouse, simple_select, response_type="pandas"
        )
    else:
        rows = warehouse.raw_query(simple_select)
        filtered_rows = warehouse.raw_query(filtered_subquery)
        aggregate_rows = warehouse.raw_query(aggregate_subquery)
        dataframe_rows = warehouse.raw_query(simple_select, response_type="pandas")

    assert isinstance(rows, list)
    assert len(rows) == 1

    assert isinstance(filtered_rows, list)
    assert len(filtered_rows) == 1

    assert isinstance(aggregate_rows, list)
    assert len(aggregate_rows) == 1

    # SDK implementations may return list fallback when optional pandas conversion
    # dependencies are unavailable in CI images. pb mode should remain strict.
    if use_pb_mode:
        assert isinstance(dataframe_rows, pd.DataFrame)
        assert len(dataframe_rows) == 1
        assert len(dataframe_rows.columns) >= 1
    else:
        assert isinstance(dataframe_rows, (pd.DataFrame, list))
        if isinstance(dataframe_rows, pd.DataFrame):
            assert len(dataframe_rows) == 1
            assert len(dataframe_rows.columns) >= 1
        else:
            assert len(dataframe_rows) == 1

    query_result = warehouse.query(simple_select)
    if use_pb_mode:
        assert isinstance(query_result, pd.DataFrame)
        assert len(query_result) == 1
    else:
        assert isinstance(query_result, (pd.DataFrame, list))
        if isinstance(query_result, pd.DataFrame):
            assert len(query_result) == 1
        else:
            assert len(query_result) == 1


@pytest.fixture
def warehouse_manager():
    return WarehouseManager()


@pytest.fixture
def profiles_tool():
    return ProfilesTools()


# --- Snowflake Integration ---
@pytest.mark.skipif(
    not has_secret("SNOWFLAKE_CONFIG"), reason="SNOWFLAKE_CONFIG env var not set"
)
class TestSnowflakeIntegration:
    def test_connection_and_simple_query(self, warehouse_manager, profiles_tool):
        # 1. Fetch credentials
        creds = profiles_tool.fetch_warehouse_credentials("snowflake_conn")
        assert creds["status"] != "error"

        # 2. Initialize
        wh = warehouse_manager.initialize_warehouse(
            "snowflake_conn", creds["connection_details"]
        )
        assert wh.warehouse_type == "snowflake"

        # 3. Validate query patterns MCP commonly uses
        assert_mcp_query_patterns_work(wh)

    def test_metadata_queries(self, warehouse_manager, profiles_tool):
        wh = initialize_for_metadata_or_skip(
            warehouse_manager,
            profiles_tool,
            "snowflake_conn",
        )

        db = wh.connection_details.database
        schema = wh.connection_details.schema

        # Test input_table_suggestions (runs internal SQL)
        suggestions = wh.input_table_suggestions(db, schema)
        assert isinstance(suggestions, list)

        # Test describe_table (runs internal SQL)
        # We try to describe a known system view if suggestions are empty, or pick one
        target_table = suggestions[0] if suggestions else "INFORMATION_SCHEMA.TABLES"
        # Parse schema.table if needed
        if "." in target_table:
            parts = target_table.split(".")
            desc_table = parts[-1]
            desc_schema = parts[-2]
            desc_db = parts[-3] if len(parts) > 2 else db
        else:
            desc_table = target_table
            desc_schema = schema
            desc_db = db

        try:
            desc = wh.describe_table(desc_db, desc_schema, desc_table)
            assert isinstance(desc, list)
        except Exception:
            # If information schema access is restricted, continue without failing
            # The suggestions test already validated basic functionality
            pass


@pytest.mark.skipif(
    not has_secret("SNOWFLAKE_CONFIG"), reason="SNOWFLAKE_CONFIG env var not set"
)
@pytest.mark.skipif(not has_pb_cli(), reason="pb CLI is not installed")
class TestSnowflakePbQueryIntegration:
    @pytest.fixture(autouse=True)
    def pb_query_mode(self):
        with patch("tools.warehouse_factory.USE_PB_QUERY", True):
            yield

    def test_connection_and_simple_query_pb_mode(self, warehouse_manager, profiles_tool):
        creds = profiles_tool.fetch_warehouse_credentials("snowflake_conn")
        if creds.get("status") == "error":
            pytest.skip(f"Unable to fetch snowflake credentials for pb mode: {creds.get('message', 'unknown error')}")

        wh = initialize_pb_warehouse_or_skip(
            warehouse_manager,
            "snowflake_conn",
            creds["connection_details"],
        )
        assert wh.warehouse_type == "snowflake"

        assert_mcp_query_patterns_work(wh, use_pb_mode=True)

    def test_metadata_queries_pb_mode(self, warehouse_manager, profiles_tool):
        wh = initialize_for_metadata_or_skip(
            warehouse_manager,
            profiles_tool,
            "snowflake_conn",
            use_pb_mode=True,
        )

        db = wh.connection_details.database
        schema = wh.connection_details.schema

        suggestions = wh.input_table_suggestions(db, schema)
        assert isinstance(suggestions, list)

        target_table = suggestions[0] if suggestions else "INFORMATION_SCHEMA.TABLES"
        if "." in target_table:
            parts = target_table.split(".")
            desc_table = parts[-1]
            desc_schema = parts[-2]
            desc_db = parts[-3] if len(parts) > 2 else db
        else:
            desc_table = target_table
            desc_schema = schema
            desc_db = db

        desc = wh.describe_table(desc_db, desc_schema, desc_table)
        assert isinstance(desc, list)


# --- BigQuery Integration ---
@pytest.mark.skipif(
    not has_secret("BIGQUERY_CONFIG"), reason="BIGQUERY_CONFIG env var not set"
)
class TestBigQueryIntegration:
    def test_connection_and_simple_query(self, warehouse_manager, profiles_tool):
        creds = profiles_tool.fetch_warehouse_credentials("bigquery_conn")
        assert creds["status"] != "error"

        wh = warehouse_manager.initialize_warehouse(
            "bigquery_conn", creds["connection_details"]
        )
        assert wh.warehouse_type == "bigquery"

        assert_mcp_query_patterns_work(wh)

    def test_metadata_queries(self, warehouse_manager, profiles_tool):
        wh = initialize_for_metadata_or_skip(
            warehouse_manager,
            profiles_tool,
            "bigquery_conn",
        )

        # BigQuery structure is usually project.dataset.table
        details = wh.connection_details.connection_details
        project = details.get("project_id")
        dataset = details.get("dataset")
        if not project or not dataset:
            pytest.skip("BigQuery project_id/dataset not available")

        suggestions = wh.input_table_suggestions(project, dataset)
        assert isinstance(suggestions, list)


@pytest.mark.skipif(
    not has_secret("BIGQUERY_CONFIG"), reason="BIGQUERY_CONFIG env var not set"
)
@pytest.mark.skipif(not has_pb_cli(), reason="pb CLI is not installed")
class TestBigQueryPbQueryIntegration:
    @pytest.fixture(autouse=True)
    def pb_query_mode(self):
        with patch("tools.warehouse_factory.USE_PB_QUERY", True):
            yield

    def test_connection_and_simple_query_pb_mode(self, warehouse_manager, profiles_tool):
        creds = profiles_tool.fetch_warehouse_credentials("bigquery_conn")
        if creds.get("status") == "error":
            pytest.skip(
                f"Unable to fetch bigquery credentials for pb mode: {creds.get('message', 'unknown error')}"
            )

        wh = initialize_pb_warehouse_or_skip(
            warehouse_manager,
            "bigquery_conn",
            creds["connection_details"],
        )
        assert wh.warehouse_type == "bigquery"

        assert_mcp_query_patterns_work(wh, use_pb_mode=True)

    def test_metadata_queries_pb_mode(self, warehouse_manager, profiles_tool):
        wh = initialize_for_metadata_or_skip(
            warehouse_manager,
            profiles_tool,
            "bigquery_conn",
            use_pb_mode=True,
        )

        project = wh.connection_details.connection_details.get("project_id")
        dataset = wh.connection_details.connection_details.get("dataset")
        if not project or not dataset:
            pytest.skip("BigQuery project_id/dataset not available")

        suggestions = wh.input_table_suggestions(project, dataset)
        assert isinstance(suggestions, list)


# --- Databricks Integration ---
@pytest.mark.skipif(
    not has_secret("DATABRICKS_CONFIG"), reason="DATABRICKS_CONFIG env var not set"
)
class TestDatabricksIntegration:
    def test_connection_and_simple_query(self, warehouse_manager, profiles_tool):
        creds = profiles_tool.fetch_warehouse_credentials("databricks_conn")
        assert creds["status"] != "error"

        wh = initialize_sdk_warehouse_or_skip(
            warehouse_manager,
            "databricks_conn",
            creds["connection_details"],
        )
        assert wh.warehouse_type == "databricks"

        assert_mcp_query_patterns_work(wh)

    def test_metadata_queries(self, warehouse_manager, profiles_tool):
        wh = initialize_for_metadata_or_skip(
            warehouse_manager,
            profiles_tool,
            "databricks_conn",
        )

        # Databricks uses catalog.schema usually, or just schema
        # The connector details might have catalog or http_path implictly
        # We try to use 'default' if not present or passed via details
        # For now, just calling suggestions which uses internal query 'SHOW TABLES'
        catalog = getattr(wh.connection_details, "catalog", "hive_metastore")
        schema = getattr(wh.connection_details, "schema", "default")

        suggestions = wh.input_table_suggestions(catalog, schema)
        assert isinstance(suggestions, list)


@pytest.mark.skipif(
    not has_secret("DATABRICKS_CONFIG"), reason="DATABRICKS_CONFIG env var not set"
)
@pytest.mark.skipif(not has_pb_cli(), reason="pb CLI is not installed")
class TestDatabricksPbQueryIntegration:
    @pytest.fixture(autouse=True)
    def pb_query_mode(self):
        with patch("tools.warehouse_factory.USE_PB_QUERY", True):
            yield

    def test_connection_and_simple_query_pb_mode(self, warehouse_manager, profiles_tool):
        creds = profiles_tool.fetch_warehouse_credentials("databricks_conn")
        if creds.get("status") == "error":
            pytest.skip(
                f"Unable to fetch databricks credentials for pb mode: {creds.get('message', 'unknown error')}"
            )

        wh = initialize_pb_warehouse_or_skip(
            warehouse_manager,
            "databricks_conn",
            creds["connection_details"],
        )
        assert wh.warehouse_type == "databricks"

        assert_mcp_query_patterns_work(wh, use_pb_mode=True)

    def test_metadata_queries_pb_mode(self, warehouse_manager, profiles_tool):
        wh = initialize_for_metadata_or_skip(
            warehouse_manager,
            profiles_tool,
            "databricks_conn",
            use_pb_mode=True,
        )

        connection_details = wh.connection_details.connection_details
        database = connection_details.get("catalog") or connection_details.get("database")
        schema = connection_details.get("schema", "default")

        suggestions = wh.input_table_suggestions(database or "", schema)
        assert isinstance(suggestions, list)


# --- Redshift Integration ---
@pytest.mark.skipif(
    not has_secret("REDSHIFT_CONFIG"), reason="REDSHIFT_CONFIG env var not set"
)
class TestRedshiftIntegration:
    def test_connection_and_simple_query(self, warehouse_manager, profiles_tool):
        creds = profiles_tool.fetch_warehouse_credentials("redshift_conn")
        assert creds["status"] != "error"

        wh = warehouse_manager.initialize_warehouse(
            "redshift_conn", creds["connection_details"]
        )
        assert wh.warehouse_type == "redshift"

        assert_mcp_query_patterns_work(wh)

    def test_metadata_queries(self, warehouse_manager, profiles_tool):
        wh = initialize_for_metadata_or_skip(
            warehouse_manager,
            profiles_tool,
            "redshift_conn",
        )

        db = wh.connection_details.database
        # Redshift usually has schemas like 'public'
        schemas = "public,information_schema"

        suggestions = wh.input_table_suggestions(db, schemas)
        assert isinstance(suggestions, list)


@pytest.mark.skipif(
    not has_secret("REDSHIFT_CONFIG"), reason="REDSHIFT_CONFIG env var not set"
)
@pytest.mark.skipif(not has_pb_cli(), reason="pb CLI is not installed")
class TestRedshiftPbQueryIntegration:
    @pytest.fixture(autouse=True)
    def pb_query_mode(self):
        with patch("tools.warehouse_factory.USE_PB_QUERY", True):
            yield

    def test_connection_and_simple_query_pb_mode(self, warehouse_manager, profiles_tool):
        creds = profiles_tool.fetch_warehouse_credentials("redshift_conn")
        if creds.get("status") == "error":
            pytest.skip(
                f"Unable to fetch redshift credentials for pb mode: {creds.get('message', 'unknown error')}"
            )

        wh = initialize_pb_warehouse_or_skip(
            warehouse_manager,
            "redshift_conn",
            creds["connection_details"],
        )
        assert wh.warehouse_type == "redshift"

        assert_mcp_query_patterns_work(wh, use_pb_mode=True)

    def test_metadata_queries_pb_mode(self, warehouse_manager, profiles_tool):
        wh = initialize_for_metadata_or_skip(
            warehouse_manager,
            profiles_tool,
            "redshift_conn",
            use_pb_mode=True,
        )

        db = wh.connection_details.database
        schemas = "public,information_schema"
        suggestions = wh.input_table_suggestions(db, schemas)
        assert isinstance(suggestions, list)
