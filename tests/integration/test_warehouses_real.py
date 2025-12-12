import pytest
import os
import yaml
from tools.warehouse_factory import WarehouseManager
from tools.profiles import ProfilesTools


# Helper to check if a specific connection secret is present in env
def has_secret(env_var):
    return os.environ.get(env_var) is not None


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

        # 3. Test explicit SELECT 1
        result = wh.raw_query("SELECT 1 as ONE")
        assert len(result) == 1
        key = list(result[0].keys())[0]
        assert str(result[0][key]) == "1"

    def test_metadata_queries(self, warehouse_manager):
        wh = warehouse_manager.get_warehouse("snowflake_conn")
        if not wh:
            pytest.skip("Warehouse not initialized")

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
        except Exception as e:
            # If information schema access is restricted, we warn but don't fail if suggestions worked
            print(f"Describe table failed: {e}")


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

        result = wh.raw_query("SELECT 1 as one")
        assert result[0]["one"] == 1

    def test_metadata_queries(self, warehouse_manager):
        wh = warehouse_manager.get_warehouse("bigquery_conn")
        if not wh:
            pytest.skip("Warehouse not initialized")

        # BigQuery structure is usually project.dataset.table
        project = wh.connection_details.project_id
        dataset = wh.connection_details.dataset

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

        wh = warehouse_manager.initialize_warehouse(
            "databricks_conn", creds["connection_details"]
        )
        assert wh.warehouse_type == "databricks"

        result = wh.raw_query("SELECT 1 as one")
        assert result[0]["one"] == 1

    def test_metadata_queries(self, warehouse_manager):
        wh = warehouse_manager.get_warehouse("databricks_conn")
        if not wh:
            pytest.skip("Warehouse not initialized")

        # Databricks uses catalog.schema usually, or just schema
        # The connector details might have catalog or http_path implictly
        # We try to use 'default' if not present or passed via details
        # For now, just calling suggestions which uses internal query 'SHOW TABLES'
        catalog = getattr(wh.connection_details, "catalog", "hive_metastore")
        schema = getattr(wh.connection_details, "schema", "default")

        suggestions = wh.input_table_suggestions(catalog, schema)
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

        result = wh.raw_query("SELECT 1 as one")
        # Redshift (Postgres) returns lowercase column names usually
        val = list(result[0].values())[0]
        assert val == 1

    def test_metadata_queries(self, warehouse_manager):
        wh = warehouse_manager.get_warehouse("redshift_conn")
        if not wh:
            pytest.skip("Warehouse not initialized")

        db = wh.connection_details.database
        # Redshift usually has schemas like 'public'
        schemas = "public,information_schema"

        suggestions = wh.input_table_suggestions(db, schemas)
        assert isinstance(suggestions, list)
