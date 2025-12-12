import os
import pytest
import yaml
import tempfile
from pathlib import Path
from unittest.mock import patch


@pytest.fixture(scope="session")
def warehouse_config_setup():
    """
    Scope: Session
    Checks environment variables for warehouse configurations (YAML strings).
    If found, writes them to a temporary siteconfig.yaml file.
    Returns the path to the temporary config file.
    """
    # Map of expected env vars to their config keys
    env_vars_map = {
        "SNOWFLAKE_CONFIG": "snowflake_conn",
        "BIGQUERY_CONFIG": "bigquery_conn",
        "DATABRICKS_CONFIG": "databricks_conn",
        "REDSHIFT_CONFIG": "redshift_conn",
    }

    connections = {}
    found_secrets = False

    for env_var, conn_name in env_vars_map.items():
        config_str = os.environ.get(env_var)
        if config_str:
            found_secrets = True
            try:
                # Parse the YAML string from secret
                conn_config = yaml.safe_load(config_str)

                # Construct the full siteconfig structure for this connection
                # We default target to 'dev'
                connections[conn_name] = {
                    "target": "dev",
                    "outputs": {"dev": conn_config},
                }
            except yaml.YAMLError as e:
                print(f"Warning: Failed to parse secret for {env_var}: {e}")

    # If we are in an integration test environment (implied by presence of secrets),
    # we create a temp siteconfig.yaml
    if found_secrets:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
            full_config = {"connections": connections}
            yaml.dump(full_config, tmp)
            tmp_path = Path(tmp.name)

        yield tmp_path

        # Cleanup
        try:
            os.remove(tmp_path)
        except OSError:
            pass
    else:
        yield None


@pytest.fixture(autouse=True)
def patch_site_config(warehouse_config_setup):
    """
    Automatically patches constants.PB_SITE_CONFIG_PATH for ALL tests.
    If we created a temp config from secrets, usage of that path will work.
    If not, it will point to None (or we can just let it identify as missing).
    """
    if warehouse_config_setup:
        # Patch the constant in the constants module
        with patch("constants.PB_SITE_CONFIG_PATH", warehouse_config_setup):
            yield
    else:
        yield


@pytest.fixture
def mock_snowflake_details():
    return {
        "type": "snowflake",
        "account": "test_account",
        "user": "test_user",
        "password": "test_password",
        "warehouse": "test_wh",
        "database": "test_db",
        "schema": "test_schema",
        "role": "test_role",
    }


@pytest.fixture
def mock_bigquery_details():
    return {
        "type": "bigquery",
        "project_id": "test_project",
        "dataset": "test_dataset",
        "credentials": {"type": "service_account", "project_id": "test_project"},
    }
