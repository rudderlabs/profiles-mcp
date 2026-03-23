import os
import pytest
import yaml
import tempfile
import warnings
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
    # Map of expected CI env vars to synthetic config keys used in integration tests.
    env_vars_map = {
        "SNOWFLAKE_CONFIG": "snowflake_conn",
        "BIGQUERY_CONFIG": "bigquery_conn",
        "DATABRICKS_CONFIG": "databricks_conn",
        "REDSHIFT_CONFIG": "redshift_conn",
    }

    # Optional local overrides: map real siteconfig connection names to synthetic keys.
    # Example:
    #   PB_TEST_SNOWFLAKE_CONN=test
    # will map ~/.pb/siteconfig.yaml connections.test to snowflake_conn for tests.
    local_override_map = {
        "PB_TEST_SNOWFLAKE_CONN": "snowflake_conn",
        "PB_TEST_BIGQUERY_CONN": "bigquery_conn",
        "PB_TEST_DATABRICKS_CONN": "databricks_conn",
        "PB_TEST_REDSHIFT_CONN": "redshift_conn",
    }

    connections = {}
    found_secrets = False

    # 1) Local override path: copy explicitly selected real connections from siteconfig.
    selected_local_connections = {
        env_var: os.environ.get(env_var)
        for env_var in local_override_map
        if os.environ.get(env_var)
    }
    if selected_local_connections:
        siteconfig_path = Path(
            os.environ.get("PB_TEST_SITECONFIG_PATH", str(Path.home() / ".pb" / "siteconfig.yaml"))
        )

        if not siteconfig_path.exists():
            warnings.warn(
                f"PB test siteconfig not found at {siteconfig_path}. "
                "Set PB_TEST_SITECONFIG_PATH or remove PB_TEST_*_CONN overrides."
            )
        else:
            try:
                with open(siteconfig_path, "r") as file:
                    local_config = yaml.safe_load(file) or {}
                local_connections = local_config.get("connections", {})

                for env_var, selected_conn_name in selected_local_connections.items():
                    synthetic_name = local_override_map[env_var]
                    existing_conn = local_connections.get(selected_conn_name)
                    if not existing_conn:
                        available = sorted(local_connections.keys())
                        warnings.warn(
                            f"{env_var}='{selected_conn_name}' not found in {siteconfig_path}. "
                            f"Available connections: {available}"
                        )
                        continue

                    target = existing_conn.get("target", "dev")
                    outputs = existing_conn.get("outputs", {})
                    if target not in outputs:
                        warnings.warn(
                            f"Connection '{selected_conn_name}' target '{target}' missing in outputs. "
                            f"Skipping mapping for {synthetic_name}."
                        )
                        continue

                    connections[synthetic_name] = {
                        "target": target,
                        "outputs": {target: outputs[target]},
                    }
                    found_secrets = True
            except yaml.YAMLError as e:
                warnings.warn(f"Failed to parse local PB siteconfig: {e}")

    # 2) CI/default path: build synthetic connections from *_CONFIG secrets.
    for env_var, conn_name in env_vars_map.items():
        config_str = os.environ.get(env_var)
        # Explicit local override should take precedence for that synthetic key.
        if config_str and conn_name not in connections:
            found_secrets = True
            try:
                # Parse the YAML string from secret
                parsed_config = yaml.safe_load(config_str)

                # Handle two possible formats:
                # 1. Full siteconfig format: connections → conn_name → target/outputs
                # 2. Simple format: just the innermost config with type, host, etc.

                if "connections" in parsed_config:
                    # Full format: extract the first connection
                    first_conn_name = list(parsed_config["connections"].keys())[0]
                    existing_conn = parsed_config["connections"][first_conn_name]

                    # Use the existing target/outputs structure
                    target = existing_conn.get("target", "dev")
                    output_config = existing_conn["outputs"][target]

                    # Re-map to our test connection name
                    connections[conn_name] = {
                        "target": target,
                        "outputs": {target: output_config},
                    }
                else:
                    # Simple format: wrap it with default target
                    connections[conn_name] = {
                        "target": "dev",
                        "outputs": {"dev": parsed_config},
                    }
            except yaml.YAMLError as e:
                warnings.warn(f"Failed to parse secret for {env_var}: {e}")
            except (KeyError, IndexError) as e:
                warnings.warn(f"Invalid structure in secret for {env_var}: {e}")

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
        # Patch the constant in the constants module AND where it is imported in tools.profiles
        with (
            patch("constants.PB_SITE_CONFIG_PATH", warehouse_config_setup),
            patch("tools.profiles.PB_SITE_CONFIG_PATH", warehouse_config_setup),
            patch("tools.warehouse_factory.PB_SITE_CONFIG_PATH", warehouse_config_setup),
        ):
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
