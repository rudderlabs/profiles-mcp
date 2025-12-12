import pytest
from unittest.mock import MagicMock, patch
from tools.snowflake import Snowflake
from tools.bigquery import BigQuery
from tools.databricks import Databricks
from tools.redshift import Redshift


# --- Snowflake Tests ---
@patch("tools.snowflake.Session")
def test_snowflake_init(mock_session_cls, mock_snowflake_details):
    wh = Snowflake()
    wh.initialize_connection(mock_snowflake_details)

    assert wh.connection_details.user == "test_user"
    assert wh.connection_details.warehouse_type == "snowflake"
    # Verify session builder was called
    mock_session_cls.builder.configs.assert_called_once()
    mock_session_cls.builder.configs.return_value.create.assert_called_once()


# --- BigQuery Tests ---
@patch("tools.bigquery.bigquery.Client")
@patch("tools.bigquery.default")
def test_bigquery_init_adc(mock_default, mock_client_cls, mock_bigquery_details):
    # Setup for ADC path
    mock_default.return_value = (MagicMock(), "default_project")

    # Remove credentials to force ADC
    details = mock_bigquery_details.copy()
    del details["credentials"]

    wh = BigQuery()
    wh.initialize_connection(details)

    mock_default.assert_called_once()
    mock_client_cls.assert_called_once()


@patch("tools.bigquery.bigquery.Client")
@patch("tools.bigquery.service_account.Credentials")
def test_bigquery_init_service_account(
    mock_creds_cls, mock_client_cls, mock_bigquery_details
):
    wh = BigQuery()
    wh.initialize_connection(mock_bigquery_details)

    mock_creds_cls.from_service_account_info.assert_called_once()
    mock_client_cls.assert_called_once()


# --- Databricks Tests ---
@patch("tools.databricks.sql.connect")
def test_databricks_init_pat(mock_connect):
    details = {
        "type": "databricks",
        "host": "test-host",
        "http_endpoint": "test-path",
        "access_token": "test-token",
    }

    wh = Databricks()
    wh.initialize_connection(details)

    mock_connect.assert_called_once()
    _, kwargs = mock_connect.call_args
    assert kwargs["server_hostname"] == "test-host"
    assert kwargs["http_path"] == "test-path"
    assert kwargs["access_token"] == "test-token"


# --- Redshift Tests ---
@patch("tools.redshift.redshift_connector.connect")
def test_redshift_init_password(mock_connect):
    details = {
        "type": "redshift",
        "host": "test-host",
        "port": 5439,
        "database": "test-db",
        "user": "test-user",
        "password": "test-password",
    }

    wh = Redshift()
    wh.initialize_connection(details)

    mock_connect.assert_called_once()
    _, kwargs = mock_connect.call_args
    assert kwargs["host"] == "test-host"
    assert kwargs["user"] == "test-user"
