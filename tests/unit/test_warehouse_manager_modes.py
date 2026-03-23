from unittest.mock import MagicMock, patch

from tools.warehouse_factory import WarehouseManager


def test_initialize_warehouse_uses_sdk_mode_by_default():
    manager = WarehouseManager()
    sdk_warehouse = MagicMock()
    sdk_warehouse.connection_details = MagicMock(warehouse_type="snowflake")
    sdk_warehouse.session = MagicMock()

    with (
        patch("tools.warehouse_factory.USE_PB_QUERY", False),
        patch(
            "tools.warehouse_factory.WarehouseFactory.create_warehouse",
            return_value=sdk_warehouse,
        ) as create_warehouse,
    ):
        wh = manager.initialize_warehouse(
            "snowflake_conn",
            {
                "type": "snowflake",
                "user": "test_user",
                "password": "test_password",
            },
        )

    create_warehouse.assert_called_once_with("snowflake")
    assert wh.warehouse_type == "snowflake"


def test_initialize_warehouse_pb_mode_sets_connection_name():
    manager = WarehouseManager()
    pb_backend = MagicMock()
    pb_backend.connection_details = MagicMock(warehouse_type="snowflake")
    pb_backend.session = None

    with (
        patch("tools.warehouse_factory.USE_PB_QUERY", True),
        patch(
            "tools.warehouse_factory.PbQueryExecutionBackend",
            return_value=pb_backend,
        ) as pb_cls,
    ):
        manager.initialize_warehouse(
            "snowflake_conn",
            {
                "type": "snowflake",
                "user": "test_user",
                "password": "test_password",
            },
        )

    pb_cls.assert_called_once_with("snowflake")
    pb_backend.initialize_connection.assert_called_once()
    args, _ = pb_backend.initialize_connection.call_args
    assert args[0]["connection_name"] == "snowflake_conn"
