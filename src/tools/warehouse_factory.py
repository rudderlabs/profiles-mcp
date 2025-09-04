from typing import Optional
from logger import setup_logger
from tools.warehouse_base import BaseWarehouse
from tools.snowflake import Snowflake
from tools.bigquery import BigQuery

logger = setup_logger(__name__)


class WarehouseFactory:
    """
    Factory class for creating warehouse instances based on connection type.

    This factory provides a centralized way to create warehouse clients
    while maintaining a consistent interface across different warehouse types.
    """

    _warehouse_classes = {
        "snowflake": Snowflake,
        "bigquery": BigQuery,
    }

    @classmethod
    def create_warehouse(cls, warehouse_type: str) -> BaseWarehouse:
        """
        Create a warehouse instance based on the specified type.

        Args:
            warehouse_type: Type of warehouse ("snowflake", "bigquery", etc.)

        Returns:
            BaseWarehouse instance for the specified warehouse type

        Raises:
            ValueError: If warehouse type is not supported
        """
        warehouse_type_lower = warehouse_type.lower()

        if warehouse_type_lower not in cls._warehouse_classes:
            supported_types = list(cls._warehouse_classes.keys())
            raise ValueError(
                f"Unsupported warehouse type: '{warehouse_type}'. "
                f"Supported types: {supported_types}"
            )

        warehouse_class = cls._warehouse_classes[warehouse_type_lower]
        logger.info(f"Creating {warehouse_type} warehouse instance")

        return warehouse_class()

    @classmethod
    def get_supported_types(cls) -> list[str]:
        """
        Get a list of all supported warehouse types.

        Returns:
            List of supported warehouse type strings
        """
        return list(cls._warehouse_classes.keys())

    @classmethod
    def is_supported(cls, warehouse_type: str) -> bool:
        """
        Check if a warehouse type is supported.

        Args:
            warehouse_type: Type of warehouse to check

        Returns:
            True if warehouse type is supported, False otherwise
        """
        return warehouse_type.lower() in cls._warehouse_classes

    @classmethod
    def register_warehouse(cls, warehouse_type: str, warehouse_class: type) -> None:
        """
        Register a new warehouse type with the factory.

        This method allows for dynamic registration of new warehouse types
        without modifying the factory code.

        Args:
            warehouse_type: String identifier for the warehouse type
            warehouse_class: Class that implements BaseWarehouse interface

        Raises:
            TypeError: If warehouse_class doesn't implement BaseWarehouse
        """
        if not issubclass(warehouse_class, BaseWarehouse):
            raise TypeError(
                f"Warehouse class must implement BaseWarehouse interface. "
                f"Got: {warehouse_class.__name__}"
            )

        warehouse_type_lower = warehouse_type.lower()
        cls._warehouse_classes[warehouse_type_lower] = warehouse_class
        logger.info(f"Registered warehouse type: {warehouse_type}")


class WarehouseManager:
    """
    Manager class for handling multiple warehouse connections.

    This class provides a higher-level interface for managing warehouse
    connections, including connection pooling and session management.
    """

    def __init__(self):
        self._warehouses = {}
        self._active_warehouse: Optional[BaseWarehouse] = None
        self._active_warehouse_name: Optional[str] = None

    def initialize_warehouse(
        self, connection_name: str, connection_details: dict
    ) -> BaseWarehouse:
        """
        Initialize a warehouse connection.

        Args:
            connection_name: Unique name for this connection
            connection_details: Dictionary containing connection configuration

        Returns:
            Initialized BaseWarehouse instance

        Raises:
            ValueError: If warehouse type is not supported
        """
        warehouse_type = connection_details.get("type")
        if not warehouse_type:
            raise ValueError("Connection details must include 'type' field")

        # Create warehouse instance
        warehouse = WarehouseFactory.create_warehouse(warehouse_type)

        # Initialize the connection
        warehouse.initialize_connection(connection_details)

        # Store the warehouse instance
        self._warehouses[connection_name] = warehouse

        # Set as active warehouse
        self._active_warehouse = warehouse
        self._active_warehouse_name = connection_name

        logger.info(f"Initialized {warehouse_type} warehouse: {connection_name}")
        return warehouse

    def get_warehouse(self, connection_name: str = None) -> Optional[BaseWarehouse]:
        """
        Get a warehouse instance by name, or return the active warehouse.

        Args:
            connection_name: Name of the warehouse connection (optional)

        Returns:
            BaseWarehouse instance or None if not found
        """
        if connection_name:
            return self._warehouses.get(connection_name)
        return self._active_warehouse

    def get_active_warehouse(self) -> Optional[BaseWarehouse]:
        """
        Get the currently active warehouse.

        Returns:
            Active BaseWarehouse instance or None if no active warehouse
        """
        return self._active_warehouse

    def set_active_warehouse(self, connection_name: str) -> bool:
        """
        Set the active warehouse by connection name.

        Args:
            connection_name: Name of the warehouse connection

        Returns:
            True if warehouse was set as active, False if not found
        """
        warehouse = self._warehouses.get(connection_name)
        if warehouse:
            self._active_warehouse = warehouse
            self._active_warehouse_name = connection_name
            logger.info(f"Set active warehouse: {connection_name}")
            return True
        return False

    def get_connection_names(self) -> list[str]:
        """
        Get names of all initialized warehouse connections.

        Returns:
            List of connection names
        """
        return list(self._warehouses.keys())

    def get_active_warehouse_name(self) -> Optional[str]:
        """
        Get the name of the currently active warehouse.

        Returns:
            Name of active warehouse or None if no active warehouse
        """
        return self._active_warehouse_name

    def close_warehouse(self, connection_name: str) -> bool:
        """
        Close a warehouse connection and remove it from the manager.

        Args:
            connection_name: Name of the warehouse connection

        Returns:
            True if warehouse was closed, False if not found
        """
        warehouse = self._warehouses.get(connection_name)
        if warehouse:
            # Close session if it has a close method
            if hasattr(warehouse, "session") and warehouse.session:
                try:
                    if hasattr(warehouse.session, "close"):
                        warehouse.session.close()
                except Exception as e:
                    logger.warning(f"Error closing warehouse session: {e}")

            # Remove from warehouses
            del self._warehouses[connection_name]

            # Clear active warehouse if it was the one being closed
            if self._active_warehouse_name == connection_name:
                self._active_warehouse = None
                self._active_warehouse_name = None

            logger.info(f"Closed warehouse connection: {connection_name}")
            return True
        return False

    def close_all_warehouses(self) -> None:
        """Close all warehouse connections."""
        connection_names = list(self._warehouses.keys())
        for connection_name in connection_names:
            self.close_warehouse(connection_name)
        logger.info("Closed all warehouse connections")
