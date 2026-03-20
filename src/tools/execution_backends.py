from abc import ABC, abstractmethod
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Union
from uuid import uuid4

import pandas as pd
import yaml

from logger import setup_logger
from tools.warehouse_base import BaseWarehouse, WarehouseConnectionDetails

_validate_identifier = BaseWarehouse._validate_identifier

logger = setup_logger(__name__)


class WarehouseExecutionBackend(ABC):
    """Execution backend interface for warehouse operations."""

    @abstractmethod
    def initialize_connection(self, connection_details: dict) -> None:
        """Initialize backend-specific connection/session state."""

    @abstractmethod
    def create_session(self) -> Any:
        """Create or recover an execution session/client."""

    @abstractmethod
    def ensure_valid_session(self) -> None:
        """Validate backend connectivity and refresh if needed."""

    @abstractmethod
    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        """Execute SQL query and return result."""

    @abstractmethod
    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        """Describe a table in column:type format."""

    @abstractmethod
    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        """Return input table suggestions."""

    @abstractmethod
    def cleanup(self) -> None:
        """Release resources owned by backend."""

    @property
    @abstractmethod
    def connection_details(self) -> WarehouseConnectionDetails:
        """Warehouse connection details used by backend."""

    @property
    @abstractmethod
    def session(self) -> Any:
        """Warehouse session/client object if available."""


class SdkExecutionBackend(WarehouseExecutionBackend):
    """Adapter backend that delegates to existing SDK warehouse implementations."""

    def __init__(self, warehouse: BaseWarehouse):
        self._warehouse = warehouse

    def initialize_connection(self, connection_details: dict) -> None:
        self._warehouse.initialize_connection(connection_details)

    def create_session(self) -> Any:
        return self._warehouse.create_session()

    def ensure_valid_session(self) -> None:
        self._warehouse.ensure_valid_session()

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        return self._warehouse.raw_query(query, response_type=response_type)

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        return self._warehouse.describe_table(database, schema, table)

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        return self._warehouse.input_table_suggestions(database, schemas)

    def cleanup(self) -> None:
        # SDK backends usually keep the client/session on the warehouse object.
        if hasattr(self._warehouse, "session") and self._warehouse.session:
            try:
                if hasattr(self._warehouse.session, "close"):
                    self._warehouse.session.close()
            except Exception as exc:
                logger.warning(f"Error closing SDK warehouse session: {exc}")

    @property
    def connection_details(self) -> WarehouseConnectionDetails:
        return self._warehouse.connection_details

    @property
    def session(self) -> Any:
        return getattr(self._warehouse, "session", None)


class PbQueryStrategy(ABC):
    """Warehouse-specific SQL semantics for pb-query backend helpers."""

    @abstractmethod
    def warehouse_type(self) -> str:
        """Strategy warehouse type identifier."""

    @abstractmethod
    def describe_table_query(self, database: str, schema: str, table: str) -> str:
        """Build helper SQL to describe a table."""

    @abstractmethod
    def list_tables_query(self, database: str, schema: str) -> str:
        """Build helper SQL to list tables in a schema."""

    @abstractmethod
    def top_events_query(self, database: str, schema: str, table: str) -> str:
        """Build helper SQL to fetch top events from a tracks table."""

    @abstractmethod
    def relation_name(self, database: str, schema: str, table: str) -> str:
        """Build formatted relation name for suggestions/output."""

    def extract_table_names(self, rows: List[Dict]) -> List[str]:
        """Extract table names from list query output."""
        names = []
        for row in rows:
            name = (
                row.get("name")
                or row.get("NAME")
                or row.get("table")
                or row.get("tableName")
                or row.get("table_name")
                or row.get("TABLE_NAME")
            )
            if name:
                names.append(name)
        return names

    def normalize_describe_rows(self, rows: List[Dict]) -> List[str]:
        """Normalize DESCRIBE output to column:type format."""
        normalized = []
        for row in rows:
            col = (
                row.get("name")
                or row.get("NAME")
                or row.get("column_name")
                or row.get("COLUMN_NAME")
                or row.get("col_name")
            )
            dtype = (
                row.get("type")
                or row.get("TYPE")
                or row.get("data_type")
                or row.get("DATA_TYPE")
            )
            if col and dtype:
                normalized.append(f"{col}: {dtype}")
        return normalized


class SnowflakePbQueryStrategy(PbQueryStrategy):
    def warehouse_type(self) -> str:
        return "snowflake"

    def relation_name(self, database: str, schema: str, table: str) -> str:
        return f"{database}.{schema}.{table}"

    def describe_table_query(self, database: str, schema: str, table: str) -> str:
        return f"DESCRIBE TABLE {self.relation_name(database, schema, table)}"

    def list_tables_query(self, database: str, schema: str) -> str:
        return f"SHOW TABLES IN {database}.{schema}"

    def top_events_query(self, database: str, schema: str, table: str) -> str:
        relation = self.relation_name(database, schema, table)
        return (
            f"SELECT event, count(*) FROM {relation} "
            "group by event order by 2 desc limit 20"
        )


class BigQueryPbQueryStrategy(PbQueryStrategy):
    def warehouse_type(self) -> str:
        return "bigquery"

    def _qualify_relation(self, project: str, dataset: str, relation: str) -> str:
        parts = relation.split(".")
        if len(parts) == 3:
            return relation
        if len(parts) == 2:
            return f"{project}.{relation}"
        return f"{project}.{dataset}.{relation}"

    def _quoted_relation(self, project: str, dataset: str, relation: str) -> str:
        fq = self._qualify_relation(project, dataset, relation)
        return f"`{fq}`"

    def relation_name(self, database: str, schema: str, table: str) -> str:
        return self._qualify_relation(database, schema, table)

    def describe_table_query(self, database: str, schema: str, table: str) -> str:
        return (
            "SELECT column_name AS name, data_type AS type "
            f"FROM `{database}.{schema}.INFORMATION_SCHEMA.COLUMNS` "
            f"WHERE table_name = '{table}' "
            "ORDER BY ordinal_position"
        )

    def list_tables_query(self, database: str, schema: str) -> str:
        return (
            "SELECT table_name "
            f"FROM `{database}.{schema}.INFORMATION_SCHEMA.TABLES` "
            "WHERE table_type = 'BASE TABLE'"
        )

    def top_events_query(self, database: str, schema: str, table: str) -> str:
        relation = self._quoted_relation(database, schema, table)
        return (
            "SELECT event, COUNT(*) as count "
            f"FROM {relation} "
            "GROUP BY event ORDER BY count DESC LIMIT 20"
        )


class DatabricksPbQueryStrategy(PbQueryStrategy):
    def __init__(self, catalog: str = None):
        self._catalog = catalog

    def warehouse_type(self) -> str:
        return "databricks"

    def _is_uc(self) -> bool:
        return bool(self._catalog and self._catalog.strip())

    def relation_name(self, database: str, schema: str, table: str) -> str:
        if self._is_uc():
            return f"{self._catalog}.{schema}.{table}"
        if database and database.strip() and database != schema:
            return f"{database}.{schema}.{table}"
        return f"{schema}.{table}"

    def describe_table_query(self, database: str, schema: str, table: str) -> str:
        return f"DESCRIBE TABLE {self.relation_name(database, schema, table)}"

    def list_tables_query(self, database: str, schema: str) -> str:
        if self._is_uc():
            return f"SHOW TABLES IN {self._catalog}.{schema}"
        if database and database.strip() and database != schema:
            return f"SHOW TABLES IN {database}.{schema}"
        return f"SHOW TABLES IN {schema}"

    def top_events_query(self, database: str, schema: str, table: str) -> str:
        relation = self.relation_name(database, schema, table)
        return (
            "SELECT event, COUNT(*) as count "
            f"FROM {relation} GROUP BY event ORDER BY count DESC LIMIT 20"
        )


class RedshiftPbQueryStrategy(PbQueryStrategy):
    def warehouse_type(self) -> str:
        return "redshift"

    def relation_name(self, database: str, schema: str, table: str) -> str:
        if database and database.strip() and database != schema:
            return f"{database}.{schema}.{table}"
        return f"{schema}.{table}"

    def describe_table_query(self, database: str, schema: str, table: str) -> str:
        return (
            "SELECT column_name AS name, data_type AS type "
            "FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
            "ORDER BY ordinal_position"
        )

    def list_tables_query(self, database: str, schema: str) -> str:
        return (
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' ORDER BY table_name"
        )

    def top_events_query(self, database: str, schema: str, table: str) -> str:
        relation = self.relation_name(database, schema, table)
        return (
            "SELECT event, COUNT(*) as count "
            f"FROM {relation} GROUP BY event ORDER BY count DESC LIMIT 20"
        )


class PbQueryExecutionBackend(WarehouseExecutionBackend):
    """Backend that executes SQL through `pb query` CLI."""

    ANSI_ESCAPE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
    _schema_version_cache: int = None

    @staticmethod
    def _query_timeout_seconds() -> int:
        raw_value = os.environ.get("PB_QUERY_TIMEOUT_SECONDS", "540")
        try:
            timeout = int(raw_value)
        except (TypeError, ValueError):
            timeout = 540
        return max(timeout, 1)

    def __init__(self, warehouse_type: str):
        self._warehouse_type = warehouse_type.lower()
        self._connection_details: WarehouseConnectionDetails = None
        self._strategy: PbQueryStrategy = None
        self._stub_project_path: str = None
        self._connection_name: str = None
        self._siteconfig_path: str = None
        self._session = None
        self._pb_initialized = False

    @classmethod
    def _get_schema_version(cls) -> int:
        if cls._schema_version_cache is not None:
            return cls._schema_version_cache

        try:
            result = subprocess.run(
                ["pb", "version"], capture_output=True, text=True, timeout=30
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "pb CLI is not available. Please install profiles-rudderstack and ensure pb is on PATH."
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("pb CLI version check timed out") from exc

        combined_output = (result.stdout or "") + (result.stderr or "")
        match = re.search(r"Native schema version:\s+(\d+)", combined_output)
        if not match:
            raise RuntimeError("Could not determine pb schema version from pb version")

        cls._schema_version_cache = int(match.group(1))
        return cls._schema_version_cache

    def _build_strategy(self, connection_details: dict) -> PbQueryStrategy:
        if self._warehouse_type == "snowflake":
            return SnowflakePbQueryStrategy()
        if self._warehouse_type == "bigquery":
            return BigQueryPbQueryStrategy()
        if self._warehouse_type == "databricks":
            return DatabricksPbQueryStrategy(
                catalog=connection_details.get("catalog")
            )
        if self._warehouse_type == "redshift":
            return RedshiftPbQueryStrategy()
        raise ValueError(f"Unsupported warehouse type for pb-query mode: {self._warehouse_type}")

    def _default_siteconfig_path(self) -> str:
        return str(Path.home() / ".pb" / "siteconfig.yaml")

    def _run_timeout_seconds(self) -> int:
        raw_value = os.environ.get("PB_RUN_TIMEOUT_SECONDS", "540")
        try:
            timeout = int(raw_value)
        except (TypeError, ValueError):
            timeout = 540
        return max(timeout, 1)

    def _run_pb_initialization(self) -> None:
        if self._pb_initialized:
            return

        cmd = ["pb", "run", "-p", self._stub_project_path, "--migrate_on_load"]

        if self._siteconfig_path and self._siteconfig_path != self._default_siteconfig_path():
            cmd.extend(["-c", self._siteconfig_path])

        timeout_seconds = self._run_timeout_seconds()
        start_time = time.monotonic()
        logger.info(
            "Starting pb run initialization",
            extra={
                "warehouse_type": self._warehouse_type,
                "connection_name": self._connection_name,
                "timeout_seconds": timeout_seconds,
            },
        )

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "pb CLI is not available. Please install profiles-rudderstack and ensure pb is on PATH."
            ) from exc
        except subprocess.TimeoutExpired as exc:
            elapsed_seconds = round(time.monotonic() - start_time, 3)
            logger.warning(
                "pb run initialization timed out",
                extra={
                    "warehouse_type": self._warehouse_type,
                    "connection_name": self._connection_name,
                    "elapsed_seconds": elapsed_seconds,
                    "timeout_seconds": timeout_seconds,
                },
            )
            stderr_text = exc.stderr.decode("utf-8", errors="ignore") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            concise = self._concise_error(
                stderr_text,
                "pb run initialization timed out.",
            )
            raise RuntimeError(concise) from exc

        elapsed_seconds = round(time.monotonic() - start_time, 3)
        logger.info(
            "Finished pb run initialization",
            extra={
                "warehouse_type": self._warehouse_type,
                "connection_name": self._connection_name,
                "returncode": result.returncode,
                "elapsed_seconds": elapsed_seconds,
            },
        )

        if result.returncode != 0:
            stderr_clean = self.ANSI_ESCAPE.sub("", result.stderr or "")
            concise = self._concise_error(
                stderr_clean,
                "pb run initialization failed. Please verify connection and siteconfig.",
            )
            raise RuntimeError(concise)

        self._pb_initialized = True

    def _setup_stub_project(self) -> None:
        self._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_")
        os.makedirs(os.path.join(self._stub_project_path, "models"), exist_ok=True)
        os.makedirs(os.path.join(self._stub_project_path, "output"), exist_ok=True)

        schema_version = self._get_schema_version()
        pb_project = {
            "name": "pb_mcp_stub",
            "schema_version": schema_version,
            "connection": self._connection_name,
            "model_folders": ["models"],
        }
        with open(
            os.path.join(self._stub_project_path, "pb_project.yaml"), "w"
        ) as handle:
            yaml.dump(pb_project, handle, default_flow_style=False)

    def _concise_error(self, stderr: str, fallback: str) -> str:
        clean = self.ANSI_ESCAPE.sub("", stderr or "").strip()
        if not clean:
            return fallback
        first_line = clean.splitlines()[0].strip()
        return first_line or fallback

    def initialize_connection(self, connection_details: dict) -> None:
        self._connection_details = WarehouseConnectionDetails(connection_details)
        self._strategy = self._build_strategy(connection_details)
        self._connection_name = connection_details.get("connection_name")
        self._siteconfig_path = connection_details.get(
            "siteconfig_path", self._default_siteconfig_path()
        )

        if not self._connection_name:
            raise ValueError("connection_details must include 'connection_name' for pb-query mode")

        self._setup_stub_project()
        self._session = True

        try:
            self._run_pb_initialization()
            self.raw_query("SELECT 1")
        except Exception:
            self.cleanup()
            raise

    def create_session(self) -> Any:
        return True

    def ensure_valid_session(self) -> None:
        return None

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        if not self._stub_project_path:
            raise RuntimeError("pb-query backend is not initialized")

        self._run_pb_initialization()

        csv_name = f"query_{uuid4().hex}.csv"
        csv_path = os.path.join(self._stub_project_path, "output", csv_name)

        cmd = [
            "pb",
            "query",
            query,
            "-p",
            self._stub_project_path,
            "-f",
            csv_name,
            "--max_rows",
            "0",
        ]

        if self._siteconfig_path and self._siteconfig_path != self._default_siteconfig_path():
            cmd.extend(["-c", self._siteconfig_path])

        timeout_seconds = self._query_timeout_seconds()
        query_preview = " ".join(query.split())[:160]
        start_time = time.monotonic()
        logger.info(
            "Starting pb query",
            extra={
                "warehouse_type": self._warehouse_type,
                "connection_name": self._connection_name,
                "response_type": response_type,
                "timeout_seconds": timeout_seconds,
                "query_preview": query_preview,
            },
        )

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "pb CLI is not available. Please install profiles-rudderstack and ensure pb is on PATH."
            ) from exc
        except subprocess.TimeoutExpired as exc:
            elapsed_seconds = round(time.monotonic() - start_time, 3)
            logger.warning(
                "pb query timed out",
                extra={
                    "warehouse_type": self._warehouse_type,
                    "connection_name": self._connection_name,
                    "response_type": response_type,
                    "elapsed_seconds": elapsed_seconds,
                    "timeout_seconds": timeout_seconds,
                    "query_preview": query_preview,
                },
            )
            logger.debug(f"pb query timeout. sql={query}")
            timeout_detail = ""
            stderr_text = exc.stderr.decode("utf-8", errors="ignore") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            if stderr_text:
                first_line = self._concise_error(stderr_text, "")
                if first_line:
                    timeout_detail = f" Last stderr: {first_line}"
            raise RuntimeError(
                f"Query execution timed out in pb query. Try reducing query scope or disabling migrations.{timeout_detail}"
            ) from exc

        elapsed_seconds = round(time.monotonic() - start_time, 3)
        logger.info(
            "Finished pb query",
            extra={
                "warehouse_type": self._warehouse_type,
                "connection_name": self._connection_name,
                "response_type": response_type,
                "returncode": result.returncode,
                "elapsed_seconds": elapsed_seconds,
                "query_preview": query_preview,
            },
        )

        if result.returncode != 0:
            stderr_clean = self.ANSI_ESCAPE.sub("", result.stderr or "")
            logger.debug(
                "pb query failed",
                extra={
                    "sql": query,
                    "returncode": result.returncode,
                    "stderr": stderr_clean,
                },
            )
            concise = self._concise_error(
                stderr_clean,
                "pb query execution failed. Please verify SQL and connection configuration.",
            )
            raise RuntimeError(concise)

        if not os.path.exists(csv_path):
            logger.debug("pb query completed without CSV output", extra={"sql": query})
            raise RuntimeError(
                "pb query returned no output file. Please verify connection and query syntax."
            )

        try:
            df = pd.read_csv(csv_path, na_values=["<nil>"])
        finally:
            try:
                if os.path.exists(csv_path):
                    os.remove(csv_path)
            except OSError:
                pass

        if response_type == "list":
            return df.to_dict(orient="records")
        if response_type == "pandas":
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].fillna("Null")
            return df
        raise ValueError(f"Invalid response_type: {response_type}")

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        try:
            if database:
                _validate_identifier(database, "database")
            _validate_identifier(schema, "schema")
            _validate_identifier(table, "table")
            query = self._strategy.describe_table_query(database, schema, table)
            rows = self.raw_query(query, response_type="list")
            normalized = self._strategy.normalize_describe_rows(rows)
            if normalized:
                return normalized
            return ["Failed to describe table: empty schema metadata returned"]
        except Exception as exc:
            logger.debug(
                "pb describe_table failed",
                extra={
                    "database": database,
                    "schema": schema,
                    "table": table,
                    "error": str(exc),
                },
            )
            return ["Failed to describe table: unable to fetch table metadata"]

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        default_tables = ["tracks", "pages", "identifies", "screens"]
        schema_list = [s.strip() for s in schemas.split(",") if s.strip()]
        suggestions = []

        if database:
            _validate_identifier(database, "database")
        for schema_name in schema_list:
            _validate_identifier(schema_name, "schema")

        def find_matching_tables(schema_name: str, table_names: list, candidates: list) -> list:
            matches = []
            for candidate in candidates:
                for table_name in table_names:
                    if candidate.lower() in table_name.lower():
                        matches.append(
                            self._strategy.relation_name(database, schema_name, table_name)
                        )
            return matches

        try:
            for schema_name in schema_list:
                rows = self.raw_query(
                    self._strategy.list_tables_query(database, schema_name),
                    response_type="list",
                )
                table_names = self._strategy.extract_table_names(rows)

                suggestions.extend(
                    find_matching_tables(schema_name, table_names, default_tables)
                )

                tracks_tables = [t for t in table_names if "tracks" in t.lower()]
                for tracks_table in tracks_tables:
                    try:
                        event_rows = self.raw_query(
                            self._strategy.top_events_query(
                                database, schema_name, tracks_table
                            ),
                            response_type="list",
                        )
                        event_names = [
                            row.get("event")
                            or row.get("EVENT")
                            or row.get("Event")
                            for row in event_rows
                            if row.get("event") or row.get("EVENT") or row.get("Event")
                        ]
                        suggestions.extend(
                            find_matching_tables(
                                schema_name,
                                table_names,
                                [name for name in event_names if isinstance(name, str)],
                            )
                        )
                    except Exception as exc:
                        logger.debug(
                            "pb top-events query failed",
                            extra={
                                "schema": schema_name,
                                "table": tracks_table,
                                "error": str(exc),
                            },
                        )
        except Exception as exc:
            logger.debug(
                "pb input_table_suggestions failed",
                extra={"database": database, "schemas": schemas, "error": str(exc)},
            )

        return list(set(suggestions))

    def cleanup(self) -> None:
        if self._stub_project_path and os.path.exists(self._stub_project_path):
            try:
                shutil.rmtree(self._stub_project_path)
            except OSError as exc:
                logger.warning(f"Failed to clean up pb temp project: {exc}")
        self._stub_project_path = None
        self._session = None
        self._pb_initialized = False

    @property
    def connection_details(self) -> WarehouseConnectionDetails:
        return self._connection_details

    @property
    def session(self) -> Any:
        return self._session
