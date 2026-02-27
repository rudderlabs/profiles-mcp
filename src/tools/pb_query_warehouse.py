import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Union, List, Dict, Any
from uuid import uuid4

import pandas as pd
import yaml

from logger import setup_logger
from tools.warehouse_base import BaseWarehouse, WarehouseConnectionDetails

logger = setup_logger(__name__)

ANSI_ESCAPE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


class PbQueryWarehouse(BaseWarehouse):
    """
    Warehouse implementation that routes queries through `pb query` CLI.

    Instead of maintaining direct SDK connections, this delegates all query
    execution to the `pb query` subprocess, which handles warehouse connectivity
    via siteconfig.yaml. Requires a stub pb project (minimal pb_project.yaml +
    empty models/ dir) to satisfy pb's project structure requirements.

    Currently validated for Snowflake only (PoC).
    """

    _schema_version_cache: int = None

    def __init__(self):
        super().__init__()
        self._stub_project_path: str = None
        self._connection_name: str = None
        self._siteconfig_path: str = None

    @classmethod
    def _get_schema_version(cls) -> int:
        """Get the native schema version from `pb version` output.

        Caches the result at class level since it won't change during a session.
        """
        if cls._schema_version_cache is not None:
            return cls._schema_version_cache

        try:
            result = subprocess.run(
                ["pb", "version"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            combined_output = (result.stdout or "") + (result.stderr or "")
            match = re.search(r"Native schema version:\s+(\d+)", combined_output)
            if match:
                cls._schema_version_cache = int(match.group(1))
                return cls._schema_version_cache
            raise RuntimeError(
                f"Could not parse schema version from pb version output: {combined_output}"
            )
        except FileNotFoundError:
            raise RuntimeError(
                "pb CLI not found. Ensure profiles-rudderstack is installed and pb is on PATH."
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("pb version command timed out")

    def initialize_connection(self, connection_details: dict) -> None:
        """Initialize a pb query-based warehouse connection.

        Creates a stub pb project directory and validates connectivity with SELECT 1.

        Args:
            connection_details: Must contain 'connection_name'. Other fields
                (type, account, user, etc.) are stored but not used for SDK connections.
        """
        self._connection_name = connection_details.get("connection_name")
        if not self._connection_name:
            raise ValueError("connection_details must include 'connection_name'")

        self._siteconfig_path = connection_details.get(
            "siteconfig_path", str(Path.home() / ".pb" / "siteconfig.yaml")
        )

        self.connection_details = WarehouseConnectionDetails(connection_details)

        # Create stub project directory
        self._stub_project_path = tempfile.mkdtemp(prefix="pb_mcp_")
        models_dir = os.path.join(self._stub_project_path, "models")
        output_dir = os.path.join(self._stub_project_path, "output")
        os.makedirs(models_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        # Write pb_project.yaml with dynamic schema version
        schema_version = self._get_schema_version()
        pb_project = {
            "name": "pb_mcp_stub",
            "schema_version": schema_version,
            "connection": self._connection_name,
            "model_folders": ["models"],
        }
        pb_project_path = os.path.join(self._stub_project_path, "pb_project.yaml")
        with open(pb_project_path, "w") as f:
            yaml.dump(pb_project, f, default_flow_style=False)

        logger.info(
            f"Created stub project at {self._stub_project_path} "
            f"with connection '{self._connection_name}' and schema_version {schema_version}"
        )

        # Validate connectivity
        try:
            self.raw_query("SELECT 1")
            logger.info("pb query connectivity validated with SELECT 1")
        except Exception as e:
            # Clean up on failure
            self.cleanup()
            raise RuntimeError(
                f"Failed to validate pb query connectivity: {e}"
            ) from e

        self.update_last_used()

    def create_session(self) -> Any:
        """No-op — pb query manages its own connections."""
        return True

    def ensure_valid_session(self) -> None:
        """No-op — pb query manages its own connections."""
        pass

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        """Execute a SQL query via pb query and return results.

        Args:
            query: SQL query to execute
            response_type: "list" for List[Dict], "pandas" for DataFrame

        Returns:
            Query results in the specified format

        Raises:
            RuntimeError: If pb query fails or times out
        """
        csv_filename = f"query_{uuid4().hex}.csv"
        csv_output_path = os.path.join(
            self._stub_project_path, "output", csv_filename
        )

        cmd = [
            "pb", "query", query,
            "-p", self._stub_project_path,
            "-f", csv_filename,
            "--max_rows", "0",
            "--migrate_on_load",
        ]

        # Add siteconfig path if non-default
        default_siteconfig = str(Path.home() / ".pb" / "siteconfig.yaml")
        if self._siteconfig_path and self._siteconfig_path != default_siteconfig:
            cmd.extend(["-c", self._siteconfig_path])

        logger.info(f"Executing pb query: {query}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=540,
            )

            if result.returncode != 0:
                stderr_clean = ANSI_ESCAPE.sub("", result.stderr or "")
                raise RuntimeError(
                    f"pb query failed (exit code {result.returncode}): {stderr_clean}"
                )

            # Parse CSV output
            if not os.path.exists(csv_output_path):
                raise RuntimeError(
                    f"pb query did not produce output file at {csv_output_path}"
                )

            df = pd.read_csv(csv_output_path, na_values=["<nil>"])

            if response_type == "list":
                return df.to_dict(orient="records")
            elif response_type == "pandas":
                for col in df.columns:
                    if df[col].dtype == "object":
                        df[col] = df[col].fillna("Null")
                return df
            else:
                raise ValueError(f"Invalid response_type: {response_type}")

        except subprocess.TimeoutExpired:
            raise RuntimeError("pb query timed out after 540 seconds")
        finally:
            # Clean up CSV file
            try:
                if os.path.exists(csv_output_path):
                    os.remove(csv_output_path)
            except OSError:
                pass

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        """Describe a table using DESCRIBE TABLE via pb query."""
        try:
            self._validate_identifier(database, "database")
            self._validate_identifier(schema, "schema")
            self._validate_identifier(table, "table")

            results = self.raw_query(
                f"DESCRIBE TABLE {database}.{schema}.{table}"
            )
            return [
                f"{row.get('name') or row.get('NAME')}: {row.get('type') or row.get('TYPE')}"
                for row in results
            ]
        except Exception as e:
            logger.error(f"Failed to describe table: {e}")
            return [f"Failed to describe table: {str(e)}"]

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        """Suggest relevant tables for profiles input configuration."""
        default_tables = ["tracks", "pages", "identifies", "screens"]
        schema_list = [schema.strip() for schema in schemas.split(",")]
        suggestions = []

        def find_matching_tables(
            schema: str, table_names: list, candidates: list
        ) -> list:
            matches = []
            for candidate in candidates:
                for t in table_names:
                    if candidate.lower() in t.lower():
                        matches.append(f"{database}.{schema}.{t}")
            return matches

        for schema in schema_list:
            tables = self.raw_query(f"SHOW TABLES IN {database}.{schema}")
            table_names = [
                table.get("name") or table.get("NAME") for table in tables
            ]

            suggestions.extend(
                find_matching_tables(schema, table_names, default_tables)
            )

            tracks_like_tables = [
                t for t in table_names if "tracks" in t.lower()
            ]
            for tracks_table in tracks_like_tables:
                try:
                    rows = self.raw_query(
                        f"SELECT event, count(*) FROM {database}.{schema}.{tracks_table} "
                        f"group by event order by 2 desc limit 20"
                    )
                    event_names = [
                        row.get("EVENT") or row.get("event")
                        for row in rows
                        if row.get("EVENT") or row.get("event")
                    ]
                    suggestions.extend(
                        find_matching_tables(schema, table_names, event_names)
                    )
                except Exception:
                    logger.warning(
                        f"Failed to query events from {schema}.{tracks_table}"
                    )

        return list(set(suggestions))

    def cleanup(self) -> None:
        """Remove the stub project temp directory."""
        if self._stub_project_path and os.path.exists(self._stub_project_path):
            try:
                shutil.rmtree(self._stub_project_path)
                logger.info(f"Cleaned up stub project at {self._stub_project_path}")
            except OSError as e:
                logger.warning(f"Failed to clean up stub project: {e}")
            self._stub_project_path = None
