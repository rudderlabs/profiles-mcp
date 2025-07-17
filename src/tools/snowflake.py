from datetime import datetime, timedelta
import snowflake.snowpark
from snowflake.snowpark import Session
import os
from logger import setup_logger
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend

logger = setup_logger(__name__)

class Snowflake:
    session: Session
    last_used: datetime
    connection_details: dict

    def __init__(self):
        self.session = None
        self.last_used = None
        self.connection_details = None

    def initialize_connection(self, connection_details: dict):
        """Initialize a Snowflake connection with provided credentials"""
        logger.info(f"Initializing Snowflake connection for user: {connection_details.get('user')}")
        self.connection_details = connection_details
        self.create_session()
        self.update_last_used()

    def is_session_expired(self, timeout_hours: int = 1) -> bool:
        """Check if session hasn't been used for timeout_hours"""
        return datetime.now() - self.last_used > timedelta(hours=timeout_hours)

    def update_last_used(self):
        """Update the last used timestamp"""
        self.last_used = datetime.now()

    def create_session(self) -> snowflake.snowpark.Session:
        """Create a new Snowflake session with proper authentication handling"""
        logger.info(f"Creating new Snowflake session for user: {self.connection_details.get('user')}")
        
        # Build base config dict, filtering out None and empty string values
        config = {}
        base_config = {
            "user": self.connection_details.get('user'),
            "account": self.connection_details.get('account'),
            "warehouse": self.connection_details.get('warehouse'),
            "database": self.connection_details.get('database'),
            "schema": self.connection_details.get('schema'),
            "role": self.connection_details.get('role')
        }
        
        # Add base config (non-auth fields)
        for key, value in base_config.items():
            if value is not None and value != "":
                config[key] = value
        
        # Handle authentication - only one method should be used
        private_key_content = self.connection_details.get('private_key')
        private_key_file = self.connection_details.get('private_key_file')
        password = self.connection_details.get('password')
        private_key_passphrase = self.connection_details.get('private_key_passphrase')
        
        if private_key_content and private_key_content.strip():
            # Private key content provided directly
            logger.info("Using private key authentication (direct content)")
            try:
                private_key = load_pem_private_key(
                    private_key_content.encode(),
                    password=(
                        private_key_passphrase.encode()
                        if private_key_passphrase and private_key_passphrase.strip()
                        else None
                    ),
                    backend=default_backend(),
                )
                config["private_key"] = private_key
            except Exception as e:
                raise Exception(f"Failed to load private key content: {str(e)}")
                
        elif private_key_file and private_key_file.strip():
            # Private key file path provided
            logger.info("Using private key authentication (file path)")
            config["private_key_file"] = private_key_file
            if private_key_passphrase and private_key_passphrase.strip():
                config["private_key_passphrase"] = private_key_passphrase
                
        elif password and password.strip():
            # Password authentication
            logger.info("Using password authentication")
            config["password"] = password
        else:
            raise Exception("No valid authentication method found. Provide either password, private_key, or private_key_file")
        
        self.session = Session.builder.configs(config).create()

    def ensure_valid_session(self) -> None:
        """Ensure we have a valid Snowflake session"""

        if self.session is None:
            raise Exception("Session is not initialized. Call initialize_snowflake_connection() mcp tool first.")

        try:
            self.session.sql("SELECT 1").collect()
            self.update_last_used()
        except Exception as e:
            # Session expired or invalid, create new one
            logger.warning(f"Session invalid or expired: {str(e)}")
            if self.session is not None:
                try:
                    self.session.close()
                except:
                    pass

            # Create new session
            logger.info("Creating new session due to expiration/invalidity")
            self.session = self.create_session()
            self.update_last_used()

    def query(self, query: str) -> list[str]:
        """Query Snowflake and return results"""
        try:
            logger.info(f"Executing query: {query}")
            self.ensure_valid_session()
            result = self.session.sql(query)
            if query.lower().strip().startswith("select"):
                return result.toPandas()
            else:
                return result.collect()
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise Exception(f"Failed to execute query: {str(e)}")

    def raw_query(self, query: str, response_type: str = "list") -> list[str]:
        """Query Snowflake and return results"""
        try:
            logger.info(f"Executing raw query: {query} and response_type: {response_type}")
            self.ensure_valid_session()
            result = self.session.sql(query)
            if response_type == "list":
                return result.collect()
            elif response_type == "pandas":
                try:
                    df = result.toPandas()
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].fillna('Null')
                    return df
                except Exception as e:
                    logger.error(f"Failed to convert query to pandas: {str(e)}")
                    return result
            else:
                raise Exception(f"Invalid response type: {response_type}")
        except Exception as e:
            message = f"Raw query execution failed: {str(e)}"
            logger.error(message)
            raise Exception(message)

    def input_table_suggestions(self, database: str, schemas: str) -> list[str]:
        default_tables = ['tracks', 'pages', 'identifies', 'screens']
        schema_list = [schema.strip() for schema in schemas.split(',')]
        suggestions = []

        def find_matching_tables(schema: str, table_names: list, candidates: list) -> list:
            """Find tables from the candidates list that exist in table_names (substring match)"""
            matches = []
            for candidate in candidates:
                for t in table_names:
                    if candidate.lower() in t.lower():
                        matches.append(f"{database}.{schema}.{t}")
            return matches

        for schema in schema_list:
            tables = self.raw_query(f"SHOW TABLES IN {database}.{schema}")
            table_names = [table['name'] for table in tables]

            # Substring match for default tables
            suggestions.extend(find_matching_tables(schema, table_names, default_tables))

            # For each table that matches 'tracks' as a substring, get event tables
            tracks_like_tables = [t for t in table_names if 'tracks' in t.lower()]
            for tracks_table in tracks_like_tables:
                try:
                    rows = self.raw_query(f"SELECT event, count(*) FROM {database}.{schema}.{tracks_table} group by event order by 2 desc limit 20")
                    event_names = [row['EVENT'] for row in rows]
                    # For each event, check if a table with that event name exists (substring match)
                    suggestions.extend(find_matching_tables(schema, table_names, event_names))
                except Exception:
                    logger.warning(f"Failed to query events from {schema}.{tracks_table}")

        return list(set(suggestions))  # Remove duplicates

    def describe_table(self, database: str, schema: str, table: str) -> list[str]:
        """Describe a table"""
        try:
            self.ensure_valid_session()
            results = self.raw_query(f"DESCRIBE TABLE {database}.{schema}.{table}")
            return [f"{row['name']}: {row['type']}" for row in results]
        except Exception as e:
            logger.error(f"Failed to describe table: {str(e)}")
            return f"Failed to describe table: {str(e)}"

    def get_row_count(self, table_name: str, count_column: str = "COUNT(*)", where_clause: str | None = None) -> int:
        """Helper function to get count of rows from a table"""
        query = f"SELECT {count_column} FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        try:
            self.ensure_valid_session()
            response = self.raw_query(query)
            return response[0][count_column] or 0
        except Exception as e:
            message = f"Failed to get row count for table {table_name} with where_clause {where_clause}: {str(e)}"
            logger.error(message)
            raise Exception(message)

    def eligible_user_evaluator(self, filter_sqls: list[str], label_table: str, label_column: str, entity_column: str, min_pos_rate: float = 0.10, max_pos_rate: float = 0.90, min_total_rows: int = 5000) -> dict:
        """Evaluate eligible user filters"""

        try:
            logger.info(f"Evaluating eligible user filters: {filter_sqls}")
            self.ensure_valid_session()

            total_positive_rows = self.get_row_count(label_table, f"COUNT(DISTINCT {entity_column})", f"{label_column}=1") or 1
            logger.info(f"Total positive rows: {total_positive_rows}")

            best_filter = None
            best_metrics = {
                "filter_sql": None,
                "recall": -1.0,
                "eligible_rows": -1,
                "positive_label_rows": -1,
                "negative_label_rows": -1,
                "positive_rate": -1.0,
            }


            for filter_sql in filter_sqls:
                filter_total_rows = self.get_row_count(label_table, f"COUNT(DISTINCT {entity_column})", filter_sql) or 1
                filter_positive_rows = self.get_row_count(label_table, f"COUNT(DISTINCT {entity_column})", f"{label_column}=1 AND {filter_sql}") or 1
                filter_negative_rows = filter_total_rows - filter_positive_rows

                positive_rate = filter_positive_rows / filter_total_rows
                recall = filter_positive_rows / total_positive_rows

                logger.info(f"Filter: {filter_sql}, Total rows: {filter_total_rows}, Positive rows: {filter_positive_rows}, Positive rate: {positive_rate}, Recall: {recall}")

                if positive_rate < min_pos_rate or positive_rate > max_pos_rate:
                    continue

                is_better = (
                    recall > best_metrics["recall"] or
                    (recall == best_metrics["recall"] and filter_total_rows > best_metrics["eligible_rows"])
                )

                if is_better:
                    best_metrics = {
                        "filter_sql": filter_sql,
                        "eligible_rows": filter_total_rows,
                        "positive_label_rows": filter_positive_rows,
                        "negative_label_rows": filter_negative_rows,
                        "positive_rate": round(positive_rate, 3),
                        "recall": round(recall, 3),
                    }
                    best_filter = filter_sql

            return {
                "best_filter": best_filter,
                "best_metrics": best_metrics,
            }

        except Exception as e:
            message = f"Failed to evaluate eligible user filters: {str(e)}"
            logger.error(message)
            raise Exception(message)

