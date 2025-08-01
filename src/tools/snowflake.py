from datetime import datetime, timedelta
import snowflake.snowpark
from snowflake.snowpark import Session
import os
from logger import setup_logger
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from dateutil.parser import parse

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

    def suggest_optimal_pilot_dates(self, input_tables: list[str], target_duration_days: int = 7) -> dict:
        """
        Intelligently suggests optimal begin_time and end_time for pilot/dry runs to achieve fast execution.
        
        This method replaces arbitrary date selection with data-driven analysis to find the minimum 
        date range required for a successful and fast profiles run. It analyzes actual data patterns
        in your input tables to recommend optimal test periods.

        Args:
            input_tables: List of fully qualified table names (e.g., ["DB.SCHEMA.TABLE1", "DB.SCHEMA.TABLE2"])
            target_duration_days: Desired test duration in days (default: 7 for weekly patterns)
                                 - 1-3 days: Ultra-fast testing
                                 - 7 days: Recommended for most cases  
                                 - 14+ days: Comprehensive testing

        Returns:
            dict: Comprehensive analysis with suggested date ranges containing:
                - recommended: Primary recommendation with begin_time and end_time
                - alternatives: List of alternative options (conservative, extended)
                - analysis: Detailed analysis of data patterns and reasoning
                - warnings: Any issues found during analysis
                - success: Boolean indicating if analysis succeeded
        """
        try:
            logger.info(f"Analyzing input tables for optimal pilot dates: {input_tables}")
            self.ensure_valid_session()

            if not input_tables:
                return {
                    "success": False,
                    "error": "No input tables provided",
                    "recommended": None,
                    "alternatives": [],
                    "analysis": {},
                    "warnings": ["No input tables provided for analysis"]
                }

            max_timestamps = []
            table_analysis = {}
            warnings = []

            # Common timestamp column names to check
            timestamp_columns = [
                'timestamp', 'sent_at', 'received_at', 'created_at', 'updated_at', 
                'event_time', 'occurred_at', 'original_timestamp', 'loaded_at'
            ]

            for table in input_tables:
                try:
                    # Get table schema to find timestamp columns
                    parts = table.split('.')
                    if len(parts) != 3:
                        warnings.append(f"Invalid table format '{table}'. Expected format: DATABASE.SCHEMA.TABLE")
                        continue
                    
                    database, schema, table_name = parts
                    table_columns = self.describe_table(database, schema, table_name)
                    
                    # Find timestamp columns in this table
                    found_timestamp_cols = []
                    for col_info in table_columns:
                        col_name = col_info.split(':')[0].strip().lower()
                        col_type = col_info.split(':')[1].strip().lower()
                        
                        if col_name in timestamp_columns or 'timestamp' in col_type:
                            found_timestamp_cols.append(col_name)

                    if not found_timestamp_cols:
                        warnings.append(f"No timestamp columns found in table '{table}'")
                        continue

                    # Query max timestamp for each found column
                    table_max_timestamps = []
                    for col in found_timestamp_cols:
                        try:
                            query = f"SELECT MAX({col}) as max_ts FROM {table} WHERE {col} IS NOT NULL"
                            result = self.raw_query(query)
                            
                            if result and result[0]['MAX_TS']:
                                table_max_timestamps.append({
                                    'column': col,
                                    'max_timestamp': result[0]['MAX_TS'],
                                    'table': table
                                })
                        except Exception as e:
                            warnings.append(f"Failed to query {col} from {table}: {str(e)}")

                    if table_max_timestamps:
                        # Get the most recent timestamp from this table
                        latest_in_table = max(table_max_timestamps, key=lambda x: x['max_timestamp'])
                        max_timestamps.append(latest_in_table)
                        table_analysis[table] = {
                            'timestamp_columns': found_timestamp_cols,
                            'latest_timestamp': latest_in_table['max_timestamp'],
                            'latest_column': latest_in_table['column']
                        }
                    else:
                        warnings.append(f"No valid timestamp data found in table '{table}'")

                except Exception as e:
                    warnings.append(f"Error analyzing table '{table}': {str(e)}")

            if not max_timestamps:
                return {
                    "success": False,
                    "error": "No valid timestamp data found in any input tables",
                    "recommended": None,
                    "alternatives": [],
                    "analysis": table_analysis,
                    "warnings": warnings
                }

            # Find the overall maximum timestamp across all tables
            overall_max = max(max_timestamps, key=lambda x: x['max_timestamp'])
            max_timestamp = overall_max['max_timestamp']
            
            # Convert to datetime if it's a string
            if isinstance(max_timestamp, str):
                max_timestamp = parse(max_timestamp)

            # Calculate suggested date ranges
            end_time = max_timestamp
            begin_time = end_time - timedelta(days=target_duration_days)
            
            # Format timestamps for profiles usage (ISO format with timezone)
            begin_time_str = begin_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

            # Generate alternative options
            alternatives = []
            
            # Conservative option (1 day)
            if target_duration_days > 1:
                conservative_begin = end_time - timedelta(days=1)
                alternatives.append({
                    "name": "conservative",
                    "duration_days": 1,
                    "begin_time": conservative_begin.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "end_time": end_time_str,
                    "rationale": "Ultra-fast execution with minimal data for quick validation"
                })

            # Extended option (14 days)
            if target_duration_days < 14:
                extended_begin = end_time - timedelta(days=14)
                alternatives.append({
                    "name": "extended",
                    "duration_days": 14,
                    "begin_time": extended_begin.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "end_time": end_time_str,
                    "rationale": "More comprehensive testing with broader data coverage"
                })

            # Calculate data freshness
            days_since_last_data = (datetime.now() - max_timestamp).days
            freshness_warning = None
            if days_since_last_data > 7:
                freshness_warning = f"Latest data is {days_since_last_data} days old. Consider checking if data pipeline is current."

            analysis = {
                "overall_max_timestamp": max_timestamp.isoformat(),
                "days_since_last_data": days_since_last_data,
                "target_duration_days": target_duration_days,
                "tables_analyzed": len(input_tables),
                "tables_with_data": len(table_analysis),
                "data_freshness": "current" if days_since_last_data <= 1 else "stale" if days_since_last_data > 7 else "recent"
            }

            if freshness_warning:
                warnings.append(freshness_warning)

            recommended = {
                "begin_time": begin_time_str,
                "end_time": end_time_str,
                "duration_days": target_duration_days,
                "rationale": f"Using last {target_duration_days} days of data ending at {end_time_str} for optimal balance of speed and data coverage",
                "confidence": "high" if days_since_last_data <= 1 else "medium" if days_since_last_data <= 7 else "low"
            }

            return {
                "success": True,
                "recommended": recommended,
                "alternatives": alternatives,
                "analysis": analysis,
                "table_details": table_analysis,
                "warnings": warnings
            }

        except Exception as e:
            error_message = f"Failed to suggest optimal pilot dates: {str(e)}"
            logger.error(error_message)
            return {
                "success": False,
                "error": error_message,
                "recommended": None,
                "alternatives": [],
                "analysis": {},
                "warnings": [error_message]
            }

