import re
from utils.pb_models_parser import PBModelsData
from logger import setup_logger
from tools.warehouse_base import BaseWarehouse
from utils.pb_config_parser import ProfilesUtils


logger = setup_logger(__name__)


class PropensityValidator:
    """
    Validates propensity model configurations for common pitfalls.

    This class provides a clean, modular approach to validating propensity models
    with clear separation of concerns and extensible validation rules.
    """

    def __init__(
        self, 
        project_path: str, 
        model_name: str, 
        warehouse_client: BaseWarehouse,
        pb_models_data: PBModelsData
    ):
        """
        Initialize the validator.
        
        Args:
            project_path: Path to the profiles project directory
            model_name: Name of the propensity model to validate
            warehouse_client: Warehouse client for data validation queries
            pb_models_data: PBModelsData object from pb show model_details command (optional)
        """
        self.project_path = project_path
        self.model_name = model_name
        self.warehouse_client = warehouse_client
        self.pb_models_data = pb_models_data
        self._initialize_result()

    def validate(self) -> dict:
        """
        Main validation entry point for propensity models.

        Combines both approaches:
        - Builds config map from YAML files for input table configurations
        - Uses pb_models_data for deterministic dependency traversal

        Returns:
            dict: Structured validation results with errors, warnings, and suggestions
        """
        logger.info(f"Validating propensity model: {self.model_name}")

        try:
            # Check if we have pb_models_data
            if not self.pb_models_data:
                self.result["errors"].append({
                    "type": "NO_MODELS_DATA",
                    "message": "pb_models_data not provided to validator. Seems that the project cannot be loaded.",
                    "remediation": "Ensure pb_models_data is passed to PropensityValidator"
                })
                self.result["validation_status"] = "FAILED"
                return self.result

            # Initialize configs from YAML files
            self._initialize_configs()

            # Validate YAML config only if model was found
            self._validate_propensity_model_spec(self.propensity_model)
            
            # Build input tables map for historic data validation
            input_tables_map = self._create_input_tables_map(self.configs["inputs"])
            
            # Validate using pb_models_data (this is the primary validation)
            self._validate_using_pb_models_data(input_tables_map)

            self._set_final_status()

        except Exception as e:
            self._handle_validation_error(e)

        return self.result

    def _initialize_result(self) -> None:
        """Initialize the validation result structure."""
        self.result = {
            "model_name": self.model_name,
            "validation_status": "PASSED",
            "errors": [],
            "warnings": [],
            "suggestions": [],
            "table_stats": {},
        }

    def _initialize_configs(self) -> None:
        """Initialize the configuration files."""
        utils = ProfilesUtils()
        self.configs = utils.load_all_configs(self.project_path)
        self.propensity_model = utils.find_model(
            self.configs["models"], self.model_name, "propensity"
        )

    def _validate_propensity_model_spec(self, prop_model) -> None:
        """
        Validate that the propensity model has a valid model_spec.
        """
        if not prop_model:
            self.result["errors"].append(
                {
                    "type": "MODEL_NOT_FOUND",
                    "message": f"Propensity model '{self.model_name}' not found in models configuration",
                    "remediation": "Verify the model name exists in your profiles.yaml file",
                }
            )
            self.result["validation_status"] = "FAILED"
            return
        
        self._validate_propensity_model_predict_window_days()

    def _validate_propensity_model_predict_window_days(self) -> None:
        """
        Validate that the propensity model has a predict_window_days defined.
        """
        pwd = self.propensity_model["model_spec"]["training"].get("predict_window_days")
        if pwd is None:
            self.result["errors"].append(
                {
                    "type": "PREDICT_WINDOW_DAYS_NOT_FOUND",
                    "message": f"Propensity model '{self.model_name}' has no predict_window_days defined",
                    "remediation": "Add a predict_window_days to the model_spec",
                }
            )
            self.result["validation_status"] = "FAILED"
        elif pwd <= 0:
            self.result["errors"].append(
                {
                    "type": "PREDICT_WINDOW_DAYS_NOT_POSITIVE",
                    "message": f"Propensity model '{self.model_name}' has a non-positive predict_window_days: {pwd}",
                    "remediation": "Set predict_window_days to a positive integer",
                }
            )
            self.result["validation_status"] = "FAILED"

    def _validate_using_pb_models_data(self, input_tables_map: dict) -> None:
        """
        Main validation method using pb_models_data for dependency traversal
        and YAML configs for input table validation.
        """
        # Find all propensity models
        prop_model = self.pb_models_data.get_model_by_name(self.model_name)
        
        if not prop_model:
            self.result["errors"].append({
                "type": "PROPENSITY_MODEL_NOT_FOUND",
                "message": f"Propensity model '{self.model_name}' not found",
                "remediation": "Verify the model name exists in your profiles.yaml"
            })
            return
        
        self._validate_propensity_model(prop_model, input_tables_map)

    def _create_input_tables_map(self, inputs_config: dict) -> dict:
        """Create a map of input table names to their configurations."""
        input_map = {}
        inputs = inputs_config.get("inputs", [])
        for input_table in inputs:
            table_name = input_table.get("name")
            if table_name:
                input_map[table_name] = input_table
        return input_map

    def _create_entity_vars_map(self, models_config: dict) -> dict:
        """Create a map of entity variable names to their configurations."""
        entity_vars_map = {}
        var_groups = models_config.get("var_groups", [])
        for var_group in var_groups:
            vars_list = var_group.get("vars", [])
            for var_item in vars_list:
                entity_var = var_item.get("entity_var")
                if entity_var and entity_var.get("name"):
                    entity_vars_map[entity_var["name"]] = entity_var
        return entity_vars_map

    def _parse_table_reference(self, table_ref: str) -> dict:
        """Parse table reference like 'inputs/table_name'."""
        parts = table_ref.split("/")
        if len(parts) == 2 and parts[0] == "inputs":
            return {"source_type": parts[0], "table_name": parts[1]}
        return None

    def _validate_propensity_model(self, prop_model, input_tables_map: dict) -> None:
        """
        Validate a propensity model set using combined approach.
        
        Uses pb_models_data for dependency traversal and YAML configs for input validation.
        
        Args:
            prop_model: The propensity model to validate
            input_tables_map: Map of input table names to their YAML configurations
        """
        model_name = prop_model.name
        logger.info(f"Validating propensity model set: {model_name}")
        
        # Get training model 
        training_model = self.pb_models_data.get_model_by_name(f"{model_name}_training")

        # Track validated entity vars to avoid duplicate validation
        validated_entity_vars = set()
        leaf_model_event_stream_error_set = set()
        
        logger.info(f"Validating model: {training_model.name} ({training_model.model_type})")
        
        # Validation 1: Check direct inputs have is_feature=true
        self._validate_direct_input_features(training_model, model_name)

        # Validation 2-4: Validate entity-var dependencies
        if training_model.dependencies:
            for dep_path in training_model.dependencies:
                dep_model = self._find_model_by_path(dep_path)
                if not dep_model:
                    self.result["errors"].append({
                        "type": "DEPENDENCY_NOT_FOUND",
                        "message": f"Dependency '{dep_path}' not found in the project",
                        "remediation": "Ensure the dependency specified in the propensity model exists in the project"
                    })
                    continue
                
                # Only validate entity_var_item models
                if dep_model.model_type == "entity_var_item":
                    if dep_model.path_ref in validated_entity_vars:
                        continue
                    validated_entity_vars.add(dep_model.path_ref)
                    
                    # Validation 2: Check for time-based functions
                    self._validate_entity_var_time_functions(dep_model)
                    
                    # Validation 3: Check direct dependencies for is_event_stream
                    self._validate_entity_var_direct_dependencies(dep_model)
                    
                # Validation 4: Traverse to leaf nodes and validate historic data
                self._validate_entity_var_leaf_inputs(dep_model, input_tables_map, leaf_model_event_stream_error_set)
    
    def _validate_entity_var_time_functions(self, entity_var_model) -> None:
        """
        Validate that entity_var doesn't use time-based functions.
        
        Checks for current_date() or datediff() in the feature's yaml definition.
        
        Args:
            entity_var_model: The entity_var_item model to validate
            prop_model_name: Name of the propensity model (for error reporting)
        """
        if not entity_var_model.feature_data or not entity_var_model.feature_data.yaml:
            return

        yaml_content = entity_var_model.feature_data.yaml

        # Regex patterns for time-based functions (case-insensitive)
        current_date_pattern = re.compile(r'\bcurrent_date\s*\(', re.IGNORECASE)
        datediff_pattern = re.compile(r'\bdatediff\s*\(', re.IGNORECASE)
        
        # Check for current_date()
        if current_date_pattern.search(yaml_content):
            self.result["errors"].append({
                "type": "TIME_FUNCTION_IN_FEATURE",
                "feature": entity_var_model.name,
                "message": f"Feature '{entity_var_model.name}' uses current_date() which is not allowed in propensity models",
                "remediation": "Remove current_date() and use macro macro_datediff or macro_datediff_n instead"
            })
        
        # Check for datediff()
        if datediff_pattern.search(yaml_content):
            self.result["errors"].append({
                "type": "TIME_FUNCTION_IN_FEATURE",
                "feature": entity_var_model.name,
                "message": f"Feature '{entity_var_model.name}' uses datediff() which is not allowed in propensity models",
                "remediation": "Remove datediff() and use macro macro_datediff or macro_datediff_n instead"
            })

        # check dependencies for indirect var dependencies also.
        for dep_path in entity_var_model.dependencies:
            dep_model = self._find_model_by_path(dep_path)
            if dep_model.model_type in ["entity_var_item", "input_var_item"]:
                self._validate_entity_var_time_functions(dep_model)
    
    def _validate_entity_var_direct_dependencies(self, entity_var_model) -> None:
        """
        Validate that direct dependencies of entity_var have is_event_stream=true.
        
        For direct dependencies of type 'input' or 'sql_template', checks if is_event_stream is true.
        
        Args:
            entity_var_model: The entity_var_item model to validate
            prop_model_name: Name of the propensity model (for error reporting)
        """
        if not entity_var_model.dependencies:
            return

        for dep_path in entity_var_model.dependencies:
            dep_model = self._find_model_by_path(dep_path)
            if not dep_model:
                self.result["errors"].append({
                    "type": "DEPENDENCY_NOT_FOUND",
                    "message": f"Dependency '{dep_path}' not found in the project",
                    "remediation": "Ensure the dependency specified in the propensity model exists in the project"
                })
                continue
            
            # Check if it's an input or sql_template
            if dep_model.model_type in ["input", "sql_template"]:
                if not dep_model.is_event_stream:
                    self.result["errors"].append({
                        "type": "NON_EVENT_STREAM_INPUT",
                        "feature": entity_var_model.name,
                        "table": dep_model.name,
                        "message": f"Input table '{dep_model.name}' used by feature '{entity_var_model.name}' must have is_event_stream: true for propensity modeling",
                        "remediation": f"Add occurred_at_col in app_defaults of the input table or model_spec of sql_template type model '{dep_model.name}'"
                    })
                continue
            
            if dep_model.model_type == "entity_var_item":
                self._validate_entity_var_direct_dependencies(dep_model)
    
    def _validate_entity_var_leaf_inputs(self, entity_var_model, input_tables_map: dict, leaf_model_event_stream_error_set: set) -> None:
        """
        Traverse dependency tree to leaf nodes and validate historic data.
        
        For leaf nodes of type 'input', validates that sufficient historic data exists.
        
        Args:
            entity_var_model: The entity_var_item model to start traversal from
            prop_model_name: Name of the propensity model (for error reporting)
            input_tables_map: Map of input table names to their YAML configurations
        """
        visited = set()
        leaf_input_nodes = {}
        
        def traverse(current_model):
            if current_model.path_ref in visited:
                return
            visited.add(current_model.path_ref)

            # If no dependencies, it's a leaf node
            if not current_model.dependencies:
                if current_model.model_type == "input":
                    leaf_input_nodes[f"inputs/{current_model.name}"] = current_model
                return

            # Traverse dependencies
            has_valid_dependency = False
            for dep_path in current_model.dependencies:
                dep_model = self._find_model_by_path(dep_path)
                if dep_model:
                    has_valid_dependency = True
                    traverse(dep_model)
            
            # If all dependencies are invalid and it's an input, it's a leaf node
            if not has_valid_dependency and current_model.model_type == "input":
                leaf_input_nodes[f"inputs/{current_model.name}"] = current_model
        
        # Start traversal from the entity_var
        traverse(entity_var_model)

        # Validate historic data for each leaf input
        for leaf_path, leaf_model in leaf_input_nodes.items():
            if leaf_path in leaf_model_event_stream_error_set:
                continue
            leaf_model_event_stream_error_set.add(leaf_path)
            input_table_config = input_tables_map.get(leaf_model.name)
            if input_table_config:
                # Get predict_window_days from propensity model
                prop_model_obj = self._find_model_by_path(leaf_path)
                if not prop_model_obj:
                    self.result["errors"].append({
                        "type": "DEPENDENCY_NOT_FOUND",
                        "message": f"Dependency {leaf_path} not found in the project",
                        "remediation": "Ensure the dependency specified in the propensity model exists in the project"
                    })
                    continue
                if not prop_model_obj.is_event_stream:
                    self.result["errors"].append({
                        "type": "NON_EVENT_STREAM_INPUT",
                        "feature": entity_var_model.name,
                        "message": f"Input table {leaf_path} used by feature '{entity_var_model.name}' must have is_event_stream: true",
                        "remediation": f"Add occurred_at_col in app_defaults of the input table or model_spec of sql_template trype model '{prop_model_obj.name}'"
                    })
                
                # Call _validate_historic_data with is_fallback=False
                self._validate_historic_data(
                    input_table_config, 
                    feature_name=entity_var_model.name, 
                    is_fallback=False
                )

    def _validate_historic_data(
        self,
        input_table_config: dict,
        feature_name: str = None,
        is_fallback: bool = False,
    ) -> None:
        """
        Validate historic data availability in the input table.

        Args:
            input_table_config: Configuration of the input table
            feature_name: Name of the feature (for direct validation) or None (for fallback)
            is_fallback: If True, reports as warning; if False, reports as error
        """
        app_defaults = input_table_config.get("app_defaults", {})
        table_name = input_table_config.get("name")
        db_table_name = app_defaults.get("table")
        occurred_at_col = app_defaults.get("occurred_at_col")

        if table_name in self.result["table_stats"]:
            logger.debug(
                f"Skipping historic data validation for table: {table_name} because it already has stats"
            )
            return

        if not occurred_at_col:
            logger.debug(
                f"Skipping historic data validation for table: {table_name} because it doesn't have an occurred_at_col"
            )
            return

        try:
            # Use warehouse-independent date difference calculation
            warehouse_type = getattr(self.warehouse_client, "warehouse_type", "unknown")

            if warehouse_type.lower() == "bigquery":
                # BigQuery syntax for date difference
                date_diff_expr = f"DATE_DIFF(DATE(MAX({occurred_at_col})), DATE(MIN({occurred_at_col})), DAY)"
            else:
                # Snowflake and other warehouses (default)
                date_diff_expr = (
                    f"DATEDIFF(day, MIN({occurred_at_col}), MAX({occurred_at_col}))"
                )

            query = f"""
            SELECT
                MIN({occurred_at_col}) as min_date,
                MAX({occurred_at_col}) as max_date,
                {date_diff_expr} as date_range_days,
                COUNT(*) as total_rows
            FROM {db_table_name}
            WHERE {occurred_at_col} IS NOT NULL
            """

            stats_result = self.warehouse_client.raw_query(
                query, response_type="pandas"
            )
            logger.debug(f"Stats result: {db_table_name}, {stats_result}")

            if not stats_result.empty:
                self._process_table_stats(
                    stats_result, input_table_config, feature_name, is_fallback
                )

        except Exception as e:
            logger.warning(
                f"Could not validate historic data for table {db_table_name}: {e}"
            )
            if is_fallback:
                self.result["warnings"].append(
                    {
                        "type": "FALLBACK_DATA_VALIDATION_SKIPPED",
                        "table": table_name,
                        "message": f"Could not validate historic data availability for table '{table_name}': {str(e)}",
                        "context": "This input table issue may not affect your propensity model if this table doesn't contribute to your model's feature pipeline",
                        "remediation": "If this table is used by your propensity model features, manually verify it contains sufficient historic data",
                    }
                )
            else:
                self.result["suggestions"].append(
                    {
                        "type": "DATA_VALIDATION_SKIPPED",
                        "feature": feature_name,
                        "table": table_name,
                        "message": f"Could not validate historic data availability: {str(e)}",
                        "remediation": "Manually verify the table contains sufficient historic data",
                    }
                )

    def _process_table_stats(
        self,
        stats_result,
        input_table_config: dict,
        feature_name: str = None,
        is_fallback: bool = False,
    ) -> None:
        """
        Process and validate table statistics.

        Args:
            stats_result: Pandas DataFrame with table statistics
            input_table_config: Configuration of the input table
            feature_name: Name of the feature (for direct validation) or None (for fallback)
            is_fallback: If True, reports as warning; if False, reports as error
        """
        stats = stats_result.iloc[0]
        min_date = stats["MIN_DATE"]
        max_date = stats["MAX_DATE"]
        date_range_days = stats["DATE_RANGE_DAYS"] or 0
        total_rows = stats["TOTAL_ROWS"] or 0
        table_name = input_table_config.get("name", "unknown_table")

        # Store table statistics
        self.result["table_stats"][table_name] = {
            "min_date": str(min_date) if min_date else None,
            "max_date": str(max_date) if max_date else None,
            "date_range_days": date_range_days,
            "total_rows": total_rows,
            "occurred_at_col": input_table_config.get("app_defaults", {}).get(
                "occurred_at_col"
            ),
        }

        min_required_days = self.propensity_model["model_spec"]["training"][
            "predict_window_days"
        ]

        if date_range_days < min_required_days:
            if is_fallback:
                self.result["warnings"].append(
                    {
                        "type": "FALLBACK_INSUFFICIENT_HISTORIC_DATA",
                        "table": table_name,
                        "message": f"Table '{table_name}' has only {date_range_days} days of data (min: {min_date}, max: {max_date}). Minimum {min_required_days} days (i.e., > predict_window_days) is recommended for propensity modeling",
                        "context": "This input table issue may not affect your propensity model if this table doesn't contribute to your model's feature pipeline",
                        "remediation": "If this table is used by your propensity model features, ensure it contains sufficient historic data or consider using a different data source",
                    }
                )
            else:
                self.result["errors"].append(
                    {
                        "type": "INSUFFICIENT_HISTORIC_DATA",
                        "feature": feature_name,
                        "table": table_name,
                        "message": f"Table has only {date_range_days} days of data (min: {min_date}, max: {max_date}). Minimum {min_required_days} days (i.e., > predict_window_days) is required for propensity modeling for it to generate past point-in-time feature data required for the model training. ETL tables that overwrite data cannot provide this historical context",
                        "remediation": "Ensure the table contains sufficient historic data or consider using a different data source",
                    }
                )

    def _set_final_status(self) -> None:
        """Set the final validation status based on errors and warnings."""
        if self.result["errors"]:
            self.result["validation_status"] = "FAILED"
        elif self.result["warnings"]:
            self.result["validation_status"] = "WARNINGS"
        else:
            self.result["validation_status"] = "PASSED"

    def _handle_validation_error(self, error: Exception) -> None:
        """Handle unexpected validation errors."""
        logger.error(f"Error during validation: {error}")
        self.result["errors"].append(
            {
                "type": "VALIDATION_ERROR",
                "message": f"Unexpected error during validation: {str(error)}",
                "remediation": "Check project configuration files and try again",
            }
        )
        self.result["validation_status"] = "FAILED"

    
    def _validate_direct_input_features(self, model, prop_model_name: str) -> None:
        """
        Validate that direct input features have is_feature: true.
        
        Direct inputs of type entity_var_item or nested_column must have is_feature: true
        
        Args:
            model: The model to validate
            prop_model_name: Name of the propensity model (for error reporting)
        """
        if not model.dependencies:
            return
        
        for dep_path in model.dependencies:
            dep_model = self._find_model_by_path(dep_path)
            if not dep_model:
                continue
            
            # Check if it's a direct input of specified types
            if dep_model.model_type in ["entity_var_item", "nested_column"]:
                if not dep_model.is_feature:
                    self.result["errors"].append({
                        "type": "NON_FEATURE_INPUT",
                        "feature": dep_model.name,
                        "message": f"Direct input '{dep_model.name}' must have is_feature: true for propensity modeling",
                        "remediation": f"Ensure '{dep_model.name}' is marked as a feature in your configuration"
                    })
    
    def _find_model_by_path(self, path_ref: str):
        """
        Find a model by its path_ref.
        
        Args:
            path_ref: The path reference to search for
            
        Returns:
            Model object or None if not found
        """
        for model in self.pb_models_data.models:
            if model.path_ref == path_ref:
                return model
        return None

