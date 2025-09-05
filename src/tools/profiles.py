import datetime
import glob
import json
import os
import re
import shutil
import subprocess

import yaml
from constants import PB_SITE_CONFIG_PATH
from logger import setup_logger
from utils.environment import is_cloud_based_environment

logger = setup_logger(__name__)


def str_presenter(dumper, data):
    if data.count("\n") > 0:
        data = "\n".join(
            [line.rstrip() for line in data.splitlines()]
        )  # Remove any trailing spaces, then put it back together again
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


class ProfilesTools:
    # Pre-compiled fake name patterns for performance
    FAKE_NAME_PATTERNS = {
        "my_database",
        "my_schema",
        "my_table",
        "my_connection",
        "example_db",
        "sample_schema",
        "test_table",
        "demo_",
        "your_database",
        "your_schema",
        "your_table",
        "database_name",
        "schema_name",
        "table_name",
        "connection_name",
        "user_confirmed",
        "user_chosen",  # Even these placeholders should be replaced
    }

    def __init__(self):
        pass

    def analyze_and_validate_project(self, project_path: str) -> dict:
        """
        Analyzes the structure of an existing profiles project.

        This tool analyzes project structure by reading pb_project.yaml and scanning
        the specified model_folders for YAML configuration files.
        It's designed to help CSMs and users understand existing projects without requiring
        warehouse access.

        Args:
            project_path: Path to the profiles project directory (should contain pb_project.yaml)

        Returns:
            dict: Project analysis results with structure info, errors, warnings, and status
        """
        try:
            result = self._analyze_project_structure(project_path)

            # Determine overall status
            if result["errors"]:
                result["status"] = "error"
            elif result["warnings"]:
                result["status"] = "warning"
            else:
                result["status"] = "success"

        except Exception as e:
            logger.error(f"Error analyzing project: {e}")
            result = {
                "project_path": project_path,
                "pb_project_found": False,
                "pb_project_config": {},
                "model_folders": [],
                "yaml_files": {},
                "scanned_directories": [],
                "errors": [f"Failed to analyze project: {str(e)}"],
                "warnings": [],
                "status": "error",
                "summary": {
                    "total_yaml_files": 0,
                    "model_folders_found": 0,
                    "model_folders_scanned": 0,
                    "pb_project_valid": False,
                },
            }

        return result

    def _analyze_project_structure(self, project_path: str) -> dict:
        """
        Analyzes the structure of a profiles project directory efficiently.
        Only scans pb_project.yaml and the model_folders specified within it.
        Skips output/, migrations/, and hidden directories for performance.

        Returns:
            dict: Project structure analysis including found files, directories, and metadata
        """
        analysis = {
            "project_path": os.path.abspath(project_path),
            "pb_project_found": False,
            "pb_project_config": {},
            "model_folders": [],
            "yaml_files": {},
            "scanned_directories": [],
            "errors": [],
            "warnings": [],
        }

        project_abs_path = os.path.abspath(project_path)

        # Check if project directory exists
        if not os.path.exists(project_abs_path):
            analysis["errors"].append(
                f"Project directory does not exist: {project_abs_path}"
            )
            return analysis

        if not os.path.isdir(project_abs_path):
            analysis["errors"].append(f"Path is not a directory: {project_abs_path}")
            return analysis

        try:
            # Step 1: Look for pb_project.yaml in the base directory
            pb_project_path = os.path.join(project_abs_path, "pb_project.yaml")

            if not os.path.exists(pb_project_path):
                analysis["warnings"].append(
                    "pb_project.yaml not found. This might be a new/greenfield project."
                )
                analysis["model_folders"] = []
                analysis["summary"] = {
                    "total_yaml_files": 0,
                    "model_folders_found": 0,
                    "model_folders_scanned": 0,
                    "pb_project_valid": False,
                }
                return analysis

            analysis["pb_project_found"] = True
            analysis["yaml_files"]["pb_project"] = {
                "path": pb_project_path,
                "relative_path": "pb_project.yaml",
                "size_bytes": os.path.getsize(pb_project_path),
                "type": "project_config",
            }

            # Step 2: Parse pb_project.yaml to get model_folders
            try:
                with open(pb_project_path, "r", encoding="utf-8") as f:
                    file_content = f.read().strip()

                    # Check for empty file
                    if not file_content:
                        analysis["errors"].append("pb_project.yaml is empty.")
                        return analysis

                    pb_config = yaml.safe_load(file_content)
                    analysis["pb_project_config"] = pb_config or {}

                    # Extract model_folders from configuration
                    model_folders = (
                        pb_config.get("model_folders") if pb_config else None
                    )

                    if model_folders is None:
                        analysis["errors"].append(
                            "pb_project.yaml is missing required 'model_folders' key."
                        )
                        return analysis

                    if not isinstance(model_folders, list):
                        analysis["errors"].append(
                            "'model_folders' in pb_project.yaml must be a list."
                        )
                        return analysis

                    if not model_folders:
                        analysis["errors"].append(
                            "'model_folders' in pb_project.yaml cannot be empty."
                        )
                        return analysis

                    analysis["model_folders"] = model_folders

            except yaml.YAMLError as e:
                analysis["errors"].append(
                    f"Failed to parse pb_project.yaml - invalid YAML syntax: {str(e)}"
                )
                return analysis
            except PermissionError:
                analysis["errors"].append(
                    f"Permission denied reading pb_project.yaml: {pb_project_path}"
                )
                return analysis
            except FileNotFoundError:
                analysis["errors"].append(
                    f"pb_project.yaml not found at expected path: {pb_project_path}"
                )
                return analysis
            except Exception as e:
                analysis["errors"].append(
                    f"Unexpected error reading pb_project.yaml: {str(e)}"
                )
                return analysis

            # Step 3: Scan the specified model_folders for YAML files
            yaml_patterns = ["*.yaml", "*.yml"]
            found_yamls = []

            for model_folder in analysis["model_folders"]:
                # Skip commented out folders (lines starting with #)
                if model_folder.strip().startswith("#"):
                    continue

                folder_path = os.path.join(project_abs_path, model_folder)
                if not os.path.exists(folder_path):
                    analysis["warnings"].append(
                        f"Model folder does not exist: {model_folder}"
                    )
                    continue

                if not os.path.isdir(folder_path):
                    analysis["warnings"].append(
                        f"Model folder is not a directory: {model_folder}"
                    )
                    continue

                analysis["scanned_directories"].append(model_folder)

                # Recursively find YAML files in this model folder
                for pattern in yaml_patterns:
                    folder_yamls = glob.glob(
                        os.path.join(folder_path, "**", pattern), recursive=True
                    )
                    found_yamls.extend(folder_yamls)

            # Step 4: Categorize YAML files found in model folders
            for yaml_file in found_yamls:
                rel_path = os.path.relpath(yaml_file, project_abs_path)
                file_name = os.path.basename(yaml_file)
                file_size = os.path.getsize(yaml_file)

                analysis["yaml_files"][rel_path] = {
                    "path": yaml_file,
                    "relative_path": rel_path,
                    "size_bytes": file_size,
                    "filename": file_name,
                }

            # Step 5: Add summary statistics
            analysis["summary"] = {
                "total_yaml_files": len(analysis["yaml_files"]),
                "model_folders_found": len(analysis["model_folders"]),
                "model_folders_scanned": len(analysis["scanned_directories"]),
                "pb_project_valid": len(analysis["pb_project_config"]) > 0,
            }

            # Step 6: Add warnings for common issues
            if analysis["summary"]["model_folders_scanned"] == 0:
                analysis["warnings"].append("No valid model folders found or scanned.")

            if (
                analysis["summary"]["model_folders_found"]
                != analysis["summary"]["model_folders_scanned"]
            ):
                analysis["warnings"].append(
                    "Some model folders specified in pb_project.yaml were not found on disk."
                )

        except Exception as e:
            logger.error(f"Error during project structure analysis: {e}")
            analysis["errors"].append(f"Error analyzing project structure: {str(e)}")

        return analysis

    def get_existing_connections(self) -> list[str]:
        try:
            with open(PB_SITE_CONFIG_PATH, "r") as file:
                config = yaml.safe_load(file)
                connections = config["connections"]
                return list(connections.keys())
        except Exception as e:
            return f"Unable to read siteconfig.yaml file: {e}. Please run `pb init connection` to create a connection."

    def get_profiles_output_schema(self, pb_project_file_path: str) -> str:
        with open(pb_project_file_path, "r") as file:
            pb_project_config = yaml.safe_load(file)
            connection_name = pb_project_config["connection"]
        try:
            with open(PB_SITE_CONFIG_PATH, "r") as file:
                config = yaml.safe_load(file)
                connection_config = config["connections"][connection_name]
                output_schema = connection_config["outputs"][
                    connection_config["target"]
                ]["schema"]
                output_db = connection_config["outputs"][connection_config["target"]][
                    "dbname"
                ]
            return f"{output_db.upper()}.{output_schema.upper()}"
        except Exception as e:
            return f"Unable to read siteconfig.yaml file: {e}"

    def extract_json_from_output(self, text):
        # Remove ANSI color codes (optional but recommended)
        ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
        clean_text = ansi_escape.sub("", text)

        # Find the first '{'
        start = clean_text.find("{")
        if start == -1:
            raise ValueError("No JSON object found in output.")

        # Use brace counting to find the matching '}'
        brace_count = 0
        for i in range(start, len(clean_text)):
            if clean_text[i] == "{":
                brace_count += 1
            elif clean_text[i] == "}":
                brace_count -= 1
                if brace_count == 0:
                    end = i + 1
                    break
        else:
            raise ValueError("No matching closing brace found for JSON object.")

        json_str = clean_text[start:end]

        return json.loads(json_str)

    def get_profiles_models_details(
        self, pb_project_file_path: str, pb_show_models_output_file_path: str
    ) -> dict:
        output_schema = self.get_profiles_output_schema(pb_project_file_path).upper()
        tables_info = {}
        with open(pb_show_models_output_file_path, "r") as file:
            pb_response = file.read()
        try:
            models_details = self.extract_json_from_output(pb_response)
        except Exception as e:
            logger.error(f"Error extracting JSON from output: {e}")
            error_message = f"Unable to parse the pb show models output file due to some error in parsing the JSON: {e}. Please check the output file for any detailed logs that can help with the error"
            return {"error": error_message}
        for _, model_info in models_details.items():
            if model_info.get("model_type") == "feature_view":
                entity_name = model_info.get("model_path").split("/")[0]
                if entity_name not in tables_info:
                    tables_info[entity_name] = {"feature_views": [], "id_stitcher": ""}
                tables_info[entity_name]["feature_views"].append(
                    f"{output_schema}.{model_info['material_name'].upper()}"
                )
            elif model_info.get("model_type") == "id_stitcher":
                entity_name = model_info["model_path"].split("/")[0]
                if entity_name == "models":
                    continue
                if entity_name not in tables_info:
                    tables_info[entity_name] = {"feature_views": [], "id_stitcher": ""}
                id_stitcher_name = model_info["material_name"].upper()
                if (
                    "DEFAULT" not in id_stitcher_name
                    or tables_info[entity_name]["id_stitcher"] == ""
                ):
                    # Capture the id stitcher name if it's not captured yet. If it's already captured, overwrite if the original one was the default id-stitcher
                    # An underlying assumption here is that an entity can have max two id-stitchers, one with 'default' in the name and one without.
                    tables_info[entity_name][
                        "id_stitcher"
                    ] = f"{output_schema}.{id_stitcher_name}"
        response = {"output_schema": output_schema, "tables_info": tables_info}
        return response

    def setup_new_profiles_project(self, project_path: str) -> dict:
        """
        Sets up a new profiles project in the specified directory using pip and venv.
        Steps:
        1. Ensure the project directory exists.
        2. Verify Python 3.10 is installed.
        3. Create a Python virtual environment (unless running in kubernetes pod environment).
        4. Install the profiles-rudderstack package using pip (unless running in kubernetes pod).
        5. Return a status dict with messages and errors.
        """
        messages = []
        errors = []

        abs_project_path = os.path.abspath(project_path)
        messages.append(f"Target project path: {abs_project_path}")

        def ensure_directory(path: str) -> bool:
            try:
                os.makedirs(path, exist_ok=True)
                messages.append(f"Project directory '{path}' ensured.")
                return True
            except Exception as e:
                errors.append(f"Error creating directory '{path}': {e}")
                return False

        def find_executable(name: str) -> str | None:
            executable_path = shutil.which(name)
            if not executable_path:
                errors.append(
                    f"`{name}` command not found. Please ensure it is installed and in your PATH."
                )
                return None
            messages.append(f"`{name}` command found at: {executable_path}.")
            return executable_path

        def check_python_version(executable_path: str) -> bool:
            try:
                result = subprocess.run(
                    [executable_path, "-c", "import sys; print(sys.version_info[:2])"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                version_str = result.stdout.strip()
                # Parse the output which looks like "(3, 10)"
                version_tuple = eval(version_str)
                if version_tuple[0] != 3 or version_tuple[1] != 10:
                    errors.append(
                        f"Python version {version_tuple[0]}.{version_tuple[1]} detected. Python 3.10 is required."
                    )
                    return False
                messages.append(f"Python 3.10 detected. Version requirement satisfied.")
                return True
            except Exception as e:
                errors.append(f"Failed to check Python version: {e}")
                return False

        def run_command(command: list[str], cwd: str, desc: str) -> bool:
            messages.append(f"Attempting: {desc}")
            messages.append(f"Executing: `{' '.join(command)}` in `{cwd}`")
            logger.info(f"Running command: {command}")
            current_env = os.environ.copy()
            try:
                process = subprocess.run(
                    command,
                    cwd=cwd,
                    check=True,
                    capture_output=True,
                    text=True,
                    env=current_env,
                )
                messages.append(f"Successfully executed: `{' '.join(command)}`.")
                if process.stdout.strip():
                    messages.append(f"Stdout:\n{process.stdout.strip()}")
                if process.stderr.strip():
                    messages.append(f"Stderr:\n{process.stderr.strip()}")
                return True
            except subprocess.CalledProcessError as e:
                logger.error(f"Command failed: {command}")
                errors.append(f"Error during: {desc}")
                errors.append(
                    f"Command `{' '.join(command)}` failed with exit code {e.returncode}."
                )
                if e.stdout and e.stdout.strip():
                    errors.append(f"Stdout:\n{e.stdout.strip()}")
                if e.stderr and e.stderr.strip():
                    errors.append(f"Stderr:\n{e.stderr.strip()}")
                return False
            except Exception as e:
                logger.error(f"An unexpected error occurred - {str(e)}")
                errors.append(f"An unexpected error occurred - {str(e)}")
                return False

        if not ensure_directory(abs_project_path):
            return {"status": "failure", "messages": messages, "errors": errors}

        python_executable = find_executable("python3") or find_executable("python")
        if not python_executable:
            return {"status": "failure", "messages": messages, "errors": errors}

        # Check Python version
        if not check_python_version(python_executable):
            return {"status": "failure", "messages": messages, "errors": errors}

        pip_executable = find_executable("pip3") or find_executable("pip")
        if not pip_executable:
            return {"status": "failure", "messages": messages, "errors": errors}

        readme_path = os.path.join(abs_project_path, "README.md")
        readme_content = ""
        readme_writestatus_msg = ""
        # Check if running in kubernetes pod
        if is_cloud_based_environment():
            messages.append(
                "Kubernetes pod detected - skipping virtual environment creation"
            )
            resp = self._setup_cloud_based_project(abs_project_path, messages)
            readme_content = resp["readme_content"]
            readme_writestatus_msg = resp["message"]
        else:
            venv_path = os.path.join(abs_project_path, ".venv")
            venv_bin_dir = os.path.join(
                venv_path, "bin" if os.name != "nt" else "Scripts"
            )

            # Check for pip3 first, fall back to pip if pip3 is not available
            venv_pip3 = os.path.join(venv_bin_dir, "pip3")
            venv_pip = os.path.join(venv_bin_dir, "pip")
            venv_pip_to_use = venv_pip3 if os.path.exists(venv_pip3) else venv_pip
            venv_pb = os.path.join(venv_bin_dir, "pb")

            commands_to_execute = [
                {
                    "cmd": [python_executable, "-m", "venv", ".venv"],
                    "desc": "Create virtual environment (.venv)",
                    "success_message": "Virtual environment '.venv' created",
                    "skip_message": f"Virtual environment '.venv' already exists at '{venv_path}'",
                    "pre_check": lambda: os.path.isdir(venv_path),
                },
                {
                    "cmd": [venv_pip_to_use, "install", "profiles-rudderstack"],
                    "desc": "Install 'profiles-rudderstack' package using pip",
                    "success_message": "Package 'profiles-rudderstack' installed in the virtual environment",
                    "pre_check": lambda: os.path.exists(venv_pb),
                    "skip_message": f"Package 'profiles-rudderstack' already installed in the virtual environment at '{venv_pb}'",
                },
                {
                    "cmd": [venv_pip_to_use, "install", "profiles-mlcorelib"],
                    "desc": "Install 'profiles-mlcorelib' package using pip",
                    "success_message": "Package 'profiles-mlcorelib' installed in the virtual environment",
                    "pre_check": lambda: self._check_package_installed(
                        venv_bin_dir, "profiles_mlcorelib"
                    ),
                    "skip_message": f"Package 'profiles-mlcorelib' already installed in the virtual environment",
                },
            ]

            for item in commands_to_execute:
                pre_check = item.get("pre_check")
                logger.info(f"Pre-check: {pre_check}")
                if pre_check and pre_check():
                    logger.info(f"Pre-check passed for command: {item['cmd']}")
                    messages.append(
                        item.get("skip_message", "Step skipped due to pre-check.")
                    )
                    continue
                if not run_command(item["cmd"], abs_project_path, item["desc"]):
                    return {"status": "failure", "messages": messages, "errors": errors}
                if "success_message" in item:
                    messages.append(item["success_message"])

            readme_content = """# RudderStack Profiles Project

## Environment Setup

This project uses a Python virtual environment. To activate it, use one of the following methods:

### Using the standard Python venv activation:

```bash
source .venv/bin/activate
```

### Using uv (if installed):

```bash
uv activate .venv
```

Once activated, you can run Profiles commands with the `pb` CLI tool.

## Getting Started

After activating the environment, you can:

1. Initialize a new connection:

   ```
   pb init connection
   ```

   or

   use initialize_warehouse_connection() tool to create a new connection

2. Create your project configuration files (pb_project.yaml, inputs.yaml, models.yaml)

3. Run your profiles project:
   ```
   pb run
   ```

For more information, refer to the RudderStack Profiles documentation.
"""
            readme_writestatus_msg = (
                "Created README.md with environment activation instructions"
            )

        try:
            # Create README.md file with instructions
            with open(readme_path, "w") as f:
                f.write(readme_content)
            messages.append(readme_writestatus_msg)
        except Exception as e:
            errors.append(f"Error creating README.md file: {e}")

        return {
            "status": "success",
            "summary": "Project setup complete",
            "messages": messages,
            "errors": errors,
        }

    def _check_package_installed(self, venv_bin_dir: str, package_name: str) -> bool:
        """Check if a Python package is installed in the virtual environment by trying to import it."""
        python_path = os.path.join(venv_bin_dir, "python")
        try:
            # Try to import the package in the virtual environment
            cmd = [python_path, "-c", f"import {package_name}"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception:
            return False

    def _setup_cloud_based_project(self, project_path: str, messages: list) -> dict:
        """Setup project for kubernetes pod (no virtual environment needed)."""

        readme_content = """# RudderStack Profiles Project (Kubernetes Pod Environment)

## Environment Setup

This project is configured for a kubernetes pod environment where Python packages are pre-installed.

### Running in Kubernetes Pod Environment

Since you're running in a kubernetes pod, the required Python packages 
(profiles-rudderstack, profiles-mlcorelib) should already be available in your container.

## Getting Started

You can directly start using Profiles commands with the `pb` CLI tool:

1. Initialize a new connection:

   ```
   pb init connection
   ```

   or

   use initialize_warehouse_connection() tool to create a new connection

2. Create your project configuration files (pb_project.yaml, inputs.yaml, models.yaml)

3. Run your profiles project:
   ```
   pb run
   ```

For more information, refer to the RudderStack Profiles documentation.
"""

        return {
            "readme_content": readme_content,
            "message": "Created README.md with cloud environment instructions",
        }

    def workflow_guide(
        self,
        user_goal: str,
        current_action: str = "start",
        user_confirmed_tables: str = "",
        user_confirmed_connection: str = "",
        knowledge_phase_completed: str = "",
    ) -> dict:
        """
        **MANDATORY FIRST TOOL**: Your complete workflow guide for profiles projects.
        Provides task recommendations, step-by-step guidance, and validation all in one place.

        CRITICAL: This should be the FIRST tool called for any profiles-related task.

        Args:
            user_goal: What you want to accomplish (e.g., "build customer profiles", "create features")
            current_action: What you're about to do or current step:
                           - "start" (just beginning)
                           - "knowledge_gathering" (learning about profiles concepts)
                           - "discover_resources" (finding tables and connections)
                           - "create_inputs_yaml"
                           - "create_models_yaml"
                           - "create_entity_vars"
                           - "add_date_filtering"
                           - "run_pilot_test"
                           - "create_propensity_model"
            user_confirmed_tables: REQUIRED for create_* actions. Comma-separated list of table names that USER has confirmed
            user_confirmed_connection: REQUIRED for create_* actions. Connection name that USER has confirmed
            knowledge_phase_completed: REQUIRED for create_* actions. Comma-separated list of about_* tools completed ("profiles,inputs,models,macros")

        Returns:
            dict: Complete workflow guidance including next tools, validation, and warnings
        """
        current_year = datetime.datetime.now().year
        # Initialize base guide structure
        guide = self._initialize_guide_structure(
            user_goal, current_action, current_year
        )

        # Validate knowledge gathering phase completion for configuration actions
        config_validation = self._validate_knowledge_phase(
            current_action, knowledge_phase_completed
        )
        if not config_validation["valid"]:
            return self._merge_validation_results(guide, config_validation)

        # Validate user confirmations
        user_validation = self._validate_user_confirmations(
            current_action, user_confirmed_tables, user_confirmed_connection
        )
        if not user_validation["valid"]:
            return self._merge_validation_results(guide, user_validation)

        # Route to specific action handler
        action_handlers = {
            "start": self._handle_start_action,
            "knowledge_gathering": self._handle_knowledge_gathering_action,
            "discover_resources": self._handle_discover_resources_action,
            "create_inputs_yaml": self._handle_create_inputs_yaml_action,
            "create_models_yaml": self._handle_create_models_yaml_action,
            "create_entity_vars": self._handle_create_entity_vars_action,
            "add_date_filtering": self._handle_add_date_filtering_action,
            "run_pilot_test": self._handle_run_pilot_test_action,
            "create_propensity_model": self._handle_create_propensity_model_action,
            "analyze_existing_project": self._handle_analyze_existing_project_action,
        }

        handler = action_handlers.get(current_action)
        if handler:
            return handler(
                guide,
                user_goal,
                user_confirmed_tables,
                user_confirmed_connection,
                knowledge_phase_completed,
            )
        else:
            return self._handle_unknown_action(guide, current_action)

    def _initialize_guide_structure(
        self, user_goal: str, current_action: str, current_year: int
    ) -> dict:
        """Initialize the base guide structure with common elements."""
        return {
            "user_goal": user_goal,
            "current_action": current_action,
            "next_tools": [],
            "workflow_steps": [],
            "validation_status": "pending",
            "critical_warnings": self._get_base_critical_warnings(current_year),
            "examples": [],
            "status": "in_progress",
            "blocked_reasons": [],
        }

    def _merge_validation_results(self, guide: dict, validation: dict) -> dict:
        """Merge validation results into the guide structure."""
        guide["validation_status"] = validation["validation_status"]
        guide["blocked_reasons"] = validation["blocked_reasons"]
        guide["critical_warnings"].extend(validation["critical_warnings"])
        guide["next_tools"] = validation["next_tools"]
        guide["workflow_steps"] = validation["workflow_steps"]
        return guide

    def _handle_start_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'start' action."""
        guide["validation_status"] = "APPROVED"
        guide["next_tools"] = ["about_profiles"]
        guide["workflow_steps"] = [
            "1. MANDATORY: Call about_profiles(topic='profiles') to understand project structure and concepts",
            "2. NEXT: Call workflow_guide() with current_action='knowledge_gathering'",
        ]
        guide["critical_warnings"].extend(
            [
                "🚨 STEP 1: MUST learn about profiles concepts before any configuration",
                "🚨 DO NOT skip to table discovery or YAML creation",
                "🚨 Follow the mandatory sequence: knowledge → discovery → configuration",
            ]
        )
        return guide

    def _handle_knowledge_gathering_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'knowledge_gathering' action."""
        guide["validation_status"] = "APPROVED"
        task_lower = user_goal.lower()

        if any(word in task_lower for word in ["inputs", "yaml", "configuration"]):
            guide["workflow_steps"] = [
                "1. MANDATORY: Call about_profiles(topic='inputs') to understand input configuration",
                "2. MANDATORY: Call about_profiles(topic='models') to understand model configuration",
                "3. MANDATORY: Call about_profiles(topic='macros') to understand reusable code blocks",
                "4. NEXT: Call workflow_guide() with current_action='discover_resources'",
            ]
        else:
            guide["workflow_steps"] = [
                "1. MANDATORY: Call about_profiles(topic='inputs') to understand input configuration",
                "2. MANDATORY: Call about_profiles(topic='models') to understand model configuration",
                "3. NEXT: Call workflow_guide() with current_action='discover_resources'",
            ]

        guide["next_tools"] = ["about_profiles"]
        guide["critical_warnings"].extend(
            [
                "🚨 COMPLETE ALL about_profiles(topic='...') tool calls before proceeding",
                "🚨 These calls provide essential syntax and examples",
                "🚨 DO NOT create YAML without this knowledge",
            ]
        )
        return guide

    def _handle_discover_resources_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'discover_resources' action."""
        task_lower = user_goal.lower()

        if any(
            word in task_lower
            for word in ["new", "setup", "initialize", "create project"]
        ):
            guide["next_tools"] = [
                "setup_new_profiles_project",
                "get_existing_connections",
                "input_table_suggestions",
            ]
            guide["workflow_steps"] = [
                "1. Set up project infrastructure with setup_new_profiles_project()",
                "2. Discover available connections with get_existing_connections()",
                "3. PRESENT connection options to user and get their choice",
                "4. Discover available tables with input_table_suggestions()",
                "5. PRESENT table options to user and get their confirmation",
                "6. Examine user-confirmed table structures with describe_table()",
                "7. NEXT: Call workflow_guide() with current_action='create_inputs_yaml' and confirmed details",
            ]
        else:
            guide["next_tools"] = [
                "get_existing_connections",
                "input_table_suggestions",
                "describe_table",
            ]
            guide["workflow_steps"] = [
                "1. Discover available connections with get_existing_connections()",
                "2. PRESENT connection options to user and get their choice",
                "3. Discover available tables with input_table_suggestions()",
                "4. PRESENT table options to user and get their confirmation",
                "5. Examine user-confirmed table structures with describe_table()",
                "6. Use run_query() to understand data patterns",
                "7. NEXT: Call workflow_guide() with current_action='create_inputs_yaml' and confirmed details",
            ]

        guide["validation_status"] = "APPROVED"
        guide["critical_warnings"].extend(
            [
                "⚠️ MUST present discovery results to user and get their confirmation",
                "⚠️ MUST get actual table names and connection names from user",
                "⚠️ DO NOT proceed with placeholder names",
            ]
        )
        return guide

    def _handle_create_inputs_yaml_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'create_inputs_yaml' action."""
        guide["validation_status"] = "APPROVED"
        guide["next_tools"] = ["describe_table", "run_query", "search_profiles_docs"]
        guide["workflow_steps"] = [
            f"1. Use describe_table() for each user-confirmed table: {user_confirmed_tables}",
            "2. PRESENT column information to user and ask which columns to use for IDs",
            "3. WAIT for user confirmation of column names",
            "4. Run 'pb validate access' to verify connection and permissions work properly",
            "5. Use run_query() to examine sample data from confirmed tables",
            "6. Use search_profiles_docs(query='inputs yaml examples')",
            f"7. Create inputs.yaml using connection: {user_confirmed_connection}",
            f"8. Create inputs.yaml using tables: {user_confirmed_tables}",
            "9. PRESENT final inputs.yaml to user for approval before proceeding",
            "10. NO WHERE clauses with dates in inputs.yaml",
        ]
        guide["critical_warnings"].extend(
            [
                f"✅ USER CONFIRMED: Using connection '{user_confirmed_connection}'",
                f"✅ USER CONFIRMED: Using tables '{user_confirmed_tables}'",
                "⚠️ STILL NEED: User confirmation of column names for IDs and timestamps",
                "⚠️ VALIDATE: Run 'pb validate access' to ensure connection works",
                "⚠️ FINAL STEP: Present completed inputs.yaml to user for approval",
            ]
        )
        return guide

    def _handle_create_models_yaml_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'create_models_yaml' action."""
        guide["validation_status"] = "APPROVED"
        guide["next_tools"] = ["search_profiles_docs", "run_query"]
        guide["workflow_steps"] = [
            "1. Use search_profiles_docs(query='entity variables examples')",
            "2. Use search_profiles_docs(query='entity variables best practices')",
            f"3. Use run_query() on user-confirmed tables: {user_confirmed_tables}",
            "4. PRESENT feature suggestions to user and get their confirmation",
            "5. Use simple aggregations: count(), sum(), max(), min(), avg()",
            "6. Avoid complex window functions unless user specifically requests them",
            "7. Use macros for date calculations. For examples, refer to the about_profiles(topic='macros') tool",
            "8. PRESENT final models.yaml to user for approval before proceeding",
        ]
        guide["critical_warnings"].extend(
            [
                f"✅ USER CONFIRMED: Using tables '{user_confirmed_tables}'",
                "⚠️ STILL NEED: User confirmation of which features to create",
                "⚠️ entity_var with 'from' MUST use aggregation functions",
                "⚠️ FINAL STEP: Present completed models.yaml to user for approval",
            ]
        )
        return guide

    def _handle_create_entity_vars_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'create_entity_vars' action."""
        guide["validation_status"] = "APPROVED"
        guide["next_tools"] = ["describe_table", "run_query", "search_profiles_docs"]
        guide["workflow_steps"] = [
            f"1. Use describe_table() to verify columns in: {user_confirmed_tables}",
            f"2. Use run_query() to examine sample data from: {user_confirmed_tables}",
            "3. Use search_profiles_docs() to find similar examples",
            "4. PRESENT feature options to user and get their confirmation",
            "5. Prefer simple aggregations over window functions",
            "6. Use macros for date calculations. For examples, refer to the about_profiles(topic='macros') tool",
            "7. PRESENT final entity_vars to user for approval",
        ]
        guide["critical_warnings"].extend(
            [
                f"✅ USER CONFIRMED: Using tables '{user_confirmed_tables}'",
                "⚠️ STILL NEED: User confirmation of which entity variables to create",
                "⚠️ Avoid complex window functions unless user specifically requests them",
                "⚠️ entity_var with 'from' key MUST use aggregation in select",
            ]
        )
        guide["examples"] = [
            "count(distinct session_id)",
            "sum(order_amount)",
            "max(timestamp)",
            "{{macro_datediff('min(created_at)')}}",
        ]
        return guide

    def _handle_add_date_filtering_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'add_date_filtering' action."""
        import datetime

        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().month

        guide["validation_status"] = "ERROR"
        guide["critical_warnings"].extend(
            [
                "❌ NEVER add WHERE clauses with dates in inputs.yaml or at the top level of models.yaml to filter recent data for test/dry runs.",
                "✅ Use 'pb run --begin_time' flag for project-level date filtering (e.g., for test/dry runs).",
                "⚠️ For time-based features (e.g., days_since_last_seen, is_active_last_week), you MUST use date filters in entity_vars, but ONLY via the provided macros.",
                "🔗 See about_profiles(topic='datediff-entity-vars') for correct usage of date macros in entity_vars.",
            ]
        )
        guide["examples"] = [
            f"pb run --begin_time '{current_year-1}-{current_month:02d}-01T00:00:00Z'",
            "# NOT in YAML: where: \"timestamp >= '2024-01-01'\"  ← WRONG!",
            "# IN ENTITY VARS: where: \"{{macro_datediff_n('timestamp','30')}}\"  ← CORRECT! (see about_profiles(topic='datediff-entity-vars'))",
        ]
        guide["next_tools"] = ["about_profiles"]
        return guide

    def _handle_run_pilot_test_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'run_pilot_test' action."""
        import datetime

        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().month

        guide["validation_status"] = "APPROVED"
        guide["next_tools"] = ["about_profiles"]
        guide["workflow_steps"] = [
            "1. Run 'pb compile' first to check generated SQL",
            f"2. Use 'pb run --begin_time' with {current_year} date for pilot",
            "3. Use 'pb run --concurrency 10' for faster runs in Snowflake",
            "4. Check output tables after successful run",
        ]
        guide["examples"] = [
            "pb compile",
            f"pb run --begin_time '{current_year-1}-{(current_month-2) % 12 + 1:02d}-01T00:00:00Z' --concurrency 10",
        ]
        return guide

    def _handle_create_propensity_model_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'create_propensity_model' action."""
        guide["validation_status"] = "APPROVED"
        guide["next_tools"] = ["about_profiles", "search_profiles_docs"]
        guide["workflow_steps"] = [
            "1. Use about_profiles(topic='propensity') for detailed guidance",
            "2. Ensure binary label (0/1) exists in your features",
            "3. Use search_profiles_docs() to find propensity examples",
            "4. PRESENT propensity model configuration to user for approval",
            "5. Validate training features exist before configuration",
            "6. Use macros for date calculations. For examples, refer to the about_profiles(topic='macros') tool",
            "7. Validate propensity model configuration with validate_propensity_model_config()",
        ]
        return guide

    def _handle_analyze_existing_project_action(
        self,
        guide: dict,
        user_goal: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
        knowledge_phase_completed: str,
    ) -> dict:
        """Handle the 'analyze_existing_project' action."""
        # Check if basic profiles knowledge is acquired (simplified requirement)
        completed_knowledge = [
            k.strip().lower() for k in knowledge_phase_completed.split(",") if k.strip()
        ]
        required_knowledge = ["profiles"]  # Only require basic profiles understanding
        missing_knowledge = [
            k for k in required_knowledge if k not in completed_knowledge
        ]

        if missing_knowledge:
            guide["validation_status"] = "BLOCKED"
            guide["blocked_reasons"].append(
                "❌ BASIC KNOWLEDGE REQUIRED: Must understand profiles concepts to interpret analysis results"
            )
            guide["critical_warnings"].append(
                "🚨 You MUST call about_profiles() first to understand project structure concepts"
            )
            guide["critical_warnings"].append(
                "🚨 This knowledge is essential for explaining analysis results to users"
            )
            guide["next_tools"] = ["about_profiles"]
            guide["workflow_steps"] = [
                "1. MANDATORY: Call about_profiles(topic='profiles') to understand project structure and concepts",
                "2. Call workflow_guide() again with knowledge_phase_completed='profiles'",
            ]
            return guide

        guide["validation_status"] = "APPROVED"
        guide["next_tools"] = ["analyze_and_validate_project"]
        guide["workflow_steps"] = [
            "1. Use analyze_and_validate_project() with the project path provided by user",
            "2. EXAMINE the project structure and YAML validation results",
            "3. IDENTIFY any configuration issues, missing files, or validation errors",
            "4. EXPLAIN findings in user-friendly terms using your profiles knowledge",
            "5. PRESENT actionable recommendations based on the analysis",
            "6. If errors found, suggest specific fixes or next debugging steps",
            "7. Optional: Use search_profiles_docs() for configuration examples if issues found",
        ]
        guide["critical_warnings"] = [
            "✅ OFFLINE MODE: No warehouse access required for this analysis",
            "✅ SIMPLIFIED knowledge requirement: only basic profiles understanding needed",
            "⚠️ This tool only analyzes project structure and YAML syntax - no runtime validation",
            "⚠️ For runtime errors, check log files in logs/pb.log or output directories",
            "💡 Use this for CSM support scenarios and project debugging",
            "🧠 Use your profiles knowledge to explain technical findings in user-friendly terms",
        ]
        guide["examples"] = [
            "analyze_and_validate_project('/path/to/customer-project')",
            "# Examines pb_project.yaml, inputs/models files, logs, outputs",
            "# Returns structure analysis + YAML validation results",
            "# Explain findings: 'Your project has X inputs configured, Y entity types, found Z warnings...'",
        ]
        return guide

    def _handle_unknown_action(self, guide: dict, current_action: str) -> dict:
        """Handle unknown actions."""
        supported_actions = [
            "start",
            "knowledge_gathering",
            "discover_resources",
            "create_inputs_yaml",
            "create_models_yaml",
            "create_entity_vars",
            "add_date_filtering",
            "run_pilot_test",
            "create_propensity_model",
            "analyze_existing_project",
        ]

        guide["validation_status"] = "ERROR"
        guide["critical_warnings"].extend(
            [
                f"❌ Unknown action: '{current_action}'",
                f"✅ Supported actions: {', '.join(supported_actions)}",
                "💡 Use 'start' if you're just beginning your profiles project",
            ]
        )
        guide["next_tools"] = ["about_profiles", "search_profiles_docs"]
        return guide

    def _get_base_critical_warnings(self, current_year: int) -> list[str]:
        """Get base critical warnings that apply to all workflow actions."""
        return [
            f"🚨 CRITICAL: Current year is {current_year}, NOT 2024!",
            "🚨 MANDATORY: Call about_profiles(topic='inputs'), about_profiles(topic='models'), about_profiles(topic='macros') BEFORE creating any YAML",
            "🚨 NEVER add WHERE clauses with dates in YAML to filter old data from all input tabels - use --begin_time flag. This is different from where clause in entity-vars, which is acceptable while using timestamp macros",
            "🚨 NEVER assume column names exist - always use describe_table() first",
            "🚨 PREFER simple aggregations over complex window functions",
            "🚨 ALWAYS use search_profiles_docs() tools for examples",
            "🚨 NEVER make autonomous decisions about using input tables and connections - ALWAYS get user confirmation for tables and connections",
            "🚨 NEVER CREATE YAML WITHOUT COMPLETING KNOWLEDGE GATHERING PHASE FIRST",
            "🚨 ALWAYS USE datediff macros to create entity-vars with timestamp filters. NEVER use current_timestamp(), datediff etc directly. Use only through the macros",
        ]

    def _validate_knowledge_phase(
        self, current_action: str, knowledge_phase_completed: str
    ) -> dict:
        """Validate that required knowledge phase is completed for configuration actions."""
        config_actions = [
            "create_inputs_yaml",
            "create_models_yaml",
            "create_entity_vars",
        ]

        if current_action not in config_actions:
            return {"valid": True}

        required_knowledge = {
            "create_inputs_yaml": ["profiles", "inputs"],
            "create_models_yaml": ["profiles", "inputs", "models", "macros"],
            "create_entity_vars": ["profiles", "models", "macros"],
        }

        completed_knowledge = [
            k.strip().lower() for k in knowledge_phase_completed.split(",") if k.strip()
        ]
        required_for_action = required_knowledge.get(current_action, [])
        missing_knowledge = [
            k for k in required_for_action if k not in completed_knowledge
        ]

        if missing_knowledge:
            next_tools_calls = []
            for topic in missing_knowledge:
                if topic == "profiles":
                    next_tools_calls.append("about_profiles(topic='profiles')")
                else:
                    next_tools_calls.append(f"about_profiles(topic='{topic}')")

            return {
                "valid": False,
                "validation_status": "BLOCKED",
                "blocked_reasons": [
                    f"❌ KNOWLEDGE PHASE INCOMPLETE: Missing {missing_knowledge}"
                ],
                "critical_warnings": [
                    f"🚨 You MUST call about_profiles() for missing topics: {', '.join(missing_knowledge)}",
                    "🚨 NEVER create YAML configurations without understanding the concepts first",
                ],
                "next_tools": ["about_profiles"],
                "workflow_steps": [
                    f"1. MANDATORY: Call {tool_call} to understand concepts"
                    for tool_call in next_tools_calls
                ]
                + [
                    f"{len(missing_knowledge) + 1}. Call workflow_guide() again with knowledge_phase_completed='{','.join(required_for_action)}'"
                ],
            }

        return {"valid": True}

    def _validate_user_confirmations(
        self,
        current_action: str,
        user_confirmed_tables: str,
        user_confirmed_connection: str,
    ) -> dict:
        """Validate that user has confirmed required tables and connections."""
        config_actions = [
            "create_inputs_yaml",
            "create_models_yaml",
            "create_entity_vars",
        ]

        if current_action not in config_actions:
            return {"valid": True}

        # Detection of fake/generic names using optimized set intersection
        def detect_fake_names(text):
            text_lower = text.lower()
            # Use set intersection for O(1) average case lookup
            return any(pattern in text_lower for pattern in self.FAKE_NAME_PATTERNS)

        # Check for missing user confirmed data
        if not user_confirmed_tables.strip():
            return {
                "valid": False,
                "validation_status": "BLOCKED",
                "blocked_reasons": [
                    "❌ MISSING: user_confirmed_tables parameter required"
                ],
                "critical_warnings": [
                    "🚨 You MUST get user confirmation of which tables to use",
                    "🚨 Present table suggestions and ask user to choose specific tables",
                ],
                "next_tools": ["input_table_suggestions", "describe_table"],
                "workflow_steps": [
                    "1. Run input_table_suggestions() to find available tables",
                    "2. PRESENT the list to user: 'I found these tables: [list]. Which would you like to use?'",
                    "3. WAIT for user to specify exact tables they want",
                    "4. Call this tool again with user_confirmed_tables parameter",
                ],
            }

        if not user_confirmed_connection.strip():
            return {
                "valid": False,
                "validation_status": "BLOCKED",
                "blocked_reasons": [
                    "❌ MISSING: user_confirmed_connection parameter required"
                ],
                "critical_warnings": [
                    "🚨 You MUST get user confirmation of which connection to use",
                    "🚨 Present connection options and ask user to choose",
                ],
                "next_tools": ["get_existing_connections"],
                "workflow_steps": [
                    "1. Run get_existing_connections() to find available connections",
                    "2. PRESENT the list to user: 'I found these connections: [list]. Which would you like to use?'",
                    "3. WAIT for user to specify exact connection they want",
                    "4. Call this tool again with user_confirmed_connection parameter",
                ],
            }

        # Check for placeholder/fake names
        if detect_fake_names(user_confirmed_tables):
            return {
                "valid": False,
                "validation_status": "BLOCKED",
                "blocked_reasons": ["❌ PLACEHOLDER NAMES DETECTED"],
                "critical_warnings": [
                    "🚨 user_confirmed_tables still contains placeholder text",
                    "🚨 Must be actual table names the user has confirmed, like: 'PROD_DB.ANALYTICS.EVENTS,PROD_DB.CRM.USERS'",
                ],
                "next_tools": ["input_table_suggestions", "describe_table"],
                "workflow_steps": [
                    "1. Run input_table_suggestions() to find available tables",
                    "2. PRESENT the list to user and ask them to choose specific table names",
                    "3. WAIT for user to confirm actual table names (not placeholders)",
                    "4. Call this tool again with real user_confirmed_tables",
                ],
            }

        if detect_fake_names(user_confirmed_connection):
            return {
                "valid": False,
                "validation_status": "BLOCKED",
                "blocked_reasons": ["❌ PLACEHOLDER CONNECTION NAME DETECTED"],
                "critical_warnings": [
                    "🚨 user_confirmed_connection still contains placeholder text",
                    "🚨 Must be actual connection name the user has confirmed, like: 'snowflake_prod_warehouse' or 'bigquery_staging_warehouse'",
                ],
                "next_tools": ["get_existing_connections"],
                "workflow_steps": [
                    "1. Run get_existing_connections() to find available connections",
                    "2. PRESENT the list to user and ask them to choose a specific connection",
                    "3. WAIT for user to confirm actual connection name (not placeholder)",
                    "4. Call this tool again with real user_confirmed_connection",
                ],
            }

        return {"valid": True}

    def validate_propensity_model_config(
        self, project_path: str, model_name: str, warehouse_client
    ) -> dict:
        """
        Validates propensity model configuration for common pitfalls.

        - Checks if input tables have occurred_at_col defined
        - Validates features aren't sourced from static tables
        - Analyzes date ranges for historic data availability

        Args:
            project_path: Path to the profiles project directory
            model_name: Name of the propensity model to validate
            warehouse_client: Warehouse client for data validation queries

        Returns:
            dict: Structured validation results with errors, warnings, and suggestions
        """
        validator = PropensityValidator(project_path, model_name, warehouse_client)
        return validator.validate()

    def fetch_warehouse_credentials(self, connection_name: str) -> dict:
        """
        Securely fetch warehouse connection credentials from siteconfig.yaml.
        Supports multiple warehouse types: Snowflake, BigQuery, etc.

        Args:
            connection_name: Name of the connection to fetch credentials for

        Returns:
            dict: Connection details with credentials or error status
        """
        try:
            with open(PB_SITE_CONFIG_PATH, "r") as file:
                config = yaml.safe_load(file) or {}

            if "connections" not in config:
                return {
                    "status": "error",
                    "message": f"No connections found in siteconfig.yaml",
                }

            if connection_name not in config["connections"]:
                return {
                    "status": "error",
                    "message": f"Connection '{connection_name}' not found in siteconfig.yaml",
                }

            logger.info(f"Fetching credentials for connection '{connection_name}'")
            existing_connection = config["connections"][connection_name]
            target = existing_connection.get("target", "dev")
            output_config = existing_connection["outputs"][target]

            warehouse_type = output_config.get("type", "unknown").lower()

            # Base connection details common to all warehouse types
            connection_details = {
                "target": target,
                "type": warehouse_type,
                "user": output_config.get("user"),
            }

            # Add warehouse-specific details
            if warehouse_type == "snowflake":
                connection_details.update(
                    {
                        "account": output_config["account"],
                        "warehouse": output_config.get("warehouse"),
                        "database": output_config.get("dbname"),
                        "schema": output_config.get("schema"),
                        "role": output_config.get("role"),
                        "password": output_config.get("password"),
                        "private_key": output_config.get(
                            "privateKey"
                        ),  # Note: siteconfig uses "privateKey"
                        "private_key_file": output_config.get("privateKeyFile"),
                        "private_key_passphrase": output_config.get(
                            "privateKeyPassphrase"
                        ),  # Note: siteconfig uses "privateKeyPassphrase"
                    }
                )
            elif warehouse_type == "bigquery":
                connection_details.update(
                    {
                        "project_id": output_config.get("project_id"),
                        "credentials": output_config.get(
                            "credentials"
                        ),  # The parsed service account details
                        "location": output_config.get("location", "US"),
                        "dataset": output_config.get("dataset")
                        or output_config.get("schema"),
                    }
                )
            else:
                # For future warehouse types, we can add support here
                logger.warning(
                    f"Warehouse type '{warehouse_type}' support may be limited"
                )
                # Include all available configuration for extensibility
                connection_details.update(output_config)

            return {
                "status": "success",
                "connection_name": connection_name,
                "connection_details": connection_details,
            }

        except FileNotFoundError:
            return {
                "status": "error",
                "message": f"siteconfig.yaml file not found at {PB_SITE_CONFIG_PATH}",
            }
        except Exception as e:
            logger.error(f"Error fetching warehouse credentials: {e}")
            return {
                "status": "error",
                "message": f"Error fetching warehouse credentials: {str(e)}",
            }


class PropensityValidator:
    """
    Validates propensity model configurations for common pitfalls.

    This class provides a clean, modular approach to validating propensity models
    with clear separation of concerns and extensible validation rules.
    """

    def __init__(self, project_path: str, model_name: str, warehouse_client):
        """Initialize the validator with warehouse client."""
        self.project_path = project_path
        self.model_name = model_name
        self.warehouse_client = warehouse_client

    def validate(self) -> dict:
        """
        Main validation entry point for propensity models.

        Args:
            project_path: Path to the profiles project directory
            model_name: Name of the propensity model to validate
            warehouse_client: Warehouse client for data validation queries

        Returns:
            dict: Structured validation results with errors, warnings, and suggestions
        """
        logger.info(f"Validating propensity model: {self.model_name}")

        try:
            self._initialize_result()
            self._initialize_configs()

            if not self._validate_model_spec():
                return self.result

            input_tables_map = self._create_input_tables_map(self.configs["inputs"])
            entity_vars_map = self._create_entity_vars_map(self.configs["models"])

            if self._has_complex_dependencies(entity_vars_map):
                self._validate_all_input_tables_fallback(input_tables_map)
            else:
                self._validate_model_inputs(input_tables_map, entity_vars_map)

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

    def _validate_model_spec(self) -> bool:
        """Validate that the propensity model exists."""
        if not self.propensity_model:
            self.result["errors"].append(
                {
                    "type": "MODEL_NOT_FOUND",
                    "message": f"Propensity model '{self.model_name}' not found in models configuration",
                    "remediation": "Verify the model name exists in your models.yaml file",
                }
            )
            self.result["validation_status"] = "FAILED"
            return False

        model_spec = self.propensity_model.get("model_spec")
        if not model_spec:
            self.result["errors"].append(
                {
                    "type": "MODEL_SPEC_NOT_FOUND",
                    "message": f"Propensity model '{self.model_name}' has no model_spec defined",
                    "remediation": "Add a model_spec to the model configuration",
                }
            )
            self.result["validation_status"] = "FAILED"
            return False

        predict_window_days = model_spec.get("training", {}).get("predict_window_days")
        if not predict_window_days:
            self.result["errors"].append(
                {
                    "type": "PREDICT_WINDOW_DAYS_NOT_FOUND",
                    "message": f"Propensity model '{self.model_name}' has no predict_window_days defined",
                    "remediation": "Add a predict_window_days to the model_spec",
                }
            )
            self.result["validation_status"] = "FAILED"
            return False

        if predict_window_days <= 0:
            self.result["errors"].append(
                {
                    "type": "PREDICT_WINDOW_DAYS_NOT_POSITIVE",
                    "message": f"Propensity model '{self.model_name}' has a non-positive predict_window_days: {predict_window_days}",
                    "remediation": "Set predict_window_days to a positive integer",
                }
            )
            self.result["validation_status"] = "FAILED"
            return False

        model_inputs = model_spec.get("inputs")
        if not model_inputs:
            self.result["errors"].append(
                {
                    "type": "NO_INPUTS_DEFINED",
                    "message": f"Propensity model '{self.model_name}' has no inputs defined",
                    "remediation": "Add input features to the model_spec.inputs section",
                }
            )
            self.result["validation_status"] = "FAILED"
            return False

        entity_vars_map = self._create_entity_vars_map(self.configs["models"])

        for input_feature in model_inputs:
            feature_info = self._parse_feature_reference(input_feature)
            if not feature_info:
                self.result["errors"].append(
                    {
                        "type": "INVALID_FEATURE_REFERENCE",
                        "feature": input_feature,
                        "message": f"Could not parse feature reference: {input_feature}",
                        "remediation": "Ensure feature follows format: entity/entity_name/feature_name",
                    }
                )

            entity_var = entity_vars_map.get(feature_info["feature_name"])
            if not entity_var:
                self.result["errors"].append(
                    {
                        "type": "NO_ENTITY_VAR_DEFINED",
                        "feature": input_feature,
                        "message": f"Entity variable not found for feature: {input_feature}",
                        "remediation": "Ensure the entity variable is defined in your models.yaml file",
                    }
                )

            if entity_var.get("is_feature") == False:
                self.result["errors"].append(
                    {
                        "type": "INVALID_ENTITY_VAR_TYPE",
                        "feature": input_feature,
                        "message": f"Entity variable '{feature_info['feature_name']}' is not a feature",
                        "remediation": "Ensure the entity variable is a feature",
                    }
                )

        if len(self.result["errors"]) > 0:
            self.result["validation_status"] = "FAILED"
            return False

        return True

    def _validate_model_inputs(
        self, input_tables_map: dict, entity_vars_map: dict
    ) -> bool:
        """Validate all input features for the propensity model."""
        model_inputs = self.propensity_model.get("model_spec").get("inputs")

        for input_feature in model_inputs:
            logger.info(f"Validating input feature: {input_feature}")
            self._validate_single_feature(
                input_feature, input_tables_map, entity_vars_map
            )
        return True

    def _validate_single_feature(
        self, input_feature: str, input_tables_map: dict, entity_vars_map: dict
    ) -> None:
        """Validate a single input feature."""
        feature_info = self._parse_feature_reference(input_feature)
        entity_var = entity_vars_map.get(feature_info["feature_name"])
        if entity_var:
            self._validate_entity_var(entity_var, input_tables_map)
        else:
            self.result["warnings"].append(
                {
                    "type": "FEATURE_NOT_FOUND",
                    "feature": input_feature,
                    "message": f"Entity variable '{feature_info['feature_name']}' not found in models configuration",
                    "remediation": "Ensure the entity variable is defined in your models.yaml file",
                }
            )

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

    def _parse_feature_reference(self, feature_ref: str) -> dict:
        """Parse feature reference like 'entity/user/feature_name'."""
        parts = feature_ref.split("/")
        if len(parts) == 3 and parts[0] == "entity":
            return {
                "entity_type": parts[0],
                "entity_name": parts[1],
                "feature_name": parts[2],
            }
        return None

    def _validate_entity_var(self, entity_var: dict, input_tables_map: dict) -> None:
        """Validate an entity variable feature for propensity model usage."""
        feature_name = entity_var.get("name")
        from_table = entity_var.get("from")

        if not from_table:
            # Feature doesn't reference an input table directly
            return

        table_ref = self._parse_table_reference(from_table)
        if not table_ref:
            self.result["warnings"].append(
                {
                    "type": "INVALID_TABLE_REFERENCE",
                    "feature": feature_name,
                    "table_reference": from_table,
                    "message": f"Could not parse table reference: {from_table}",
                    "remediation": "Ensure table reference follows format: inputs/table_name",
                }
            )
            return

        input_table_config = input_tables_map.get(table_ref["table_name"])
        if not input_table_config:
            self.result["warnings"].append(
                {
                    "type": "INPUT_TABLE_NOT_FOUND",
                    "feature": feature_name,
                    "table": table_ref["table_name"],
                    "message": f"Input table '{table_ref['table_name']}' not found in inputs.yaml",
                    "remediation": "Add the table to inputs.yaml or remove features derived from this table",
                }
            )
            return

        # Perform the main validations
        self._validate_occurred_at_col(input_table_config, feature_name)
        self._validate_historic_data(input_table_config, feature_name)

    def _parse_table_reference(self, table_ref: str) -> dict:
        """Parse table reference like 'inputs/table_name'."""
        parts = table_ref.split("/")
        if len(parts) == 2 and parts[0] == "inputs":
            return {"source_type": parts[0], "table_name": parts[1]}
        return None

    def _validate_occurred_at_col(
        self,
        input_table_config: dict,
        feature_name: str = None,
        is_fallback: bool = False,
    ) -> None:
        """
        Validate that input table has occurred_at_col defined.

        Args:
            input_table_config: Configuration of the input table
            feature_name: Name of the feature (for direct validation) or None (for fallback)
            is_fallback: If True, reports as warning; if False, reports as error
        """
        app_defaults = input_table_config.get("app_defaults", {})
        occurred_at_col = app_defaults.get("occurred_at_col")
        table_name = input_table_config.get("name")

        if not occurred_at_col:
            if is_fallback:
                self.result["warnings"].append(
                    {
                        "type": "MISSING_OCCURRED_AT_COL",
                        "table": table_name,
                        "message": f"Input table '{table_name}' lacks occurred_at_col definition",
                        "context": "This input table issue may not affect your propensity model if this table doesn't contribute to your model's feature pipeline",
                        "remediation": "If this table is used by your propensity model features, add occurred_at_col to the table's app_defaults in inputs.yaml",
                    }
                )
            else:
                self.result["errors"].append(
                    {
                        "type": "MISSING_OCCURRED_AT_COL",
                        "feature": feature_name,
                        "table": table_name,
                        "message": f"Input table '{table_name}' lacks occurred_at_col definition",
                        "remediation": "Add occurred_at_col to the table's app_defaults in inputs.yaml, or remove features from this table",
                    }
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

    def _validate_all_input_tables_fallback(self, input_tables_map: dict) -> None:
        """
        Fallback validation: Check all input tables for common issues.
        Used when entity variables have complex dependencies that can't be traced.
        Reports violations as warnings with explanatory context.
        """
        logger.info("Running fallback validation on all input tables")

        for _, input_table_config in input_tables_map.items():
            self._validate_occurred_at_col(
                input_table_config, feature_name=None, is_fallback=True
            )
            self._validate_historic_data(
                input_table_config, feature_name=None, is_fallback=True
            )

    def _has_complex_dependencies(self, entity_vars_map: dict) -> bool:
        """
        Check if an entity variable has complex dependencies that can't be easily traced.

        Returns True for:
        - No 'from' key (derived/calculated features)
        - from: models/modelName (SQL model dependencies)
        - Any other non-direct input reference
        """
        model_inputs = self.propensity_model.get("model_spec").get("inputs")

        for input_feature in model_inputs:
            feature_info = self._parse_feature_reference(input_feature)
            if not feature_info:
                continue

            entity_var = entity_vars_map.get(feature_info["feature_name"])
            if not entity_var:
                continue

            from_table = entity_var.get("from")

            if not from_table:
                logger.debug(f"Feature {input_feature} has no from table")
                return True

            table_ref = self._parse_table_reference(from_table)

            if not table_ref or table_ref.get("source_type") != "inputs":
                logger.debug(
                    f"Feature {input_feature} has a non-inputs table reference"
                )
                return True

        return False


class ProfilesUtils:
    def load_all_configs(self, project_path: str) -> dict:
        """Load all required configuration files."""
        project_config = self.load_project_config(project_path)
        inputs_config = self.load_inputs_config(project_path, project_config)
        models_config = self.load_models_config(project_path, project_config)

        return {
            "project": project_config,
            "inputs": inputs_config,
            "models": models_config,
        }

    def load_project_config(self, project_path: str) -> dict:
        """Load pb_project.yaml configuration."""
        pb_project_path = os.path.join(project_path, "pb_project.yaml")
        if not os.path.exists(pb_project_path):
            raise FileNotFoundError(f"pb_project.yaml not found at {pb_project_path}")

        with open(pb_project_path, "r") as file:
            return yaml.safe_load(file)

    def load_inputs_config(self, project_path: str, project_config: dict) -> dict:
        """Load inputs configuration from all YAML files in the models folder."""
        model_folders = project_config.get("model_folders", ["models"])
        models_folder = model_folders[0] if model_folders else "models"
        models_dir = os.path.join(project_path, models_folder)

        if not os.path.exists(models_dir):
            raise FileNotFoundError(f"Models directory not found at {models_dir}")

        combined_inputs = {"inputs": []}

        for filename in os.listdir(models_dir):
            if filename.endswith((".yaml", ".yml")):
                file_path = os.path.join(models_dir, filename)
                try:
                    with open(file_path, "r") as file:
                        config = yaml.safe_load(file)
                        if config and "inputs" in config:
                            combined_inputs["inputs"].extend(config["inputs"])
                except Exception as e:
                    logger.warning(f"Could not parse {filename}: {e}")

        if not combined_inputs["inputs"]:
            raise FileNotFoundError(
                f"No inputs configuration found in any YAML files in {models_dir}"
            )

        return combined_inputs

    def load_models_config(self, project_path: str, project_config: dict) -> dict:
        """Load models configuration from all YAML files in the models folder."""
        model_folders = project_config.get("model_folders", ["models"])
        models_folder = model_folders[0] if model_folders else "models"
        models_dir = os.path.join(project_path, models_folder)

        if not os.path.exists(models_dir):
            raise FileNotFoundError(f"Models directory not found at {models_dir}")

        combined_config = {"models": [], "var_groups": []}

        for filename in os.listdir(models_dir):
            if filename.endswith((".yaml", ".yml")):
                file_path = os.path.join(models_dir, filename)
                try:
                    with open(file_path, "r") as file:
                        config = yaml.safe_load(file)
                        if config:
                            if "models" in config:
                                combined_config["models"].extend(config["models"])
                            if "var_groups" in config:
                                combined_config["var_groups"].extend(
                                    config["var_groups"]
                                )
                except Exception as e:
                    logger.warning(f"Could not parse {filename}: {e}")

        if not combined_config["models"] and not combined_config["var_groups"]:
            raise FileNotFoundError(
                f"No models or var_groups configuration found in any YAML files in {models_dir}"
            )

        return combined_config

    def find_model(self, models_config: dict, model_name: str, model_type: str) -> dict:
        """Find the specific propensity model in the configuration."""
        models = models_config.get("models", [])
        for model in models:
            if (
                model.get("name") == model_name
                and model.get("model_type") == model_type
            ):
                return model
        return None
