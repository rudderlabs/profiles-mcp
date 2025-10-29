import os
import yaml
from logger import setup_logger

logger = setup_logger(__name__)


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
