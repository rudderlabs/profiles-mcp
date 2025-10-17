#!/usr/bin/env python3
"""
Unit tests for PropensityValidator.validate() method.

Tests the combined validation logic that uses both pb_models_data and YAML configs.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
from src.validators.propensity_validator import PropensityValidator
from src.utils.pb_models_parser import PBModelsData, Model, Materialization, FeatureData


class TestPropensityValidator:
    """Test the combined validation approach."""
    
    @pytest.fixture
    def mock_warehouse_client(self):
        """Create a mock warehouse client."""
        client = MagicMock()
        client.warehouse_type = "snowflake"
        return client
    
    @pytest.fixture
    def mock_yaml_configs(self):
        """Mock YAML configuration files."""
        return {
            "pb_project": {
                "name": "test_project",
                "model_folders": ["models"]
            },
            "inputs": {
                "inputs": [
                    {
                        "name": "events",
                        "app_defaults": {
                            "table": "test.events",
                            "occurred_at_col": "timestamp"
                        }
                    }
                ]
            },
            "profiles": {
                "models": [
                    {
                        "name": "churn_model",
                        "model_type": "propensity",
                        "model_spec": {
                            "training": {
                                "predict_window_days": 60
                            }
                        }
                    }
                ],
                "var_groups": []
            }
        }

    @pytest.fixture
    def mock_faulty_yaml_configs(self):
        """Mock YAML configuration files."""
        return {
            "pb_project": {
                "name": "test_project",
                "model_folders": ["models"]
            },
            "inputs": {
                "inputs": [
                    {
                        "name": "events",
                        "app_defaults": {
                            "table": "test.events",
                            "occurred_at_col": "timestamp"
                        }
                    }
                ]
            },
            "profiles": {
                "models": [
                    {
                        "name": "faulty_model",
                        "model_type": "propensity",
                        "model_spec": {
                            "training": {
                                "predict_window_days": 0
                            }
                        }
                    }
                ],
                "var_groups": []
            }
        }
    
    @pytest.fixture
    def sample_propensity_model(self):
        """Create a sample propensity model."""
        return Model(
            name="churn_model",
            display_name="churn_model",
            model_type="propensity",
            path_ref="models/churn_model",
            materialization=Materialization(
                output_type="table",
                run_type="discrete",
                sql_type="multi"
            ),
            warehouse_view_name="churn_model",
            is_feature=False,
            entity="user"
        )
    
    @pytest.fixture
    def sample_propensity_model_training(self):
        """Create a sample propensity model with dependencies."""
        return Model(
            name="churn_model_training",
            display_name="churn_model",
            model_type="training",
            path_ref="models/churn_model",
            materialization=Materialization(
                output_type="table",
                run_type="discrete",
                sql_type="multi"
            ),
            warehouse_view_name="churn_model",
            is_feature=False,
            entity="user",
            dependencies=["user/all/feature1", "user/all/feature2"]
        )
    
    @pytest.fixture
    def sample_propensity_model_prediction(self):
        """Create a sample propensity model with dependencies."""
        return Model(
            name="churn_model_prediction",
            display_name="churn_model",
            model_type="prediction",
            path_ref="models/churn_model",
            materialization=Materialization(
                output_type="table",
                run_type="discrete",
                sql_type="multi"
            ),
            warehouse_view_name="churn_model",
            is_feature=False,
            entity="user",
            dependencies=["user/all/feature1", "user/all/feature2"]
        )
    
    @pytest.fixture
    def sample_feature_with_time_function(self):
        """Create a feature with time function in yaml."""
        return Model(
            name="feature1",
            display_name="feature1",
            model_type="entity_var_item",
            path_ref="user/all/feature1",
            materialization=Materialization(
                output_type="column",
                run_type="discrete",
                sql_type="multi"
            ),
            warehouse_view_name="feature1",
            dependencies=["inputs/events"],
            is_feature=True,
            feature_data=FeatureData(
                name="feature1",
                description="Feature with current_date",
                model_name="feature1",
                model_type="entity_var_item",
                entity="user",
                cohort_path="user/all",
                path_ref="user/all/feature1",
                warehouse_column_name="feature1",
                yaml="select: count(*)\nfrom: inputs/events\nwhere: created_at >= CURRENT_DATE() - 7"
            )
        )
    
    @pytest.fixture
    def sample_feature_clean(self):
        """Create a clean feature without time functions."""
        return Model(
            name="feature2",
            display_name="feature2",
            model_type="entity_var_item",
            path_ref="user/all/feature2",
            materialization=Materialization(
                output_type="column",
                run_type="discrete",
                sql_type="multi"
            ),
            warehouse_view_name="feature2",
            dependencies=["inputs/events"],
            is_feature=True,
            feature_data=FeatureData(
                name="feature2",
                description="Clean feature",
                model_name="feature2",
                model_type="entity_var_item",
                entity="user",
                cohort_path="user/all",
                path_ref="user/all/feature2",
                warehouse_column_name="feature2",
                yaml="select: sum(amount)\nfrom: inputs/events"
            )
        )
    
    @pytest.fixture
    def sample_input_non_event_stream(self):
        """Create an input model without is_event_stream."""
        return Model(
            name="events",
            display_name="events",
            model_type="input",
            path_ref="inputs/events",
            materialization=Materialization(
                output_type="ephemeral",
                run_type="pre_existing",
                sql_type="single"
            ),
            warehouse_view_name="events",
            dependencies=[],
            is_feature=False,
            is_event_stream=False
        )
    
    @pytest.fixture
    def sample_input_event_stream(self):
        """Create an input model with is_event_stream."""
        return Model(
            name="events",
            display_name="events",
            model_type="input",
            path_ref="inputs/events",
            materialization=Materialization(
                output_type="ephemeral",
                run_type="pre_existing",
                sql_type="single"
            ),
            warehouse_view_name="events",
            dependencies=[],
            is_feature=False,
            is_event_stream=True
        )
    
    def test_validate_no_models_data(self, mock_warehouse_client):
        """Test validation fails when no pb_models_data provided."""
        validator = PropensityValidator(
            "/fake/path",
            "churn_model",
            mock_warehouse_client,
            pb_models_data=None
        )
        
        result = validator.validate()
        
        assert result["validation_status"] == "FAILED"
        assert len(result["errors"]) == 1
        assert result["errors"][0]["type"] == "NO_MODELS_DATA"
    
    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_predict_window_days_not_positive(
        self, 
        mock_profiles_utils, 
        mock_warehouse_client, 
        mock_faulty_yaml_configs
    ):
        """Test validation fails when predict_window_days is not positive."""
        # Mock YAML loading with faulty config (predict_window_days: 0)
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_faulty_yaml_configs["pb_project"],
            "inputs": mock_faulty_yaml_configs["inputs"],
            "models": mock_faulty_yaml_configs["profiles"]
        }
        mock_utils_instance.find_model.return_value = mock_faulty_yaml_configs["profiles"]["models"][0]
        
        # Create a simple propensity model for pb_models_data
        faulty_propensity_model = Model(
            name="faulty_model",
            display_name="faulty_model",
            model_type="propensity",
            path_ref="models/faulty_model",
            materialization=Materialization(
                output_type="table",
                run_type="discrete",
                sql_type="multi"
            ),
            warehouse_view_name="faulty_model",
            is_feature=False,
            entity="user"
        )
        
        pb_data = PBModelsData(
            entities=[],
            models=[faulty_propensity_model]
        )
        
        validator = PropensityValidator(
            "/fake/path",
            "faulty_model",
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()
        
        assert result["validation_status"] == "FAILED"
        # Check for PREDICT_WINDOW_DAYS_NOT_POSITIVE error
        pwd_errors = [
            e for e in result["errors"] 
            if e["type"] == "PREDICT_WINDOW_DAYS_NOT_POSITIVE"
        ]
        assert len(pwd_errors) > 0
        assert "non-positive" in pwd_errors[0]["message"].lower()
    
    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_no_propensity_models(self, mock_profiles_utils, mock_warehouse_client, mock_yaml_configs):
        """Test validation fails when no propensity models found."""
        # Mock YAML loading
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_yaml_configs["pb_project"],
            "inputs": mock_yaml_configs["inputs"],
            "models": {"models": [], "var_groups": []}
        }
        mock_utils_instance.find_model.return_value = None
        
        pb_data = PBModelsData(entities=[], models=[])
        
        validator = PropensityValidator(
            "/fake/path",
            "churn_model",
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()
        
        assert result["validation_status"] == "FAILED"
        assert any(e["type"] == "NO_PROPENSITY_MODELS" for e in result["errors"])
    
    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_model_not_found(
        self, 
        mock_profiles_utils,
        mock_warehouse_client, 
        mock_yaml_configs,
        sample_propensity_model,
        sample_propensity_model_training,
        sample_propensity_model_prediction,
    ):
        """Test validation fails when specific model not found."""
        # Mock YAML loading
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_yaml_configs["pb_project"],
            "inputs": mock_yaml_configs["inputs"],
            "models": mock_yaml_configs["profiles"]
        }
        mock_utils_instance.find_model.return_value = None
        
        pb_data = PBModelsData(
            entities=[],
            models=[sample_propensity_model, sample_propensity_model_training, sample_propensity_model_prediction]
        )
        
        validator = PropensityValidator(
            "/fake/path",
            "nonexistent_model",  # Different name
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()
        
        assert result["validation_status"] == "FAILED"
        assert any(e["type"] == "MODEL_NOT_FOUND" for e in result["errors"])
    
    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_detects_time_function_current_date(
        self,
        mock_profiles_utils,
        mock_warehouse_client,
        mock_yaml_configs,
        sample_propensity_model,
        sample_feature_with_time_function,
        sample_propensity_model_training,
        sample_propensity_model_prediction,
    ):
        """Test validation detects current_date() in feature yaml."""
        # Mock YAML loading
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_yaml_configs["pb_project"],
            "inputs": mock_yaml_configs["inputs"],
            "models": mock_yaml_configs["profiles"]
        }
        mock_utils_instance.find_model.return_value = mock_yaml_configs["profiles"]["models"][0]
        
        pb_data = PBModelsData(
            entities=[],
            models=[sample_propensity_model, sample_feature_with_time_function, sample_propensity_model_training, sample_propensity_model_prediction]
        )
        
        validator = PropensityValidator(
            "/fake/path",
            "churn_model",
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()

        assert result["validation_status"] == "FAILED"
        # Should have error for time function
        time_function_errors = [
            e for e in result["errors"] 
            if e["type"] == "TIME_FUNCTION_IN_FEATURE"
        ]
        assert len(time_function_errors) > 0
        assert "current_date()" in time_function_errors[0]["message"].lower()
    
    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_detects_datediff_function(
        self,
        mock_profiles_utils,
        mock_warehouse_client,
        mock_yaml_configs,
        sample_propensity_model,
        sample_propensity_model_training,
        sample_propensity_model_prediction,
    ):
        """Test validation detects datediff() in feature yaml."""
        # Mock YAML loading
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_yaml_configs["pb_project"],
            "inputs": mock_yaml_configs["inputs"],
            "models": mock_yaml_configs["profiles"]
        }
        mock_utils_instance.find_model.return_value = mock_yaml_configs["profiles"]["models"][0]
        
        feature_with_datediff = Model(
            name="feature1",
            display_name="feature1",
            model_type="entity_var_item",
            path_ref="user/all/feature1",
            materialization=Materialization(
                output_type="column",
                run_type="discrete",
                sql_type="multi"
            ),
            warehouse_view_name="feature1",
            dependencies=["inputs/events"],
            is_feature=True,
            feature_data=FeatureData(
                name="feature1",
                description="Feature with datediff",
                model_name="feature1",
                model_type="entity_var_item",
                entity="user",
                cohort_path="user/all",
                path_ref="user/all/feature1",
                warehouse_column_name="feature1",
                yaml="select: DATEDIFF(day, created_at, updated_at)\nfrom: inputs/events"
            )
        )
        
        pb_data = PBModelsData(
            entities=[],
            models=[sample_propensity_model, feature_with_datediff, sample_propensity_model_training, sample_propensity_model_prediction]
        )
        
        validator = PropensityValidator(
            "/fake/path",
            "churn_model",
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()
        
        assert result["validation_status"] == "FAILED"
        time_function_errors = [
            e for e in result["errors"] 
            if e["type"] == "TIME_FUNCTION_IN_FEATURE"
        ]
        assert len(time_function_errors) > 0
        assert "datediff()" in time_function_errors[0]["message"].lower()
    
    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_detects_non_event_stream_input(
        self,
        mock_profiles_utils,
        mock_warehouse_client,
        mock_yaml_configs,
        sample_propensity_model,
        sample_feature_clean,
        sample_input_non_event_stream,
        sample_propensity_model_training,
        sample_propensity_model_prediction,
    ):
        """Test validation detects input without is_event_stream."""
        # Mock YAML loading
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_yaml_configs["pb_project"],
            "inputs": mock_yaml_configs["inputs"],
            "models": mock_yaml_configs["profiles"]
        }
        mock_utils_instance.find_model.return_value = mock_yaml_configs["profiles"]["models"][0]
        
        pb_data = PBModelsData(
            entities=[],
            models=[
                sample_propensity_model,
                sample_propensity_model_training,
                sample_propensity_model_prediction,
                sample_feature_clean,
                sample_input_non_event_stream
            ]
        )
        
        validator = PropensityValidator(
            "/fake/path",
            "churn_model",
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()

        assert result["validation_status"] == "FAILED"
        event_stream_errors = [
            e for e in result["errors"] 
            if e["type"] == "NON_EVENT_STREAM_INPUT"
        ]
        assert len(event_stream_errors) > 0
        assert "is_event_stream" in event_stream_errors[0]["message"]
    
    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_detects_non_feature_input(
        self,
        mock_profiles_utils,
        mock_warehouse_client,
        mock_yaml_configs,
        sample_propensity_model,
        sample_propensity_model_training,
        sample_propensity_model_prediction,
    ):
        """Test validation detects direct inputs without is_feature."""
        # Mock YAML loading
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_yaml_configs["pb_project"],
            "inputs": mock_yaml_configs["inputs"],
            "models": mock_yaml_configs["profiles"]
        }
        mock_utils_instance.find_model.return_value = mock_yaml_configs["profiles"]["models"][0]
        
        non_feature = Model(
            name="feature1",
            display_name="feature1",
            model_type="entity_var_item",
            path_ref="user/all/feature1",
            materialization=Materialization(
                output_type="column",
                run_type="discrete",
                sql_type="multi"
            ),
            warehouse_view_name="feature1",
            dependencies=["inputs/events"],
            is_feature=False,  # Not marked as feature
            feature_data=None
        )
        
        pb_data = PBModelsData(
            entities=[],
            models=[sample_propensity_model, non_feature, sample_propensity_model_training, sample_propensity_model_prediction]
        )
        
        validator = PropensityValidator(
            "/fake/path",
            "churn_model",
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()
        
        assert result["validation_status"] == "FAILED"
        feature_errors = [
            e for e in result["errors"] 
            if e["type"] == "NON_FEATURE_INPUT"
        ]
        assert len(feature_errors) > 0
        assert "is_feature" in feature_errors[0]["message"]
    
    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_passes_with_valid_model(
        self,
        mock_profiles_utils,
        mock_warehouse_client,
        mock_yaml_configs,
        sample_propensity_model,
        sample_feature_clean,
        sample_input_event_stream,
        sample_propensity_model_training,
        sample_propensity_model_prediction,
    ):
        """Test validation passes with valid configuration."""
        # Mock YAML loading
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_yaml_configs["pb_project"],
            "inputs": mock_yaml_configs["inputs"],
            "models": mock_yaml_configs["profiles"]
        }
        mock_utils_instance.find_model.return_value = mock_yaml_configs["profiles"]["models"][0]
        sample_propensity_model_training.dependencies =["user/all/feature2"]
        
        pb_data = PBModelsData(
            entities=[],
            models=[
                sample_propensity_model,    
                sample_propensity_model_training,
                sample_propensity_model_prediction,
                sample_feature_clean,
                sample_input_event_stream,
            ]
        )
        
        validator = PropensityValidator(
            "/fake/path",
            "churn_model",
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()
        
        assert result["validation_status"] == "PASSED"

    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_fails_with_an_valid_model(
        self,
        mock_profiles_utils,
        mock_warehouse_client,
        mock_yaml_configs,
        sample_propensity_model,
        sample_feature_clean,
        sample_input_event_stream,
        sample_propensity_model_training,
        sample_propensity_model_prediction,
    ):
        """Test validation passes with valid configuration."""
        # Mock YAML loading
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_yaml_configs["pb_project"],
            "inputs": mock_yaml_configs["inputs"],
            "models": mock_yaml_configs["profiles"]
        }
        mock_utils_instance.find_model.return_value = mock_yaml_configs["profiles"]["models"][0]
        
        pb_data = PBModelsData(
            entities=[],
            models=[
                sample_propensity_model,    
                sample_propensity_model_training,
                sample_propensity_model_prediction,
                sample_feature_clean,
                sample_input_event_stream
            ]
        )
        
        validator = PropensityValidator(
            "/fake/path",
            "churn_model",
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()
        
        assert result["validation_status"] == "FAILED"
    
    @patch('src.validators.propensity_validator.ProfilesUtils')
    def test_validate_tracks_training_prediction_models(
        self,
        mock_profiles_utils,
        mock_warehouse_client,
        mock_yaml_configs,
        sample_propensity_model,
        sample_propensity_model_training,
        sample_propensity_model_prediction,
    ):
        """Test validation tracks training and prediction models."""
        # Mock YAML loading
        mock_utils_instance = mock_profiles_utils.return_value
        mock_utils_instance.load_all_configs.return_value = {
            "project": mock_yaml_configs["pb_project"],
            "inputs": mock_yaml_configs["inputs"],
            "models": mock_yaml_configs["profiles"]
        }
        mock_utils_instance.find_model.return_value = mock_yaml_configs["profiles"]["models"][0]
        
        training_model = Model(
            name="churn_model_training",
            display_name="churn_model_training",
            model_type="python_model",
            path_ref="models/churn_model_training",
            materialization=Materialization(
                output_type="table",
                run_type="discrete",
                sql_type="single"
            ),
            warehouse_view_name="churn_model_training",
            dependencies=[],
            is_feature=False
        )
        
        prediction_model = Model(
            name="churn_model_prediction",
            display_name="churn_model_prediction",
            model_type="python_model",
            path_ref="models/churn_model_prediction",
            materialization=Materialization(
                output_type="table",
                run_type="discrete",
                sql_type="single"
            ),
            warehouse_view_name="churn_model_prediction",
            dependencies=[],
            is_feature=False
        )
        
        pb_data = PBModelsData(
            entities=[],
            models=[sample_propensity_model, training_model, prediction_model, sample_propensity_model_training, sample_propensity_model_prediction]
        )
        
        validator = PropensityValidator(
            "/fake/path",
            "churn_model",
            mock_warehouse_client,
            pb_models_data=pb_data
        )
        
        result = validator.validate()
        assert result["validation_status"] == "PASSED"
        
