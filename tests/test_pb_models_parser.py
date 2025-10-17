"""
Unit tests for PB Models Parser.

Tests the parsing and unmarshaling of JSON output from `pb mcp models` command.
"""

import json
import pytest
import tempfile
from pathlib import Path
from src.utils.pb_models_parser import (
    PBModelsParser,
    PBModelsData,
    Entity,
    Model,
    Materialization,
    FeatureData
)


# Sample test data based on the actual j.json structure
SAMPLE_JSON_DATA = {
    "entities": [
        {
            "name": "user",
            "description": "Platform Users. May include buyers and sellers both.",
            "id_column_name": "user_main_id",
            "id_model_ref": "models/test_id__",
            "id_types": ["test_id", "email", "rudder_id"],
            "path_ref": "user",
            "default_cohort_path_ref": "user/all"
        }
    ],
    "models": [
        {
            "name": "a_max",
            "display_name": "a_max",
            "model_type": "entity_var_item",
            "entity": "user",
            "path_ref": "user/all/a_max",
            "entity_key": "user",
            "materialization": {
                "output_type": "column",
                "run_type": "discrete",
                "sql_type": "multi"
            },
            "warehouse_view_name": "a_max",
            "cohort_path_ref": "user/all",
            "dependencies": [
                "inputs/tbl_a/num_a",
                "inputs/tbl_a/var_table/user_main_id",
                "user/all/var_table",
                "inputs/tbl_a/var_table"
            ],
            "is_feature": True,
            "feature_data": {
                "name": "a_max",
                "description": "",
                "model_name": "a_max",
                "model_type": "entity_var_item",
                "entity": "user",
                "cohort_path": "user/all",
                "path_ref": "user/all/a_max",
                "warehouse_column_name": "a_max",
                "yaml": "name: a_max\\nselect: max({{tbl_a.Var(\\\"num_a\\\")}})\\nfrom: inputs/tbl_a\\nretention_period: 24h0m0s\\n"
            }
        },
        {
            "name": "feature_view",
            "display_name": "feature_view",
            "model_type": "feature_view",
            "entity": "user",
            "path_ref": "user/all/feature_view",
            "entity_key": "user",
            "materialization": {
                "output_type": "view",
                "run_type": "discrete",
                "sql_type": "single"
            },
            "warehouse_view_name": "user_feature_view",
            "cohort_path_ref": "user/all",
            "feature_view_id_column_name": "user_main_id",
            "dependencies": [
                "user/all/a_max",
                "user/all",
                "models/test_id__"
            ],
            "is_feature": False
        },
        {
            "name": "test_id__",
            "display_name": "test_id__",
            "model_type": "id_stitcher",
            "entity": "user",
            "path_ref": "models/test_id__",
            "entity_key": "user",
            "materialization": {
                "output_type": "view",
                "run_type": "discrete",
                "sql_type": "multi"
            },
            "warehouse_view_name": "test_id__",
            "dependencies": [
                "inputs/tbl_a",
                "inputs/tbl_b",
                "inputs/tbl_c"
            ],
            "is_feature": False
        }
    ]
}


class TestMaterialization:
    """Test Materialization data class."""
    
    def test_from_dict(self):
        data = {
            "output_type": "column",
            "run_type": "discrete",
            "sql_type": "multi"
        }
        mat = Materialization.from_dict(data)
        
        assert mat.output_type == "column"
        assert mat.run_type == "discrete"
        assert mat.sql_type == "multi"
    
    def test_from_dict_missing_sql_type(self):
        data = {
            "output_type": "view",
            "run_type": "discrete"
        }
        mat = Materialization.from_dict(data)
        
        assert mat.output_type == "view"
        assert mat.run_type == "discrete"
        assert mat.sql_type is None


class TestFeatureData:
    """Test FeatureData data class."""
    
    def test_from_dict(self):
        data = {
            "name": "a_max",
            "description": "Max value",
            "model_name": "a_max",
            "model_type": "entity_var_item",
            "entity": "user",
            "cohort_path": "user/all",
            "path_ref": "user/all/a_max",
            "warehouse_column_name": "a_max",
            "yaml": "name: a_max\\nselect: max(num_a)"
        }
        feature = FeatureData.from_dict(data)
        
        assert feature.name == "a_max"
        assert feature.description == "Max value"
        assert feature.model_name == "a_max"
        assert feature.entity == "user"


class TestEntity:
    """Test Entity data class."""
    
    def test_from_dict(self):
        data = {
            "name": "user",
            "description": "Platform Users",
            "id_column_name": "user_main_id",
            "id_model_ref": "models/test_id__",
            "id_types": ["test_id", "email", "rudder_id"],
            "path_ref": "user",
            "default_cohort_path_ref": "user/all"
        }
        entity = Entity.from_dict(data)
        
        assert entity.name == "user"
        assert entity.description == "Platform Users"
        assert entity.id_column_name == "user_main_id"
        assert entity.id_model_ref == "models/test_id__"
        assert entity.id_types == ["test_id", "email", "rudder_id"]
        assert entity.path_ref == "user"
        assert entity.default_cohort_path_ref == "user/all"


class TestModel:
    """Test Model data class."""
    
    def test_from_dict_with_feature_data(self):
        data = SAMPLE_JSON_DATA["models"][0]  # a_max model with feature_data
        model = Model.from_dict(data)
        
        assert model.name == "a_max"
        assert model.display_name == "a_max"
        assert model.model_type == "entity_var_item"
        assert model.entity == "user"
        assert model.warehouse_view_name == "a_max"
        assert model.is_feature is True
        assert model.feature_data is not None
        assert model.feature_data.name == "a_max"
        assert len(model.dependencies) == 4
    
    def test_from_dict_without_feature_data(self):
        data = SAMPLE_JSON_DATA["models"][2]  # id_stitcher model
        model = Model.from_dict(data)
        
        assert model.name == "test_id__"
        assert model.model_type == "id_stitcher"
        assert model.is_feature is False
        assert model.feature_data is None
    
    def test_from_dict_with_feature_view(self):
        data = SAMPLE_JSON_DATA["models"][1]  # feature_view model
        model = Model.from_dict(data)
        
        assert model.name == "feature_view"
        assert model.model_type == "feature_view"
        assert model.feature_view_id_column_name == "user_main_id"


class TestPBModelsData:
    """Test PBModelsData container class."""
    
    @pytest.fixture
    def pb_models_data(self):
        return PBModelsData.from_dict(SAMPLE_JSON_DATA)
    
    def test_from_dict(self, pb_models_data):
        assert len(pb_models_data.entities) == 1
        assert len(pb_models_data.models) == 3
        assert pb_models_data.entities[0].name == "user"
    
    def test_get_entity_by_name(self, pb_models_data: PBModelsData):
        entity = pb_models_data.get_entity_by_name("user")
        assert entity is not None
        assert entity.name == "user"
        assert entity.id_column_name == "user_main_id"
        
        # Test non-existent entity
        assert pb_models_data.get_entity_by_name("nonexistent") is None
    
    def test_get_models_by_type(self, pb_models_data: PBModelsData):
        entity_vars = pb_models_data.get_models_by_type("entity_var_item")
        assert len(entity_vars) == 1
        assert entity_vars[0].name == "a_max"
        
        feature_views = pb_models_data.get_models_by_type("feature_view")
        assert len(feature_views) == 1
        assert feature_views[0].name == "feature_view"
        
        id_stitchers = pb_models_data.get_models_by_type("id_stitcher")
        assert len(id_stitchers) == 1
        assert id_stitchers[0].name == "test_id__"
    
    def test_get_model_by_name_and_type(self, pb_models_data):
        model = pb_models_data.get_model_by_name_and_type("a_max", "entity_var_item")
        assert model is not None
        assert model.name == "a_max"
        
        # Test non-existent model
        assert pb_models_data.get_model_by_name_and_type("nonexistent", "entity_var_item") is None
    
    def test_get_models_by_entity(self, pb_models_data):
        user_models = pb_models_data.get_models_by_entity("user")
        assert len(user_models) == 3
        assert all(m.entity == "user" for m in user_models)
    
    def test_get_feature_models(self, pb_models_data):
        features = pb_models_data.get_feature_models()
        assert len(features) == 1
        assert features[0].name == "a_max"
        assert features[0].is_feature is True
    
    def test_get_id_stitcher_models(self, pb_models_data):
        id_stitchers = pb_models_data.get_id_stitcher_models()
        assert len(id_stitchers) == 1
        assert id_stitchers[0].name == "test_id__"
    
    def test_get_feature_views(self, pb_models_data):
        feature_views = pb_models_data.get_feature_views()
        assert len(feature_views) == 1
        assert feature_views[0].name == "feature_view"


class TestPBModelsParser:
    """Test PBModelsParser parsing functionality."""
    
    def test_from_json_string(self):
        json_string = json.dumps(SAMPLE_JSON_DATA)
        pb_data = PBModelsParser.from_json_string(json_string)
        
        assert isinstance(pb_data, PBModelsData)
        assert len(pb_data.entities) == 1
        assert len(pb_data.models) == 3
        assert pb_data.entities[0].name == "user"
    
    def test_from_json_file(self):
        # Create a temporary JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(SAMPLE_JSON_DATA, f)
            temp_file = f.name
        
        try:
            pb_data = PBModelsParser.from_json_file(temp_file)
            
            assert isinstance(pb_data, PBModelsData)
            assert len(pb_data.entities) == 1
            assert len(pb_data.models) == 3
        finally:
            # Cleanup
            Path(temp_file).unlink()
    
    def test_from_json_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            PBModelsParser.from_json_file("/nonexistent/path/file.json")
    
    def test_from_json_string_invalid_json(self):
        with pytest.raises(json.JSONDecodeError):
            PBModelsParser.from_json_string("invalid json {")
    
    def test_from_dict(self):
        pb_data = PBModelsParser.from_dict(SAMPLE_JSON_DATA)
        
        assert isinstance(pb_data, PBModelsData)
        assert len(pb_data.entities) == 1
        assert len(pb_data.models) == 3


class TestActualJSONFile:
    """Test parsing the actual j.json file from the project."""
    
    def test_parse_j_json_file(self):
        """Test that we can successfully parse the actual j.json file."""
        # Path to the actual j.json file in the project
        j_json_path = "/Users/sp/rudderstack/codes/profiles-mcp/j.json"
        
        # Skip if file doesn't exist (in case test runs in different environment)
        if not Path(j_json_path).exists():
            pytest.skip(f"j.json file not found at {j_json_path}")
        
        # Parse the file
        pb_data = PBModelsParser.from_json_file(j_json_path)
        
        # Validate structure
        assert isinstance(pb_data, PBModelsData)
        assert len(pb_data.entities) > 0
        assert len(pb_data.models) > 0
        
        # Validate specific data from j.json
        user_entity = pb_data.get_entity_by_name("user")
        assert user_entity is not None
        assert user_entity.description == "Platform Users. May include buyers and sellers both."
        assert user_entity.id_column_name == "user_main_id"
        assert "test_id" in user_entity.id_types
        assert "email" in user_entity.id_types
        
        # Test querying different model types
        entity_vars = pb_data.get_models_by_type("entity_var_item")
        assert len(entity_vars) > 0
        
        feature_views = pb_data.get_feature_views()
        assert len(feature_views) > 0
        
        id_stitchers = pb_data.get_id_stitcher_models()
        assert len(id_stitchers) > 0
        
        # Validate feature models
        feature_models = pb_data.get_feature_models()
        assert len(feature_models) > 0
        
        # Check specific models
        a_max_model = pb_data.get_model_by_name_and_type("a_max", "entity_var_item")
        assert a_max_model is not None
        assert a_max_model.is_feature is True
        assert a_max_model.feature_data is not None
        
        # Check feature view
        feature_view = pb_data.get_model_by_name_and_type("feature_view", "feature_view")
        assert feature_view is not None
        assert feature_view.feature_view_id_column_name == "user_main_id"
    
    def test_parse_j_json_comprehensive_checks(self):
        """Comprehensive validation of j.json structure."""
        j_json_path = "/Users/sp/rudderstack/codes/profiles-mcp/j.json"
        
        if not Path(j_json_path).exists():
            pytest.skip(f"j.json file not found at {j_json_path}")
        
        pb_data = PBModelsParser.from_json_file(j_json_path)
        
        # Count different model types
        model_type_counts = {}
        for model in pb_data.models:
            model_type_counts[model.model_type] = model_type_counts.get(model.model_type, 0) + 1
        
        # Verify we have various model types
        assert "entity_var_item" in model_type_counts
        assert "feature_view" in model_type_counts
        assert "id_stitcher" in model_type_counts
        assert "input" in model_type_counts
        
        # Verify all feature models have feature_data
        for model in pb_data.get_feature_models():
            assert model.feature_data is not None
            assert model.feature_data.name == model.name
        
        # Verify materialization data is present
        for model in pb_data.models:
            assert model.materialization is not None
            assert model.materialization.output_type != ""
            assert model.materialization.run_type != ""
