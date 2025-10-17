"""
Parser for pb mcp models JSON output.

This module provides data models and parsing functionality for the JSON output
from the `pb mcp models -p project_path` command. The parsed models can be used
by any MCP tool that needs access to entity and model metadata.

Usage:
    from src.utils.pb_models_parser import PBModelsParser
    
    parser = PBModelsParser.from_json_file("path/to/output.json")
    # or
    parser = PBModelsParser.from_json_string(json_string)
    
    # Access entities and models
    entities = parser.entities
    models = parser.models
    
    # Query specific models
    feature_views = parser.get_models_by_type("feature_view")
    user_entity = parser.get_entity_by_name("user")
"""

import json
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from pathlib import Path


@dataclass
class Materialization:
    """Represents materialization configuration for a model."""
    output_type: str
    run_type: str
    sql_type: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Materialization":
        """Create Materialization from dictionary."""
        return cls(
            output_type=data.get("output_type", ""),
            run_type=data.get("run_type", ""),
            sql_type=data.get("sql_type")
        )


@dataclass
class FeatureData:
    """Represents feature metadata for feature models."""
    name: str
    description: str
    model_name: str
    model_type: str
    entity: str
    cohort_path: str
    path_ref: str
    warehouse_column_name: str
    yaml: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FeatureData":
        """Create FeatureData from dictionary."""
        return cls(
            name=data.get("name", ""),
            description=data.get("description", ""),
            model_name=data.get("model_name", ""),
            model_type=data.get("model_type", ""),
            entity=data.get("entity", ""),
            cohort_path=data.get("cohort_path", ""),
            path_ref=data.get("path_ref", ""),
            warehouse_column_name=data.get("warehouse_column_name", ""),
            yaml=data.get("yaml", "")
        )


@dataclass
class Entity:
    """Represents an entity definition."""
    name: str
    description: str
    id_column_name: str
    id_model_ref: str
    id_types: List[str]
    path_ref: str
    default_cohort_path_ref: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Entity":
        """Create Entity from dictionary."""
        return cls(
            name=data.get("name", ""),
            description=data.get("description", ""),
            id_column_name=data.get("id_column_name", ""),
            id_model_ref=data.get("id_model_ref", ""),
            id_types=data.get("id_types", []),
            path_ref=data.get("path_ref", ""),
            default_cohort_path_ref=data.get("default_cohort_path_ref", "")
        )


@dataclass
class Model:
    """Represents a model definition from pb mcp models output."""
    name: str
    display_name: str
    model_type: str
    path_ref: str
    materialization: Materialization
    warehouse_view_name: str
    dependencies: List[str] = field(default_factory=list)
    is_feature: bool = False
    is_event_stream: bool = False
    
    # Optional fields
    entity: Optional[str] = None
    entity_key: Optional[str] = None
    cohort_path_ref: Optional[str] = None
    feature_view_id_column_name: Optional[str] = None
    feature_data: Optional[FeatureData] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Model":
        """Create Model from dictionary."""
        materialization = Materialization.from_dict(data.get("materialization", {}))
        feature_data = None
        if data.get("feature_data"):
            feature_data = FeatureData.from_dict(data["feature_data"])
        
        return cls(
            name=data.get("name", ""),
            display_name=data.get("display_name", ""),
            model_type=data.get("model_type", ""),
            path_ref=data.get("path_ref", ""),
            materialization=materialization,
            warehouse_view_name=data.get("warehouse_view_name", ""),
            dependencies=data.get("dependencies", []),
            is_feature=data.get("is_feature", False),
            is_event_stream=data.get("is_event_stream", False),
            entity=data.get("entity"),
            entity_key=data.get("entity_key"),
            cohort_path_ref=data.get("cohort_path_ref"),
            feature_view_id_column_name=data.get("feature_view_id_column_name"),
            feature_data=feature_data
        )


@dataclass
class PBModelsData:
    """
    Container for all entities and models from pb mcp models output.
    
    This class provides a structured representation of the JSON output from
    `pb mcp models` command and offers convenient query methods.
    """
    entities: List[Entity] = field(default_factory=list)
    models: List[Model] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PBModelsData":
        """Create PBModelsData from dictionary."""
        entities = [Entity.from_dict(e) for e in data.get("entities", [])]
        models = [Model.from_dict(m) for m in data.get("models", [])]
        return cls(entities=entities, models=models)
    
    def get_entity_by_name(self, name: str) -> Optional[Entity]:
        """Get entity by name."""
        for entity in self.entities:
            if entity.name == name:
                return entity
        return None

    def get_model_by_name_and_type(self, name: str, model_type: str) -> Optional[Model]:
        """Get a specific model by name and type."""
        for model in self.models:
            if model.name == name and model.model_type == model_type:
                return model
        return None
    
    def get_models_by_type(self, model_type: str) -> List[Model]:
        """Get all models of a specific type."""
        return [m for m in self.models if m.model_type == model_type]

    def get_model_by_name(self, name: str) -> Optional[Model]:
        """Get a specific model by name."""
        for model in self.models:
            if model.name == name:
                return model
        return None
    
    def get_models_by_entity(self, entity_name: str) -> List[Model]:
        """Get all models for a specific entity."""
        return [m for m in self.models if m.entity == entity_name]
    
    def get_feature_models(self) -> List[Model]:
        """Get all feature models."""
        return [m for m in self.models if m.is_feature]
    
    def get_input_models(self) -> List[Model]:
        """Get all input models."""
        return self.get_models_by_type("input")
    
    def get_id_stitcher_models(self) -> List[Model]:
        """Get all id_stitcher models."""
        return self.get_models_by_type("id_stitcher")
    
    def get_feature_views(self) -> List[Model]:
        """Get all feature_view models."""
        return self.get_models_by_type("feature_view")
    
    def get_propensity_models(self) -> List[Model]:
        """Get all propensity models."""
        return self.get_models_by_type("python_model")


class PBModelsParser:
    """
    Parser for pb mcp models JSON output.
    
    This class provides static methods to parse JSON from files or strings
    and return structured PBModelsData objects.
    """
    
    @staticmethod
    def from_json_file(file_path: str) -> PBModelsData:
        """
        Parse PB models data from a JSON file.
        
        Args:
            file_path: Path to the JSON file (output from pb mcp models command)
            
        Returns:
            PBModelsData object containing parsed entities and models
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")
        
        with open(path, 'r') as f:
            data = json.load(f)
        
        return PBModelsData.from_dict(data)
    
    @staticmethod
    def from_json_string(json_string: str) -> PBModelsData:
        """
        Parse PB models data from a JSON string.
        
        Args:
            json_string: JSON string (output from pb mcp models command)
            
        Returns:
            PBModelsData object containing parsed entities and models
            
        Raises:
            json.JSONDecodeError: If the string contains invalid JSON
        """
        data = json.loads(json_string)
        return PBModelsData.from_dict(data)
    
    @staticmethod
    def from_dict(data: Dict[str, Any]) -> PBModelsData:
        """
        Parse PB models data from a dictionary.
        
        Args:
            data: Dictionary representation of pb mcp models output
            
        Returns:
            PBModelsData object containing parsed entities and models
        """
        return PBModelsData.from_dict(data)

