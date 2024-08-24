from datetime import datetime as dt
from datetime import timezone
from typing import Any, Dict, List, Optional

from pydantic import Field, model_validator

from ..constants import PyObjectId
from ..storage.collection import Collection
from ..utils import utc_now
from .datamodel import DataModel


def ensure_utc_timezone(value: Any) -> Any:
    if isinstance(value, dt) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    elif isinstance(value, dict):
        return {k: ensure_utc_timezone(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [ensure_utc_timezone(v) for v in value]
    return value


class CollectionModel(DataModel):

    class Collection:
        collection: Collection = Collection(db="testDB", name="test.collection")

    @model_validator(mode="before")
    def ensure_all_utc_timezone(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values = {k: ensure_utc_timezone(v) for k, v in values.items()}
        return values

    @classmethod
    def get_database(cls) -> str:
        collection = cls.Collection.collection
        if collection is None:
            raise NotImplementedError("Collection name not specified in model config")
        return collection.db

    @classmethod
    def get_collection(cls) -> str:
        collection = cls.Collection.collection
        if collection is None:
            raise NotImplementedError("Collection name not specified in model config")
        return collection.name


class InCollectionModel(CollectionModel):
    created_at: dt = Field(
        default_factory=utc_now,
        json_schema_extra={
            "local_field": "project_id",
            "foreign_field": "_id",
        },
    )

    def db_dict(self):
        return self.dict(exclude={"id"})


class OutCollectionModel(CollectionModel):
    id: PyObjectId = Field(..., description="MongoDB Object ID")
    created_at: Optional[dt] = Field(default=None)
    updated_at: Optional[dt] = Field(default=None)

    @model_validator(mode="before")
    def replace_empty_dict_with_none(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # set fiels that are {} to None
        for k, v in values.items():
            if v == {}:
                values[k] = None
        return values

    @classmethod
    def get_projection(cls):
        projection = {field: 1 for field in cls.get_keys()}
        projection["id"] = "$_id"
        projection["_id"] = 0
        return projection

    @classmethod
    def get_nested_projection(cls, nested_field):
        nested_model = cls.get_field_type(nested_field)
        local_field = cls.get_field_local_field(nested_field)
        # expand_collection = field_info.extra.get("expand_collection")
        # foreign_field = field_info.extra.get("foreign_field")
        if issubclass(nested_model, InCollectionModel):

            if_statement = {
                "$or": [
                    {"$eq": [f"${local_field}", None]},
                    {"$eq": [f"${nested_field}", None]},
                    {"$eq": [f"${nested_field}", {}]},
                ]
            }

            # then set value of nested field to None
            then_statement = None

            else_statement = {
                f"{field}": f"${nested_field}.{field}"
                for field in nested_model.get_keys()
            }
            # add id to else statement
            else_statement["id"] = f"${nested_field}._id"

            return {
                f"{nested_field}": {
                    "$cond": {
                        "if": if_statement,
                        "then": then_statement,
                        "else": else_statement,
                    }
                },
            }

    @classmethod
    def get_custom_pipelines(cls):
        custom_pipelines = {}
        for field_name in cls.get_keys():
            pipeline = cls.get_field_extra(field_name, "pipeline")
            if pipeline:
                custom_pipelines[field_name] = pipeline
        return custom_pipelines

    @classmethod
    def get_field_local_field(cls, field_name):
        return cls.get_field_extra(field_name, "local_field")

    @classmethod
    def get_field_foreign_field(cls, field_name):
        return cls.get_field_extra(field_name, "foreign_field")

    @classmethod
    def get_expandable_fields(cls) -> List[str]:
        # all fields that have a local and foreign field set
        return [
            field_name
            for field_name in cls.get_keys()
            if cls.get_field_local_field(field_name)
            and cls.get_field_foreign_field(field_name)
        ]

    class Config:
        # Exclude custom attributes from OpenAPI schema
        @staticmethod
        def json_schema_extra(schema, model):
            for prop in schema.get("properties", {}).values():
                prop.pop("local_field", None)
                prop.pop("foreign_field", None)
