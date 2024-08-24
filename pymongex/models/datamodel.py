import json
from datetime import datetime
from datetime import datetime as dt
from uuid import UUID

import orjson
from bson import ObjectId
from pydantic import BaseModel, ConfigDict

from pymongex.constants import BaseEnum

from ..constants import PyObjectId


def orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()


class DataModel(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        str_strip_whitespace=True,
        arbitrary_types_allowed=True,
        json_encoders={
            ObjectId: lambda o: str(o),
            PyObjectId: lambda o: str(o),
            UUID: lambda o: str(o),
        },
        populate_by_name=True,
        allow_inf_nan=False,
        from_attributes=True,
    )

    @classmethod
    def get_field_info(cls, field_name):
        field = cls.model_fields[field_name]
        return field

    @classmethod
    def get_field_type(cls, field_name):
        field = cls.get_field_info(field_name)
        annotation = field.annotation
        if hasattr(annotation, "__args__"):
            field_type = annotation.__args__[0]
        else:
            field_type = annotation
        return field_type

    @classmethod
    def get_field_extra(cls, field_name: str, extra_key: str):
        json_schema = cls.get_field_info(field_name=field_name).json_schema_extra
        if json_schema is None:
            return None
        extra = json_schema.get(extra_key)
        return extra

    @classmethod
    def get_keys(cls) -> list[str]:
        keys = cls.model_fields.keys()
        return list(keys)

    @classmethod
    def get_items(cls) -> list:
        items = cls.model_fields.items()
        return items

    @classmethod
    def convert_value(cls, field_type, value: str):
        if field_type is int:
            return int(value)
        elif field_type is float:
            return float(value)
        elif field_type is bool:
            return value.lower() in ["true", "1", "yes"]
        elif field_type is str:
            return value
        # check if field type is enum
        elif issubclass(field_type, BaseEnum):
            return field_type(value).value
        # check if field type is PyObjectId -> convert to ObjectId
        elif field_type is ObjectId or field_type is PyObjectId:
            return ObjectId(value)
        elif field_type is dt:
            return dt.fromisoformat(value)
        else:
            raise ValueError(f"Unsupported field type: {field_type}")

    @classmethod
    def convert_value_to_field_type(cls, field_name: str, value: str):
        field_type = cls.get_field_type(field_name)
        return cls.convert_value(field_type, value)

    def _serialize_base_field(self, field_value):
        if isinstance(field_value, ObjectId):
            return str(field_value)
        elif isinstance(field_value, UUID):
            return str(field_value)
        elif isinstance(field_value, PyObjectId):
            return str(field_value)
        elif isinstance(field_value, datetime):
            return str(field_value)
        return field_value

    def _serialize_list_field(self, field_value):
        return [
            (
                self._dict_to_json_dict(item)
                if isinstance(item, dict)
                else (
                    self._serialize_list_field(item)
                    if isinstance(item, list)
                    else self._serialize_base_field(item)
                )
            )
            for item in field_value
        ]

    def _dict_to_json_dict(self, data: dict) -> dict:
        """Return a dict which contains only serializable fields."""
        json_dict = {}
        for field_name, field_value in data.items():

            if isinstance(field_value, dict):
                json_dict[field_name] = self._dict_to_json_dict(field_value)
            elif isinstance(field_value, list):
                json_dict[field_name] = self._serialize_list_field(field_value)
            else:
                json_dict[field_name] = self._serialize_base_field(field_value)

        return json_dict

    def json_dict(self, **kwargs):
        """Return a dict which contains only serializable fields."""
        default_dict = super().model_dump(**kwargs)

        json_dict = self._dict_to_json_dict(default_dict)

        return json_dict

    def dump_to_json(self, fp: str = "data.json") -> str:
        # Convert the dict to a JSON string and back to a dict to ensure JSON compatibility
        json_dict = self.json_dict()
        json_string = json.dumps(json_dict, indent=4)
        with open(fp, "w") as f:
            f.write(json_string)
        return fp
