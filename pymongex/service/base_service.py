from typing import Type

from bson import ObjectId
from pydantic import BaseModel

from ..constants import BaseEnum, PyObjectId
from ..models.collection import (
    OutCollectionModel,
)


class BaseService:
    _out_model: Type[OutCollectionModel]

    @classmethod
    def _apply_types_to_query(cls, query: dict[str, dict]) -> dict:
        """
        Gets query where each field has a value that is a dict with a mongodb operator and a value that is always a string.
        This string value should be correctly converted to the correct type. It is possible that the field is annotated with a "." to indicate a nested field.
        Supported mongodb operators are: $eq, $ne, $lt, $lte, $gt, $gte, $regex
        The model which has the correct types is _out_model.
        """

        def convert_value(field_type, value: str):
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
            else:
                raise ValueError(f"Unsupported field type: {field_type}")

        converted_query = {}
        for field, condition in query.items():
            if "." in field:
                raise ValueError(
                    "Querying nested fields are not supported with api query"
                )
            field_type = cls._out_model.get_field_type(field)
            if not field_type:
                raise ValueError(f"Field '{field}' not found in model")

            converted_condition = {}
            for operator, value in condition.items():
                if operator not in [
                    "$eq",
                    "$ne",
                    "$lt",
                    "$lte",
                    "$gt",
                    "$gte",
                    "$regex",
                ]:
                    raise ValueError(
                        f"Unsupported operator: {operator}. Equals to '!' in query parameters. Deprecated."
                    )
                converted_condition[operator] = convert_value(field_type, value)

            converted_query[field] = converted_condition

        return converted_query

    @classmethod
    def _prepare_update(cls, update: dict) -> dict:
        if isinstance(update, BaseModel):
            update = update.dict()

        # check if no $ operator is used -> use $set
        is_set = not any([key.startswith("$") for key in update.keys()])
        if is_set:
            update = {"$set": update}
        return update
