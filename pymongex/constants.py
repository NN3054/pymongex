from enum import Enum, EnumMeta

from bson import ObjectId


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, field=None, config=None):
        if isinstance(v, str):
            if not ObjectId.is_valid(v):
                raise ValueError("Invalid objectid")
            return ObjectId(v)
        elif isinstance(v, ObjectId):
            return ObjectId(v)
        else:
            raise TypeError("ObjectId must be a string or ObjectId, not %s" % type(v))

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema, handler):
        core_schema.update(type="string", pattern="^[0-9a-fA-F]{24}$")
        return {"type": "string", "format": "objectid"}


class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class BaseEnum(Enum, metaclass=MetaEnum):

    @classmethod
    def to_list(cls):
        return [item.value for item in cls]
