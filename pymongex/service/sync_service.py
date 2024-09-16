from typing import Any, List, Optional, Type, Union

from bson import ObjectId
from pydantic import BaseModel

from ..clients.sync_client import SyncMongoClient
from ..models.collection import (
    InCollectionModel,
    OutCollectionModel,
)
from ..pipelines import PipelineBuilder
from .base_service import BaseService


class SyncBaseService(BaseService):
    _mongo_client: SyncMongoClient = SyncMongoClient()
    _in_model: Type[InCollectionModel]
    _out_model: Type[OutCollectionModel]

    @classmethod
    def create_one(
        cls,
        document: Union[dict, InCollectionModel, BaseModel],
        expand: list[str] = None,
    ) -> OutCollectionModel:
        inserted_id = cls._mongo_client.insert_one(cls._in_model, document)
        return cls.get_by_id(
            inserted_id,
            expand=expand,
        )

    @classmethod
    def create_many(
        cls,
        documents: List[Union[dict, InCollectionModel, BaseModel]],
        expand: list[str] = None,
    ) -> List[OutCollectionModel]:
        inserted_ids = cls._mongo_client.insert_many(cls._in_model, documents)
        return cls.get_by_ids(
            inserted_ids,
            expand=expand,
        )

    @classmethod
    def get_one(
        cls,
        query: dict = {},
        sort: dict = None,
        expand: list[str] = None,
        skip: int = 0,
    ) -> Optional[OutCollectionModel]:
        return cls._mongo_client.find_one(
            cls._out_model,
            query,
            sort=sort,
            expand=expand,
            skip=skip,
        )

    @classmethod
    def get_by_id(
        cls,
        id: Union[str, ObjectId],
        expand: list[str] = None,
    ) -> Optional[OutCollectionModel]:
        return cls.get_one({"_id": ObjectId(id)}, expand=expand)

    @classmethod
    def get_many(
        cls,
        query: dict = {},
        sort: dict = None,
        skip: int = 0,
        limit: int = None,
        expand: List[str] = None,
    ) -> List[OutCollectionModel]:
        return cls._mongo_client.find_many(
            cls._out_model,
            query,
            sort=sort,
            skip=skip,
            limit=limit,
            expand=expand,
        )

    @classmethod
    def get_by_ids(
        cls,
        ids: List[Union[str, ObjectId]],
        sort: dict = None,
        skip: int = 0,
        limit: int = None,
        expand: List[str] = None,
    ) -> List[OutCollectionModel]:
        return cls.get_many(
            {"_id": {"$in": [ObjectId(id) for id in ids]}},
            sort=sort,
            skip=skip,
            limit=limit,
            expand=expand,
        )

    @classmethod
    def update_one(
        cls,
        query: dict,
        update: Union[dict, BaseModel],
        expand: list[str] = None,
    ) -> OutCollectionModel:
        update = cls._prepare_update(update)
        cls._mongo_client.update_one(cls._in_model, query, update)
        return cls.get_one(query, expand=expand)

    @classmethod
    def update(
        cls,
        model: OutCollectionModel,
        expand: list[str] = None,
    ) -> OutCollectionModel:
        return cls.update_one(
            {"_id": model.id},
            model.model_dump(),
            expand=expand,
        )

    @classmethod
    def update_by_id(
        cls,
        id: Union[str, ObjectId],
        update: Union[dict, BaseModel],
        expand: list[str] = None,
    ) -> OutCollectionModel:
        return cls.update_one(
            {"_id": ObjectId(id)},
            update,
            expand=expand,
        )

    @classmethod
    def update_many(
        cls,
        query: dict,
        update: Union[dict, BaseModel],
        expand: list[str] = None,
    ) -> List[OutCollectionModel]:
        update = cls._prepare_update(update)
        cls._mongo_client.update_many(cls._in_model, query, update)
        return cls.get_many(
            query,
            expand=expand,
        )

    @classmethod
    def update_by_ids(
        cls,
        ids: List[Union[str, ObjectId]],
        update: Union[dict, BaseModel],
        expand: list[str] = None,
    ) -> List[OutCollectionModel]:
        return cls.update_many(
            {"_id": {"$in": [ObjectId(id) for id in ids]}},
            update,
            expand=expand,
        )

    @classmethod
    def delete(cls, model: OutCollectionModel) -> int:
        return cls.delete_by_id(model.id)

    @classmethod
    def delete_one(cls, query: dict) -> int:
        return cls._mongo_client.delete_one(cls._in_model, query)

    @classmethod
    def delete_by_id(cls, id: Union[str, ObjectId]) -> int:
        return cls.delete_one({"_id": ObjectId(id)})

    @classmethod
    def delete_many(cls, query: dict) -> int:
        return cls._mongo_client.delete_many(cls._in_model, query)

    @classmethod
    def delete_by_ids(cls, ids: List[Union[str, ObjectId]]) -> int:
        return cls.delete_many({"_id": {"$in": [ObjectId(id) for id in ids]}})

    @classmethod
    def count(cls, query: dict) -> int:
        return cls._mongo_client.count(cls._in_model, query)

    @classmethod
    def aggregate(
        cls, pipeline: List[dict], parse: bool = False
    ) -> Union[List[OutCollectionModel], List[Union[dict, Any]]]:
        return cls._mongo_client.aggregate(cls._out_model, pipeline, parse=parse)

    @classmethod
    def get_only_ids(
        cls,
        query: dict = {},
        sort: dict = None,
        skip: int = 0,
        limit: int = None,
    ) -> List[ObjectId]:

        pipeline = PipelineBuilder.build_simple_pipeline(
            query,
            sort=sort,
            skip=skip,
            limit=limit,
            project={"_id": 1},
        )

        documents = cls.aggregate(pipeline)
        return [doc["_id"] for doc in documents]
