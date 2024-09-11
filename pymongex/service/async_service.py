from typing import Any, List, Optional, Type, Union

from bson import ObjectId
from pydantic import BaseModel

from ..clients.async_client import AsyncMongoClient
from ..models.collection import (
    InCollectionModel,
    OutCollectionModel,
)
from ..pipelines import PipelineBuilder
from .base_service import BaseService


class AsyncBaseService(BaseService):
    _mongo_client: AsyncMongoClient = AsyncMongoClient()
    _in_model: Type[InCollectionModel]
    _out_model: Type[OutCollectionModel]

    @classmethod
    async def create_one(
        cls,
        document: Union[dict, InCollectionModel, BaseModel],
        expand: list[str] = None,
    ) -> OutCollectionModel:
        inserted_id = await cls._mongo_client.insert_one(cls._in_model, document)
        return await cls.get_by_id(
            inserted_id,
            expand=expand,
        )

    @classmethod
    async def create_many(
        cls,
        documents: List[Union[dict, InCollectionModel, BaseModel]],
        expand: list[str] = None,
    ) -> List[OutCollectionModel]:
        inserted_ids = await cls._mongo_client.insert_many(cls._in_model, documents)
        return await cls.get_by_ids(
            inserted_ids,
            expand=expand,
        )

    @classmethod
    async def get_one(
        cls,
        query: dict = {},
        sort: dict = None,
        expand: list[str] = None,
        skip: int = 0,
    ) -> Optional[OutCollectionModel]:
        return await cls._mongo_client.find_one(
            cls._out_model,
            query,
            sort=sort,
            expand=expand,
            skip=skip,
        )

    @classmethod
    async def get_by_id(
        cls,
        id: Union[str, ObjectId],
        expand: list[str] = None,
    ) -> Optional[OutCollectionModel]:
        return await cls.get_one({"_id": ObjectId(id)}, expand=expand)

    @classmethod
    async def get_many(
        cls,
        query: dict = {},
        sort: dict = None,
        skip: int = 0,
        limit: int = None,
        expand: List[str] = None,
    ) -> List[OutCollectionModel]:
        return await cls._mongo_client.find_many(
            cls._out_model,
            query,
            sort=sort,
            skip=skip,
            limit=limit,
            expand=expand,
        )

    @classmethod
    async def get_by_ids(
        cls,
        ids: List[Union[str, ObjectId]],
        sort: dict = None,
        skip: int = 0,
        limit: int = None,
        expand: List[str] = None,
    ) -> List[OutCollectionModel]:
        return await cls.get_many(
            {"_id": {"$in": [ObjectId(id) for id in ids]}},
            sort=sort,
            skip=skip,
            limit=limit,
            expand=expand,
        )

    @classmethod
    async def update_one(
        cls,
        query: dict,
        update: Union[dict, BaseModel],
        expand: list[str] = None,
    ) -> OutCollectionModel:
        update = cls._prepare_update(update)
        await cls._mongo_client.update_one(cls._in_model, query, update)
        return await cls.get_one(query, expand=expand)

    @classmethod
    async def update(
        cls,
        model: InCollectionModel,
        update: Union[dict, BaseModel],
        expand: list[str] = None,
    ) -> OutCollectionModel:
        return await cls.update_one(
            {"_id": model.id},
            update,
            expand=expand,
        )

    @classmethod
    async def update_by_id(
        cls,
        id: Union[str, ObjectId],
        update: Union[dict, BaseModel],
        expand: list[str] = None,
    ) -> OutCollectionModel:
        return await cls.update_one(
            {"_id": ObjectId(id)},
            update,
            expand=expand,
        )

    @classmethod
    async def update_many(
        cls,
        query: dict,
        update: Union[dict, BaseModel],
        expand: list[str] = None,
    ) -> List[OutCollectionModel]:
        update = cls._prepare_update(update)
        await cls._mongo_client.update_many(cls._in_model, query, update)
        return await cls.get_many(
            query,
            expand=expand,
        )

    @classmethod
    async def update_by_ids(
        cls,
        ids: List[Union[str, ObjectId]],
        update: Union[dict, BaseModel],
        expand: list[str] = None,
    ) -> List[OutCollectionModel]:
        return await cls.update_many(
            {"_id": {"$in": [ObjectId(id) for id in ids]}},
            update,
            expand=expand,
        )

    @classmethod
    async def delete(cls, model: OutCollectionModel) -> int:
        return await cls.delete_by_id(model.id)

    @classmethod
    async def delete_one(cls, query: dict) -> int:
        return await cls._mongo_client.delete_one(cls._in_model, query)

    @classmethod
    async def delete_by_id(cls, id: Union[str, ObjectId]) -> int:
        return await cls.delete_one({"_id": ObjectId(id)})

    @classmethod
    async def delete_many(cls, query: dict) -> int:
        return await cls._mongo_client.delete_many(cls._in_model, query)

    @classmethod
    async def delete_by_ids(cls, ids: List[Union[str, ObjectId]]) -> int:
        return await cls.delete_many({"_id": {"$in": [ObjectId(id) for id in ids]}})

    @classmethod
    async def count(cls, query: dict) -> int:
        return await cls._mongo_client.count(cls._in_model, query)

    @classmethod
    async def aggregate(
        cls, pipeline: List[dict], parse: bool = False
    ) -> Union[List[OutCollectionModel], List[Union[dict, Any]]]:
        return await cls._mongo_client.aggregate(cls._out_model, pipeline, parse=parse)

    @classmethod
    async def get_only_ids(
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

        documents = await cls.aggregate(pipeline)
        return [doc["_id"] for doc in documents]
