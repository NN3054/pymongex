from pymongo import MongoClient
from typing import Any, Dict, List, Optional, Union, Type
from bson import ObjectId

from pydantic import BaseModel
from ..models.collection import (
    InCollectionModel,
    OutCollectionModel,
    CollectionModel,
)
from ..singleton.sync_mongo_singleton import (
    MongoSyncClientSingleton,
)

from .base_client import BaseMongoClient


class SyncMongoClient(BaseMongoClient):
    _client: MongoClient

    def __init__(self):
        self._client = None

    def _initialize_client(self):
        if self._client is None:
            self._client = MongoSyncClientSingleton.get_client()

    def _get_collection_client(
        self,
        model: Union[
            InCollectionModel,
            OutCollectionModel,
            CollectionModel,
        ],
    ) -> MongoClient:
        self._initialize_client()
        db_name = model.get_database()
        collection_name = model.get_collection()
        return self._client[db_name][collection_name]

    def _aggregate(
        self,
        model: Type[OutCollectionModel],
        pipeline: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        self._initialize_client()
        client = self._get_collection_client(model)
        cursor = client.aggregate(pipeline)
        documents = list(cursor)
        return documents

    def insert_one(
        self,
        model: Type[InCollectionModel],
        document: Union[Dict, InCollectionModel, BaseModel],
    ) -> ObjectId:
        self._initialize_client()
        if not isinstance(document, InCollectionModel):
            if isinstance(document, BaseModel):
                document = model(**document.dict())
            elif isinstance(document, dict):
                document = model(**document)
            else:
                raise ValueError(
                    "Document must be of the same type as the model or a dict"
                )
        client = self._get_collection_client(model)
        db_dict = document.db_dict()
        result = client.insert_one(db_dict)
        return result.inserted_id

    def insert_many(
        self,
        model: Type[InCollectionModel],
        documents: List[Union[Dict, InCollectionModel, BaseModel]],
    ) -> List[ObjectId]:
        self._initialize_client()
        if all(isinstance(doc, InCollectionModel) for doc in documents):
            documents = [doc.db_dict() for doc in documents]
        elif all(isinstance(doc, BaseModel) for doc in documents):
            documents = [model(**doc.dict()).db_dict() for doc in documents]
        elif all(isinstance(doc, dict) for doc in documents):
            documents = [model(**doc).db_dict() for doc in documents]
        else:
            raise ValueError(
                "All documents must be of the same type as the model or a dict"
            )

        client = self._get_collection_client(model)
        result = client.insert_many(documents)
        return result.inserted_ids

    def find_one(
        self,
        model: Type[OutCollectionModel],
        query: dict,
        sort: dict = None,
        expand: Optional[List[str]] = None,
        skip: int = 0,
    ) -> Optional[OutCollectionModel]:
        pipeline = self._prepare_find_pipeline(
            model,
            query,
            sort,
            skip=skip,
            limit=1,
            expand=expand,
        )
        documents = self._aggregate(model, pipeline)
        document = documents[0] if documents else None
        if document:
            return self._to_model(model=model, document=document)

    def find_many(
        self,
        model: Type[OutCollectionModel],
        query: dict = {},
        sort: dict = None,
        skip: int = 0,
        limit: int = None,
        expand: Optional[List[str]] = None,
    ) -> List[OutCollectionModel]:
        pipeline = self._prepare_find_pipeline(
            model, query, sort, skip, limit, expand=expand
        )
        documents = self._aggregate(model, pipeline)
        return self._docs_to_models(model, documents)

    def update_one(
        self,
        model: Type[InCollectionModel],
        query: Dict,
        update: Dict,
    ) -> int:
        self._initialize_client()
        client = self._get_collection_client(model)
        self._add_updated_at(update=update)
        result = client.update_one(query, update)
        return result.modified_count

    def update_many(
        self,
        model: Type[InCollectionModel],
        query: Dict,
        update: Dict,
    ) -> int:
        self._initialize_client()
        client = self._get_collection_client(model)
        self._add_updated_at(update=update)
        result = client.update_many(query, update)
        return result.modified_count

    def delete_one(self, model: Type[InCollectionModel], query: Dict) -> int:
        self._initialize_client()
        client = self._get_collection_client(model)
        result = client.delete_one(query)
        return result.deleted_count

    def delete_many(self, model: Type[InCollectionModel], query: Dict) -> int:
        self._initialize_client()
        client = self._get_collection_client(model)
        result = client.delete_many(query)
        return result.deleted_count

    def aggregate(
        self,
        model: Type[OutCollectionModel],
        pipeline: List[Dict[str, Any]],
        parse: bool = False,
        map_id: bool = False,
    ) -> Union[List[OutCollectionModel], List[Union[Dict[str, Any], Any]]]:
        if map_id:
            map_id_stage = {"$addFields": {"id": "$_id"}}
            project_id_stage = {"$project": {"_id": 0}}
            pipeline.append(map_id_stage)
            pipeline.append(project_id_stage)

        documents = self._aggregate(model, pipeline)
        if not parse:
            return documents
        return self._docs_to_models(model, documents)

    def count(
        self,
        model: Type[InCollectionModel],
        query: Dict,
    ) -> int:
        self._initialize_client()
        client = self._get_collection_client(model)
        return client.count_documents(query)
