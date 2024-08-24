from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, Union

from bson import ObjectId
from pydantic import BaseModel, TypeAdapter

from ..models.collection import (
    InCollectionModel,
    OutCollectionModel,
)
from ..pipelines.pipeline_builder import PipelineBuilder
from ..utils import utc_now


class BaseMongoClient(ABC):

    def _prepare_find_pipeline(
        self,
        model: Type[InCollectionModel],
        query: dict,
        sort: Optional[Any] = None,
        skip: int = 0,
        limit: Optional[int] = None,
        expand: Optional[List[str]] = None,
        project_model: bool = True,
    ) -> List[Dict[str, Any]]:

        builder = PipelineBuilder(
            model=model,
            query=query,
            sort=sort,
            skip=skip,
            limit=limit,
            expand=expand,
            project_model=project_model,
        )
        return builder.build_pipeline()

    def _to_model(
        self,
        model: Type[OutCollectionModel],
        document: Dict[str, Any],
    ) -> OutCollectionModel:
        ta = TypeAdapter(model)
        return ta.validate_python(document, from_attributes=True)

    def _docs_to_models(
        self,
        model: Type[OutCollectionModel],
        mongodb_cursors: List[Dict[str, Any]],
    ):
        ta = TypeAdapter(list[model])
        return ta.validate_python(mongodb_cursors, from_attributes=True)

    def _add_updated_at(self, update: dict) -> None:
        update["$set"] = update.get("$set", {})
        update["$set"]["updated_at"] = utc_now()

    @abstractmethod
    def insert_one(
        self,
        model: InCollectionModel,
        document: Union[Dict, InCollectionModel, BaseModel],
    ) -> ObjectId:
        pass

    @abstractmethod
    def insert_many(
        self,
        model: InCollectionModel,
        documents: List[Union[Dict, InCollectionModel, BaseModel]],
    ) -> List[ObjectId]:
        pass

    @abstractmethod
    def find_one(
        self, model: OutCollectionModel, query: dict
    ) -> Optional[InCollectionModel]:
        pass

    @abstractmethod
    def find_many(
        self,
        model: OutCollectionModel,
        query: dict = {},
        sort: dict = None,
        skip: int = 0,
        limit: int = None,
    ) -> List[InCollectionModel]:
        pass

    @abstractmethod
    def update_one(self, model: InCollectionModel, query: Dict, update: Dict) -> int:
        pass

    @abstractmethod
    def update_many(self, model: InCollectionModel, query: Dict, update: Dict) -> int:
        pass

    @abstractmethod
    def delete_one(self, model: InCollectionModel, query: Dict) -> int:
        pass

    @abstractmethod
    def delete_many(self, model: InCollectionModel, query: Dict) -> int:
        pass

    @abstractmethod
    def aggregate(
        self,
        model: OutCollectionModel,
        pipeline: List[Dict[str, Any]],
        parse: bool = False,
        map_id: bool = False,
    ) -> Union[List[OutCollectionModel], List[Union[Dict[str, Any], Any]]]:
        pass

    @abstractmethod
    def count(self, model: InCollectionModel, query: Dict) -> int:
        pass
