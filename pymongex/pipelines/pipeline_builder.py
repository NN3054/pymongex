from typing import Any, Dict, List, Optional, Type

from ..models.collection import OutCollectionModel


class PipelineBuilder:
    def __init__(
        self,
        model: Type[OutCollectionModel],
        query: dict,
        sort: Optional[Any] = None,
        skip: int = 0,
        limit: Optional[int] = None,
        expand: Optional[List[str]] = None,
        project_model: bool = True,
    ):
        self.model = model
        self.query = query
        self.sort = sort
        self.skip = skip
        self.limit = limit
        self.expand = expand  # if expand is not None else model.get_expandable_fields()
        self.project_model = project_model
        self.pipeline: List[Dict[str, Any]] = []
        self.final_projection = model.get_projection() if self.project_model else {}
        self.db = model.get_database()

    def build_pipeline(self) -> List[Dict[str, Any]]:
        self._validate_parameters()
        self._add_match_stage()
        self._add_sort_stage()
        self._add_skip_stage()
        self._add_limit_stage()
        self._add_expand_stages()
        self._add_custom_pipelines()
        self._add_final_projection()
        return self.pipeline

    def _validate_parameters(self):
        if self.limit is not None and self.limit <= 0:
            raise ValueError("limit has to be a strict positive value or None")
        if self.skip < 0:
            raise ValueError("skip has to be a positive integer")
        if self.sort is not None and self.sort != {}:
            for key, value in self.sort.items():
                if value not in [1, -1]:
                    raise ValueError("sort values must be either 1 or -1")

    def _add_match_stage(self):
        self.pipeline.append({"$match": self.query})

    def _add_sort_stage(self):
        if self.sort is not None and self.sort != {}:
            self.pipeline.append({"$sort": self.sort})

    def _add_skip_stage(self):
        if self.skip > 0:
            self.pipeline.append({"$skip": self.skip})

    def _add_limit_stage(self):
        if self.limit is not None and self.limit > 0:
            self.pipeline.append({"$limit": self.limit})

    def _add_expand_stages(self):
        if not self.expand:
            return
        for field in self.expand:
            if field in self.model.get_keys():
                self._handle_expand_field(field)

    def _handle_expand_field(self, field: str):
        field_type: OutCollectionModel = self.model.get_field_type(field_name=field)

        expand_collection = field_type.get_collection()
        local_field = self.model.get_field_local_field(field_name=field)
        foreign_field = self.model.get_field_foreign_field(field_name=field)

        if field_type.get_database() != self.db:
            raise Exception(
                f"Can not make expand & lookup cross-db. Collections must be in the same database {self.db}."
            )

        if expand_collection and local_field and foreign_field:
            self.pipeline.append(
                {
                    "$lookup": {
                        "from": expand_collection,
                        "localField": local_field,
                        "foreignField": foreign_field,
                        "as": field,
                    }
                }
            )

            self.pipeline.append(
                {
                    "$unwind": {
                        "path": f"${field}",
                        "preserveNullAndEmptyArrays": True,
                    }
                }
            )

            if self.project_model:
                nested_projection = self.model.get_nested_projection(field)
                self.final_projection.update(nested_projection)
            self._handle_expanded_custom_pipelines(field)

    def _handle_expanded_custom_pipelines(self, field: str):
        # only handles supports pipeline which fields/info of this document
        nested_model: OutCollectionModel = self.model.get_field_type(field_name=field)
        custom_pipelines = nested_model.get_custom_pipelines()
        local_field = self.model.get_field_local_field(field_name=field)
        foreign_field = self.model.get_field_foreign_field(field_name=field)

        for nested_field, custom_pipeline in custom_pipelines.items():

            nested_pipeline = [
                {
                    "$lookup": {
                        "from": nested_model.get_collection(),
                        "localField": local_field,
                        "foreignField": foreign_field,
                        "pipeline": custom_pipeline,
                        "as": f"{field}.{nested_field}",
                    }
                },
            ]

            field_type = nested_model.get_field_type(field_name=nested_field)
            if field_type != list:
                nested_pipeline.append(
                    {
                        "$unwind": {
                            "path": f"${field}.{nested_field}",
                            "preserveNullAndEmptyArrays": True,
                        }
                    }
                )
                if field_type != dict:
                    nested_pipeline.append(
                        {
                            "$addFields": {
                                f"{field}.{nested_field}": f"${field}.{nested_field}.{nested_field}"
                            }
                        }
                    )

            self.pipeline.extend(nested_pipeline)

    def _add_custom_pipelines(self):
        # only supports pipelines on this document
        custom_pipelines = self.model.get_custom_pipelines()
        for field, custom_pipeline in custom_pipelines.items():
            # Directly apply the custom pipeline stages to the main pipeline
            for stage in custom_pipeline:
                self.pipeline.append(stage)

    def _add_final_projection(self):

        if self.final_projection:
            if self.project_model:
                self.pipeline.append({"$project": self.final_projection})

    @staticmethod
    def build_simple_pipeline(
        query: dict,
        sort: Optional[dict] = None,
        skip: int = 0,
        limit: Optional[int] = None,
        project: Optional[dict] = None,
    ) -> List[Dict[str, Any]]:

        if limit is not None and limit < 0:
            raise ValueError("Limit must be greater than 0")

        pipeline = [{"$match": query}]
        if sort:
            pipeline.append({"$sort": sort})
        if skip:
            pipeline.append({"$skip": skip})
        if limit:
            pipeline.append({"$limit": limit})
        if project:
            pipeline.append({"$project": project})

        return pipeline
