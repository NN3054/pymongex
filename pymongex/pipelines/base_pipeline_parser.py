import json
import os
from typing import Any, List


class BasePipelineParser:

    def __init__(self, __file__) -> None:
        self.this_dir = os.path.dirname(os.path.realpath(__file__))
        self.pipelines_fp = os.path.join(self.this_dir, "pipelines.json")

    def _read_raw_pipeline(self, pipeline_name: str) -> List[dict]:
        with open(self.pipelines_fp, "r") as f:
            pipelines = json.load(f)
        return pipelines[pipeline_name]

    def _replace_key_value_in_dict(self, d: dict, replace_info: dict) -> dict:
        for key, value in d.items():
            if isinstance(value, dict):
                self._replace_key_value_in_dict(value, replace_info)
            elif isinstance(value, str):
                if value in replace_info:
                    d[key] = replace_info[value]
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        self._replace_key_value_in_dict(item, replace_info)
                    elif isinstance(item, str):
                        if item in replace_info:
                            d[key][i] = replace_info[item]
        return d

    def _replace_key_value_in_pipeline(
        self, pipeline: List[dict], replace_info: dict
    ) -> List[dict]:
        for stage in pipeline:
            self._replace_key_value_in_dict(stage, replace_info)
        return pipeline

    def read_pipeline(
        self,
        pipeline_name: str,
        query: dict = None,
        limit: int = None,
        skip: int = 0,
        replace_info: dict[str, Any] = None,
    ) -> List[dict]:
        """
        Reads and returns a pipeline based on the provided parameters.

        Args:
            pipeline_name (str): The name of the pipeline to read.
            query (dict, optional): The query to filter the pipeline. Defaults to None.
            limit (int, optional): The maximum number of documents to return. Defaults to None.
            skip (int, optional): The number of documents to skip. Defaults to 0.
            replace_info (dict[str, Any], optional): The key-value pairs to replace in the pipeline.
            Key is the value to replace and the value is the new actual value. Defaults to None.

        Returns:
            List[dict]: The resulting pipeline.

        """
        pipeline = []
        if query:
            pipeline.append({"$match": query})

        pipeline.extend(self._read_raw_pipeline(pipeline_name))

        if replace_info:
            pipeline = self._replace_key_value_in_pipeline(pipeline, replace_info)

        if skip and "$skip" not in pipeline:
            pipeline.append({"$skip": skip})
        if limit and "$limit" not in pipeline:
            pipeline.append({"$limit": limit})

        return pipeline
