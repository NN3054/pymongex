from .collection import Collection


class BaseDatabase:
    def __init__(self, db_name: str):
        self._db_name: str = db_name

    def add_collection(self, collection_name: str):
        return Collection(db=self._db_name, name=collection_name)
