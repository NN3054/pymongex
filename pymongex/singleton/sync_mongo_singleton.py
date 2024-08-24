from pymongo import MongoClient
from pymongo.server_api import ServerApi

from ..config import get_connection_string


class MongoSyncClientSingleton:
    _instance = None
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoSyncClientSingleton, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_client(cls):
        if cls._client is None:
            connection_string = get_connection_string()
            if connection_string is None:
                raise ValueError("Connection string is not set.")
            cls._client = MongoClient(connection_string, server_api=ServerApi("1"))
        return cls._client

    @classmethod
    def initialize(cls):
        client = cls.get_client()
        # Perform a simple query to establish connection
        client.admin.command("ping")
        print("Sync Connected to MongoDB")

    @classmethod
    def close_client(cls):
        if cls._client is not None:
            cls._client.close()
            cls._client = None
