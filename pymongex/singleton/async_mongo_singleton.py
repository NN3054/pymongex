from motor.motor_asyncio import AsyncIOMotorClient

from ..config import get_connection_string


class MongoAsyncClientSingleton:
    _instance = None
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoAsyncClientSingleton, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_client(cls):
        if cls._client is None:
            connection_string = get_connection_string()
            if connection_string is None:
                raise ValueError("Connection string is not set.")
            cls._client = AsyncIOMotorClient(connection_string)
        return cls._client

    @classmethod
    async def initialize(cls):
        client = cls.get_client()
        # Perform a simple query to establish connection
        await client.admin.command("ping")
        print("Async Connected to MongoDB")

    @classmethod
    async def close_client(cls):
        if cls._client is not None:
            cls._client.close()
            cls._client = None
