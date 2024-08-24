from typing import Optional

from .config import set_connection_string

__all__ = ["set_connection_string"]


from .constants import *
from .models import *
from .pipelines import BasePipelineParser, PipelineBuilder
from .service import AsyncBaseService, SyncBaseService
from .singleton import MongoAsyncClientSingleton, MongoSyncClientSingleton
from .storage import BaseDatabase, Collection


async def async_connect(connection_string: Optional[str] = None):
    if connection_string is not None and isinstance(connection_string, str):
        set_connection_string(connection_string)
    await MongoAsyncClientSingleton.initialize()


def connect(connection_string: Optional[str] = None):
    if connection_string is not None and isinstance(connection_string, str):
        set_connection_string(connection_string)
    MongoSyncClientSingleton.initialize()


async def async_disconnect():
    await MongoAsyncClientSingleton.close_client()


def disconnect():
    MongoSyncClientSingleton.close_client()
