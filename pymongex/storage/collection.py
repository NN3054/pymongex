from pydantic import BaseModel, Field


class Collection(BaseModel):
    db: str = Field(..., title="Database name")
    name: str = Field(..., title="Collection name")
