"""
Models used throughout this SDK
"""

from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel, Field, validator
from pydantic.generics import GenericModel


class Preference(str, Enum):
    PERFORMANCE = "performance"
    BALANCED = "balanced"
    ECONOMY = "economy"


class APIKey(BaseModel):
    id: str = Field(..., alias="api_key_id")
    secret: str = Field(..., alias="api_key_secret")


class Configuration(BaseModel):
    state_url: str = Field(..., description="s3 or file URL for state information")
    prediction_preference: Preference


class Error(BaseModel):
    code: int
    message: str


DataType = TypeVar("DataType")


class Response(GenericModel, Generic[DataType]):
    result: DataType | None
    error: Error | None

    @validator("error", always=True)
    def check_consistency(cls, err, values):
        if err is not None and values["result"] is not None:
            raise ValueError("must not provide both result and error")
        if err is None and values.get("result") is None:
            raise ValueError("must provide result or error")
        return err
