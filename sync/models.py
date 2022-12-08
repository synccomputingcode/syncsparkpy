"""
Models used throughout this SDK
"""

from enum import Enum
from typing import Generic, TypeVar
from urllib.parse import urlparse

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
    default_project_url: str = Field(None, description="default location for Sync project data")
    default_prediction_preference: Preference | None

    @validator("default_project_url")
    def validate_url(url):
        # There are valid S3 URLs (e.g. with spaces) not supported by Pydantic URL types: https://docs.pydantic.dev/usage/types/#urls
        # Hence the manual validation here
        parsed_url = urlparse(url)

        if parsed_url.scheme != "s3":
            raise ValueError("Only S3 URLs please!")

        return url


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
