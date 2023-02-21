"""
Models used throughout this SDK
"""

from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel, Field, validator
from pydantic.generics import GenericModel
from pydantic.types import UUID4


class Preference(str, Enum):
    PERFORMANCE = "performance"
    BALANCED = "balanced"
    ECONOMY = "economy"


class Platform(str, Enum):
    EMR = ("emr", "aws-emr")
    DATABRICKS = ("databricks", "aws-databricks")

    def __new__(cls, name: str, api_name: str):
        obj = str.__new__(cls, name)
        obj._value_ = name

        obj.__api_name = api_name

        return obj

    @property
    def api_name(self) -> str:
        return self.__api_name


class Project(BaseModel):
    id: UUID4 = Field(..., description="project UUID")
    user_id: UUID4
    app_id: str = Field(
        ...,
        description="a string that uniquely identifies an application to the owner of that application",
    )
    description: str | None = Field(
        None, description="Additional information on the app, or project"
    )
    s3_url: str | None = Field(None, description="location of data from runs of the application")
    prediction_preference: Preference | None = Field(
        None, description="preferred prediction to apply"
    )


class Error(BaseModel):
    code: str
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
