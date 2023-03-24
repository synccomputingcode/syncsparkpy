"""
Models used throughout this SDK
"""

from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel, Field, root_validator, validator
from pydantic.generics import GenericModel
from pydantic.types import UUID4


class Preference(str, Enum):
    PERFORMANCE = "performance"
    BALANCED = "balanced"
    ECONOMY = "economy"


class Platform(str, Enum):
    AWS_EMR = "aws-emr"
    AWS_DATABRICKS = "aws-databricks"


class Project(BaseModel):
    id: UUID4 = Field(..., description="project UUID")
    app_id: str = Field(
        ...,
        description="a string that uniquely identifies an application to the owner of that application",
    )
    description: str | None = Field(description="Additional information on the app, or project")
    s3_url: str | None = Field(description="location of data from runs of the application")
    prediction_preference: Preference | None = Field(description="preferred prediction to apply")


class Error(BaseModel):
    code: str
    message: str

    def __str__(self):
        return f"{self.code}: {self.message}"


class PredictionError(Error):
    code: str = Field("Prediction Error", const=True)


class ProjectError(Error):
    code: str = Field("Project Error", const=True)


class EMRError(Error):
    code: str = Field("EMR Error", const=True)


class DatabricksError(Error):
    code: str = Field("Databricks Error", const=True)


class DatabricksAPIError(Error):
    @root_validator(pre=True)
    def validate_error(cls, values):
        values["code"] = "Databricks API Error"
        if values.get("error_code"):
            values["message"] = f"{values['error_code']}: {values.get('message')}"

        return values


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
