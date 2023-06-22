"""
Models used throughout this SDK
"""

from enum import Enum, unique
from typing import Generic, TypeVar, Union

from pydantic import BaseModel, Field, root_validator, validator
from pydantic.generics import GenericModel


class Preference(str, Enum):
    PERFORMANCE = "performance"
    BALANCED = "balanced"
    ECONOMY = "economy"


class Platform(str, Enum):
    AWS_EMR = "aws-emr"
    AWS_DATABRICKS = "aws-databricks"


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


@unique
class DatabricksPlanType(str, Enum):
    STANDARD = "Standard"
    PREMIUM = "Premium"
    ENTERPRISE = "Enterprise"


@unique
class DatabricksComputeType(str, Enum):
    ALL_PURPOSE_COMPUTE = "All-Purpose Compute"
    JOBS_COMPUTE = "Jobs Compute"
    JOBS_COMPUTE_LIGHT = "Jobs Compute Light"


class DatabricksClusterReport(BaseModel):
    plan_type: DatabricksPlanType
    compute_type: DatabricksComputeType
    cluster: dict
    cluster_events: dict
    instances: Union[dict, None]


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
    result: Union[DataType, None]
    error: Union[Error, None]

    @validator("error", always=True)
    def check_consistency(cls, err, values):
        if err is not None and values["result"] is not None:
            raise ValueError("must not provide both result and error")
        if err is None and values.get("result") is None:
            raise ValueError("must provide result or error")
        return err
