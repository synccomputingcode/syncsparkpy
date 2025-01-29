"""
Models used throughout this SDK
"""

import copy
import json
from dataclasses import dataclass
from enum import Enum, unique
from typing import Callable, Dict, Generic, List, Optional, TypeVar, Union

from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, root_validator, validator
from pydantic.generics import GenericModel


class Platform(str, Enum):
    AWS_DATABRICKS = "aws-databricks"
    AZURE_DATABRICKS = "azure-databricks"


@unique
class HostingType(str, Enum):
    SYNC_HOSTED = "sync-hosted"
    SELF_HOSTED = "self-hosted"


@unique
class WorkspaceCollectionTypeEnum(str, Enum):
    REMOTE = "remote"
    HOSTED = "hosted"


@unique
class WorkspaceMonitoringTypeEnum(str, Enum):
    WEBHOOK = "webhook"
    HYPERVISOR = "hypervisor"


@unique
class ComputeProvider(str, Enum):
    AWS = "aws"
    AZURE = "azure"


class AccessStatusCode(str, Enum):
    GREEN = "OK"
    YELLOW = "Action recommended"
    RED = "Action required"


@dataclass
class AccessReportLine:
    name: str
    status: AccessStatusCode
    message: Union[str, None]


class AccessReport(List[AccessReportLine]):
    def __str__(self):
        return "\n".join(f"{line.name}\n  {line.status}: {line.message}" for line in self)

    def add_boto_method_call(
        self,
        method: Callable,
        error_status: AccessStatusCode = AccessStatusCode.RED,
        **params,
    ):
        line = None
        name = f"{method.__self__.meta._service_model._service_description['metadata']['serviceId']} {''.join(word.capitalize() for word in method.__func__.__name__.split('_'))}"
        try:
            method(**params)
        except ClientError as error:  # as when the code is 'AccessDeniedException', a bad parameter like a mistyped cluster ID yields a botocore.errorfactory.InvalidRequestException with code 'InvalidRequestException'
            if error.response.get("Error", {}).get("Code") != "DryRunOperation":
                line = AccessReportLine(name, error_status, str(error))
        except Exception as exc:
            line = AccessReportLine(name, error_status, str(exc))

        if not line:
            line = AccessReportLine(
                name,
                AccessStatusCode.GREEN,
                f"{method.__func__.__name__} call succeeded",
            )

        self.append(line)
        return line


class Error(BaseModel):
    code: str
    message: str

    def __str__(self):
        return f"{self.code}: {self.message}"


class ProjectError(Error):
    code: str = Field("Project Error", const=True)


class RecommendationError(Error):
    code: str = Field("Recommendation Error", const=True)


class SubmissionError(Error):
    code: str = Field("Submission Error", const=True)


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
    tasks: List[dict]
    instances: Union[List[dict], None]
    instance_timelines: Union[List[dict], None]


class AWSDatabricksClusterReport(DatabricksClusterReport):
    volumes: Union[List[dict], None]


class AzureDatabricksClusterReport(DatabricksClusterReport):
    pass


class DatabricksError(Error):
    code: str = Field("Databricks Error", const=True)


class MissingOrIncompleteEventlogError(Error):
    dbfs_eventlog_file_size: Union[int, None] = None
    code: str = Field("Retryable Databricks Error", const=True)
    message: str = Field("Event log was missing or incomplete. Please retry.", const=True)


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


class S3ClusterLogConfiguration(BaseModel):
    destination: str
    region: str
    enable_encryption: bool
    canned_acl: str


class DBFSClusterLogConfiguration(BaseModel):
    destination: str


class AWSProjectConfiguration(BaseModel):
    node_type_id: str
    driver_node_type: str
    custom_tags: Dict
    cluster_log_conf: Union[S3ClusterLogConfiguration, DBFSClusterLogConfiguration]
    cluster_name: str
    num_workers: int
    spark_version: str
    runtime_engine: str
    autoscale: Dict
    spark_conf: Dict
    aws_attributes: Dict
    spark_env_vars: Dict


class AzureProjectConfiguration(BaseModel):
    node_type_id: str
    driver_node_type: str
    cluster_log_conf: DBFSClusterLogConfiguration
    custom_tags: Dict
    num_workers: int
    spark_conf: Dict
    spark_version: str
    runtime_engine: str
    azure_attributes: Dict


class AwsRegionEnum(str, Enum):
    US_EAST_1 = "us-east-1"
    US_EAST_2 = "us-east-2"
    US_WEST_1 = "us-west-1"
    US_WEST_2 = "us-west-2"
    AF_SOUTH_1 = "af-south-1"
    AP_EAST_1 = "ap-east-1"
    AP_SOUTH_1 = "ap-south-1"
    AP_SOUTH_2 = "ap-south-2"
    AP_NORTHEAST_1 = "ap-northeast-1"
    AP_NORTHEAST_2 = "ap-northeast-2"
    AP_NORTHEAST_3 = "ap-northeast-3"
    AP_SOUTHEAST_1 = "ap-southeast-1"
    AP_SOUTHEAST_2 = "ap-southeast-2"
    AP_SOUTHEAST_3 = "ap-southeast-3"
    AP_SOUTHEAST_4 = "ap-southeast-4"
    CA_CENTRAL_1 = "ca-central-1"
    CA_WEST_1 = "ca-west-1"
    EU_CENTRAL_1 = "eu-central-1"
    EU_CENTRAL_2 = "eu-central-2"
    EU_WEST_1 = "eu-west-1"
    EU_WEST_2 = "eu-west-2"
    EU_WEST_3 = "eu-west-3"
    EU_SOUTH_1 = "eu-south-1"
    EU_SOUTH_2 = "eu-south-2"
    EU_NORTH_1 = "eu-north-1"
    IL_CENTRAL_1 = "il-central-1"
    ME_SOUTH_1 = "me-south-1"
    ME_CENTRAL_1 = "me-central-1"
    SA_EAST_1 = "sa-east-1"


IAMRoleRequiredPermissions = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SyncEC2Permissions",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeInstances",
                "ec2:DescribeVolumes",
                "ec2:DescribeAvailabilityZones",
            ],
            "Resource": "*",
        }
    ],
}

IAMRoleTrustPolicy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::533267411813:role/sync-computing-collector"},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"sts:ExternalId": "PLACEHOLDER_EXTERNAL_ID"}},
        }
    ],
}


class AwsHostedIAMInstructions(BaseModel):
    step_1_prompt: str = "Step 1: Copy the JSON and paste in AWS IAM Permissions page:"
    step_1_value: str = json.dumps(IAMRoleRequiredPermissions)
    step_2_prompt: str = (
        "Step 2: Copy the JSON and paste in AWS IAM Trust relationships page with External ID:"
    )
    external_id: str

    @property
    def step_2_value(self) -> str:
        policy = copy.deepcopy(IAMRoleTrustPolicy)
        policy["Statement"][0]["Condition"]["StringEquals"]["sts:ExternalId"] = self.external_id
        return json.dumps(policy)


class ComputeProviderHostedValues(BaseModel):
    aws_iam_role_arn: Optional[str]
    azure_subscription_id: Optional[str]
    azure_tenant_id: Optional[str]
    azure_client_id: Optional[str]
    azure_client_secret: Optional[str]


class CreateWorkspaceConfig(BaseModel):
    workspace_id: str = Field(..., description="Unique identifier for the workspace")
    databricks_host: str = Field(..., description="Databricks service host URL")
    databricks_token: str = Field(..., description="Authentication token for Databricks service")
    sync_api_key_id: str = Field(..., description="API Key ID for synchronization service")
    sync_api_key_secret: str = Field(..., description="API Key secret for synchronization service")
    instance_profile_arn: Optional[str] = Field(None, description="AWS instance profile ARN")
    webhook_id: Optional[str] = Field(None, description="Webhook ID for notifications")
    databricks_plan_type: DatabricksPlanType = Field(
        DatabricksPlanType.STANDARD, description="Plan type for Databricks deployment"
    )
    aws_region: Optional[str] = Field(None, description="AWS region if applicable")
    cluster_policy_id: Optional[str] = Field(None, description="Cluster policy ID for Databricks")
    collection_type: WorkspaceCollectionTypeEnum = Field(
        ..., description="Type of hosting for the workspace"
    )
    monitoring_type: WorkspaceMonitoringTypeEnum = Field(
        ..., description="Type of monitoring for the workspace"
    )
    compute_provider: ComputeProvider = Field(
        ..., description="Cloud provider for compute resources"
    )
    external_id: Optional[str] = Field(None, description="External ID for AWS configurations")
    aws_iam_role_arn: Optional[str] = Field(None, description="AWS IAM role ARN if needed")
    azure_tenant_id: Optional[str] = Field(None, description="Azure tenant ID if using Azure")
    azure_client_id: Optional[str] = Field(None, description="Azure client ID if using Azure")
    azure_client_secret: Optional[str] = Field(
        None, description="Azure client secret if using Azure"
    )
    azure_subscription_id: Optional[str] = Field(
        None, description="Azure subscription ID if using Azure"
    )

    @validator("aws_region")
    def check_aws_region(cls, region, values):
        if values.get("compute_provider") == ComputeProvider.AWS and not region:
            raise ValueError("AWS Region is required for AWS compute provider")
        return region

    @validator("instance_profile_arn")
    def ensure_instance_profile_arn(cls, arn, values):
        if (
            values.get("collection_type") == WorkspaceCollectionTypeEnum.REMOTE
            and values.get("compute_provider") == ComputeProvider.AWS
            and not arn
        ):
            raise ValueError(
                "AWS Instance Profile ARN is required for REMOTE hosted AWS environments"
            )
        return arn

    @validator("aws_iam_role_arn", pre=False)
    def check_aws_iam_role_arn(cls, aws_iam_role_arn, values):
        compute_provider = values.get("compute_provider")
        if values.get("collection_type") == WorkspaceCollectionTypeEnum.HOSTED:
            if compute_provider == ComputeProvider.AWS and not aws_iam_role_arn:
                raise ValueError("AWS IAM Role ARN is required for AWS compute provider")
        return aws_iam_role_arn

    @validator("compute_provider", pre=False)
    def check_azure_hosted_fields(cls, compute_provider, values):
        if (
            values.get("compute_provider") == ComputeProvider.AZURE
            and values.get("collection_type") == WorkspaceCollectionTypeEnum.HOSTED
        ):
            required_fields = [
                "azure_tenant_id",
                "azure_client_id",
                "azure_client_secret",
                "azure_subscription_id",
            ]
            missing_fields = [field for field in required_fields if not values.get(field)]
            if missing_fields:
                raise ValueError(
                    f"Missing required fields for Azure compute provider: "
                    f"{', '.join(missing_fields)}"
                )
        return compute_provider


class UpdateWorkspaceConfig(BaseModel):
    workspace_id: str
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    sync_api_key_id: Optional[str] = None
    sync_api_key_secret: Optional[str] = None
    instance_profile_arn: Optional[str] = None
    databricks_plan_type: Optional[str] = None
    webhook_id: Optional[str] = None
    aws_region: Optional[str] = None
    cluster_policy_id: Optional[str] = None
    aws_iam_role_arn: Optional[str] = None
    azure_subscription_id: Optional[str] = None
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    collection_type: Optional[str] = None
    monitoring_type: Optional[str] = None
