from typing import List

from sync.clients.sync import get_default_client
from sync.models import CreateWorkspaceConfig, Response, UpdateWorkspaceConfig

NULL = "null"


def create_workspace_config(config: CreateWorkspaceConfig) -> Response[dict]:
    return Response(
        **get_default_client().create_workspace_config(
            config.workspace_id,
            databricks_host=config.databricks_host,
            databricks_token=config.databricks_token,
            sync_api_key_id=config.sync_api_key_id,
            sync_api_key_secret=config.sync_api_key_secret,
            webhook_id=config.webhook_id,
            plan_type=config.databricks_plan_type,
            cluster_policy_id=config.cluster_policy_id,
            collection_type=config.collection_type,
            monitoring_type=config.monitoring_type,
            compute_provider=config.compute_provider,
            aws_region=config.aws_region,
            aws_iam_role_arn=config.aws_iam_role_arn,
            instance_profile_arn=config.instance_profile_arn,
            external_id=config.external_id,
            azure_tenant_id=config.azure_tenant_id,
            azure_client_id=config.azure_client_id,
            azure_client_secret=config.azure_client_secret,
            azure_subscription_id=config.azure_subscription_id,
        )
    )


def get_workspace_config(workspace_id: str) -> Response[dict]:
    return Response(**get_default_client().get_workspace_config(workspace_id))


def get_workspace_configs() -> Response[List[dict]]:
    return Response(**get_default_client().get_workspace_configs())


def update_workspace_config(config: UpdateWorkspaceConfig) -> Response[dict]:
    params = {
        key: value if value != NULL else None
        for key, value in {
            "databricks_host": config.databricks_host,
            "databricks_token": config.databricks_token,
            "sync_api_key_id": config.sync_api_key_id,
            "sync_api_key_secret": config.sync_api_key_secret,
            "instance_profile_arn": config.instance_profile_arn,
            "webhook_id": config.webhook_id,
            "plan_type": config.databricks_plan_type,
            "aws_region": config.aws_region,
            "cluster_policy_id": config.cluster_policy_id,
            "aws_iam_role_arn": config.aws_iam_role_arn,
            "azure_tenant_id": config.azure_tenant_id,
            "azure_client_id": config.azure_client_id,
            "azure_client_secret": config.azure_client_secret,
            "azure_subscription_id": config.azure_subscription_id,
            "collection_type": config.collection_type,
            "monitoring_type": config.monitoring_type,
        }.items()
        if value is not None
    }

    return Response(**get_default_client().update_workspace_config(config.workspace_id, **params))


def delete_workspace_config(workspace_id: str) -> Response[str]:
    return Response(**get_default_client().delete_workspace_config(workspace_id))


def reset_webhook_creds(workspace_id: str) -> Response[dict]:
    return Response(**get_default_client().reset_webhook_creds(workspace_id))


def apply_workspace_config(workspace_id: str) -> Response[str]:
    return Response(**get_default_client().apply_workspace_config(workspace_id))


def onboard_job(workspace_id: str, job_id: str, project_id: str) -> Response[str]:
    return Response(**get_default_client().onboard_workflow(workspace_id, job_id, project_id))
