from sync.clients.sync import get_default_client
from sync.models import Response

NULL = "null"


def create_workspace_config(
    workspace_id: str,
    databricks_host: str,
    databricks_token: str,
    sync_api_key_id: str,
    sync_api_key_secret: str,
    instance_profile_arn: str = None,
    databricks_plan_type: str = None,
    webhook_id: str = None,
):
    return Response(
        **get_default_client().create_workspace_config(
            workspace_id,
            databricks_host=databricks_host,
            databricks_token=databricks_token,
            sync_api_key_id=sync_api_key_id,
            sync_api_key_secret=sync_api_key_secret,
            instance_profile_arn=instance_profile_arn,
            webhook_id=webhook_id,
            plan_type=databricks_plan_type,
        )
    )


def get_workspace_config(workspace_id: str):
    return Response(**get_default_client().get_workspace_config(workspace_id))


def get_workspace_configs():
    return Response(**get_default_client().get_workspace_configs())


def update_workspace_config(
    workspace_id: str,
    databricks_host: str = None,
    databricks_token: str = None,
    sync_api_key_id: str = None,
    sync_api_key_secret: str = None,
    instance_profile_arn: str = None,
    databricks_plan_type: str = None,
    webhook_id: str = None,
):
    params = {
        key: value if value != NULL else None
        for key, value in {
            "databricks_host": databricks_host,
            "databricks_token": databricks_token,
            "sync_api_key_id": sync_api_key_id,
            "sync_api_key_secret": sync_api_key_secret,
            "instance_profile_arn": instance_profile_arn,
            "webhook_id": webhook_id,
            "plan_type": databricks_plan_type,
        }.items()
        if value is not None
    }

    return Response(**get_default_client().update_workspace_config(workspace_id, **params))


def delete_workspace_config(workspace_id: str):
    return Response(**get_default_client().delete_workspace_config(workspace_id))
