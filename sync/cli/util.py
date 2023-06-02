from typing import Union
from uuid import UUID

from sync.api.projects import get_project_by_app_id


def validate_project(ctx, param, value: Union[str, None]) -> dict:
    if not value:
        return {"id": None}

    try:
        UUID(value)
        return {"id": value}
    except ValueError:
        project_response = get_project_by_app_id(value)
        error = project_response.error
        if error:
            ctx.fail(str(error))

        return project_response.result
