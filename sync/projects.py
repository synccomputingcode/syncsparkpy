from uuid import UUID

from sync.api.projects import get_project_by_app_id, get_submissions


class SubmissionRetrievalError(Exception):
    """Custom exception for errors retrieving submissions."""


class NoSubmissionsFoundError(SubmissionRetrievalError):
    """No submissions were found."""


class NoSuccessfulSubmissionsFoundError(SubmissionRetrievalError):
    """No successful submissions were found."""


class InvalidUUIDError(ValueError):
    """Raised when the provided UUID is invalid."""


class ProjectResolutionError(Exception):
    """Raised when there is an error resolving the project ID."""


def validate_uuid(value: str) -> str:
    try:
        UUID(value)
        return value
    except ValueError:
        raise InvalidUUIDError(f"Invalid UUID: {value}")


def resolve_project_id(value: str) -> str:
    try:
        return validate_uuid(value)
    except InvalidUUIDError:
        try:
            project_response = get_project_by_app_id(value)
            if project_response.error:
                raise ProjectResolutionError(
                    f"Error resolving project ID: {project_response.error}"
                )
            return project_response.result.get("id")
        except Exception as e:
            raise ProjectResolutionError(
                f"An error occurred while resolving the project ID: {str(e)}"
            )


def get_latest_submission_config(app_id: str, success_only: bool = False) -> dict:
    """
    Get the latest submission configuration for a project.

    :param app_id: Sync project ID or application name
    :type app_id: str
    :param success_only: Only show the most recent successful submission, if omitted shows the most
        recent submission regardless of state
    :type success_only: bool, optional
    """

    try:
        project_id = resolve_project_id(app_id)
        submissions = get_submissions(project_id).result
    except Exception as e:
        raise SubmissionRetrievalError(
            f"Failed to retrieve submissions for project '{app_id}'. {e}"
        ) from e

    if not submissions:
        raise NoSubmissionsFoundError(f"No submissions found for project '{project_id}'.")

    if success_only:
        latest_successful_submission = next(
            (submission for submission in submissions if submission["state"] == "SUCCESS"), None
        )

        if not latest_successful_submission:
            raise NoSuccessfulSubmissionsFoundError(
                f"No successful submissions found for project '{project_id}'."
            )

        submission_to_show = latest_successful_submission
    else:
        submission_to_show = submissions[0]
    config = submission_to_show.get("configuration", {})
    return config
