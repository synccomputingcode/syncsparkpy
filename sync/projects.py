from sync.api.projects import get_submissions


class SubmissionRetrievalError(Exception):
    """Custom exception for errors retrieving submissions."""


class NoSubmissionsFoundError(SubmissionRetrievalError):
    """No submissions were found."""


class NoSuccessfulSubmissionsFoundError(SubmissionRetrievalError):
    """No successful submissions were found."""


def get_latest_submission_config(project_id: str, success_only: bool = False) -> dict:
    """
    Get the latest submission configuration for a project.

    :param project_id: Sync project ID
    :type project_id: str
    :param success_only: Only show the most recent successful submission, if omitted shows the most
        recent submission regardless of state
    :type success_only: bool, optional
    """

    try:
        submissions = get_submissions(project_id).result
    except Exception as e:
        raise SubmissionRetrievalError(
            f"Failed to retrieve submissions for project '{project_id}'. {e}"
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
