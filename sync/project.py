"""Project functions
"""

from .models import Preference, Response


def run_once(project_id: str, preference: Preference = Preference.BALANCED) -> Response[str]:
    """Gets a prediction based off the log and config files of the latest successful run and executes it.

    A run is considered successful if it has a valid event log in the configured location for the project,
    and a config file is either present or can be generated.

    An error is returned if valid resources required for a prediction
    cannot be found or if a prediction cannot otherwise be generated.

    Args:
        project_id (str): project ID
        preference (Preference, optional): preferred prediction. Defaults to Preference.BALANCED.

    Returns:
        Response[str]: job flow ID
    """
    ...


def record_run(cluster_id: str) -> Response[None]:
    """Called on cluster completion to collect resources required for a prediction.

    This function updates the project with the status of the run, and if the cluster completed successfully configuration
    required for a new prediction will be collected add persisted alongside the event log. An error is returned if either
    step is unsuccessful.

    Args:
        cluster_id (str): EMR cluster ID

    Returns:
        Response[None]: no succeessful response
    """
    ...
