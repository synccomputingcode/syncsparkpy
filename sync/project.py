"""Project functions
"""

from .client import get_default_client
from .models import Error, Preference, Project, Response


def start_next_with_prediction(
    app_id: str, prediction_preference: Preference = None
) -> Response[str]:
    """Gets a prediction based off the log and configuration of the last successful run and kicks off a new run with the preferred configuration

    A UUID is generated to identify the run. It is added to the configuration from the prediction as a tag with the key, "sync:run-id",
    and in the "spark.eventLog.dir" property of the "spark-defaults" configuration. The event log URL contains a timestamp too for convenience:

      s3://{project bucket}/{project prefix}/{project ID}/{timestamp}/{run ID}

    Before the configuration is applied it is saved to the above S3 location for the run where the event
    log and post configuration will be persisted on successful completion of the cluster. This will create the "directory"
    that Spark requires to save the event log.

    The ID of the newly created cluster is returned if a prediction was successfully obtained and applied.

    If the Sync API cannot provide a prediction (e.g. a required input is missing),
    or - for whatever reason - a cluster cannot be created an explanitory error message is returned in the `error` attribute of the response.

    :param app_id: app ID
    :type app_id: str
    :param prediction_preference: preferred prediction, defaults to project preference
    :type prediction_preference: Preference, optional
    :return: cluster ID
    :rtype: Response[str]
    """


def start_next_with_conf(app_id: str, conf: str) -> Response[str]:
    """Run the next iteration of a project with the supplied configuration

    This is provided as a means to override the prediction that'd otherwise be used.
    It may be useful if a cluster configuration that is substantially different from the last is needed.

    A UUID is generated to identify the run. It is added to the configuration as a tag with the key, "sync:run-id",
    and in the "spark.eventLog.dir" property of the "spark-defaults" configuration.
    The event log URL contains a timestamp too for convenience:

      s3://{project bucket}/{project prefix}/{project ID}/{timestamp}/{run ID}

    Other tracking tags are applied if not present:

    1. sync:project-id
    2. sync:tenant-id

    The configuration is then saved to the above S3 location for the run where the event
    log and post configuration will be persisted on successful completion of the cluster. This will create the "directory"
    that Spark requires to save the event log.

    The ID of the newly created cluster is returned if the provided configuration was successfully applied,
    otherwise the response will contain and explanitory error message.

    :param app_id: app ID
    :type app_id: str
    :param conf: cluster configuration
    :type conf: str
    :return: cluster ID
    :rtype: Response[str]
    """


def run_once_with_prediction(
    app_id: str, prediction_preference: Preference = None
) -> Response[str]:
    """Runs a complete continuous tuning iteration

    This calls :py:func:`~sync.project.start_next_with_prediction` then waits until the cluster completes before calling :py:func:`~sync.project.record_run` returning the cluster ID.

    :param app_id: app ID
    :type app_id: str
    :param prediction_preference: preferred prediction, defaults to None
    :type prediction_preference: Preference, optional
    :return: cluster ID
    :rtype: Response[str]
    """


def run_once_with_conf(app_id: str, conf: str) -> Response[str]:
    """Run the next iteration with the supplied configuration, waits until cluster finishes to record the result

    Once the cluster completes the run is finalized with a call to :py:func:`~sync.project.record_run`

    :param app_id: app ID
    :type app_id: str
    :param conf: cluster configuration
    :type conf: str
    :return: cluster ID
    :rtype: Response[str]
    """


def init_emr(
    job_flow: dict,
    s3_project_url: str,
    app_id: str = None,
    prediction_preference: Preference = Preference.BALANCED,
) -> Response[str]:
    """Creates a Sync project and runs the job flow to initialize continuous tuning

    The Sync project is configured with the S3 location where event logs and configurations will go, and the default prediction preference.
    A run ID is generated and added to the S3 location for the first run which is defined as,

      {``s3_project_url``}/{project ID}/{timestamp}/{run ID}

    The job flow is updated with the S3 location as the value of the "spark.eventLog.dir" property of the "spark-defaults" configuration,
    and tracking attributes as tags:

    1. sync:run-id
    2. sync:project-id
    3. sync:tenant-id

    The job flow is then saved to the S3 location for the run before it is executed.

    :param job_flow: job flow
    :type job_flow: dict
    :param s3_project_url: S3 URL under which successive event logs & configurations will be stored
    :type s3_project_url: str
    :param prediction_preference: default prediction preference for project, defaults to Preference.BALANCED
    :type prediction_preference: Preference, optional
    :return: cluster ID
    :rtype: Response[str]
    """


def init_databricks(
    cluster_spec: dict, s3_project_url: str, prediction_preference: Preference = Preference.BALANCED
) -> Response[str]:
    """Creates a Sync project and starts a cluster to initialize continuous tuning

    The Sync project is configured with the S3 location where event logs and configurations will go, and the default prediction preference.
    A run ID is generated and added to the S3 location for the first run which is defined as,

      {``s3_project_url``}/{project ID}/{timestamp}/{run ID}

    The cluster configuration is updated with the S3 location as the value of the "spark.eventLog.dir" property of the "spark-defaults" configuration,
    and tracking attributes as tags:

    1. sync:run-id
    2. sync:project-id
    3. sync:tenant-id

    The cluster configuration is then saved to the S3 location for the run before it is executed.

    :param cluster_spec: Databricks cluster
    :type cluster_spec: dict
    :param s3_project_url: S3 URL under which to store successive event logs & configurations
    :type s3_project_url: str
    :param prediction_preference: default prediction preference for project, defaults to Preference.BALANCED
    :type prediction_preference: Preference, optional
    :return: cluster ID
    :rtype: Response[str]
    """


def init_databricks_and_wait(
    cluster_spec: dict, s3_project_url: str, prediction_preference: Preference = Preference.BALANCED
) -> Response[None]:
    """Creates a Sync project and records the first run

    This calls :py:func:`~sync.project.init_databricks` then waits until the cluster completes before calling :py:func:`~sync.project.record_run` and returning its result.

    :param cluster_spec: Databricks cluster definition
    :type cluster_spec: dict
    :param s3_project_url: S3 URL under which to store successive event logs & configurations
    :type s3_project_url: str
    :param prediction_preference: default prediction preference for project, defaults to Preference.BALANCED
    :type prediction_preference: Preference, optional
    :return: project ID
    :rtype: Response[str]
    """


def init_emr_and_wait(
    job_flow: dict, s3_project_url: str, prediction_preference: Preference = Preference.BALANCED
) -> Response[None]:
    """Creates a Sync project and records the first run

    This calls :py:func:`~sync.project.init_emr` then waits until the cluster completes before calling :py:func:`~sync.project.record_run` and returning its result.

    :param job_flow: job flow
    :type job_flow: dict
    :param s3_project_url: S3 URL under which to store successive event logs & configurations
    :type s3_project_url: str
    :param prediction_preference: default prediction preference for project, defaults to Preference.BALANCED
    :type prediction_preference: Preference, optional
    :return: project ID
    :rtype: Response[str]
    """


def prepare_job_flow(job_flow: dict, app_id: str) -> Response[dict]:
    """Prepares job flow for next run

    The project for the provided ``app_id`` is used to update the job flow with tracking information.
    The job flow is configued with,

    1. event log location
    2. tags:

       1. sync.project-id
       2. sync.run-id
       3. sync.tenant-id

    An error is returned if the project cannot be found

    :param job_flow: job flow
    :type job_flow: dict
    :param app_id: app ID
    :type app_id: str
    :return: updated job flow
    :rtype: Response[dict]
    """


def record_run(cluster_id: str) -> Response[str]:
    """Called on cluster completion to collect resources required for a prediction.

    This function updates the project with the status of the run, and if the cluster completed successfully configuration
    required for a new prediction will be collected add persisted alongside the event log. An error is returned if either
    step is unsuccessful.

    :param cluster_id: EMR cluster ID
    :type cluster_id: str
    :return: project ID
    :rtype: Response[str]
    """


def get_prediction(app_id: str, preference: Preference = None) -> Response[dict]:
    """Get a prediction based off the latest event log and configuration

    :param app_id: app ID
    :type app_id: str
    :param preference: preferred prediction, defaults to project preference
    :type preference: Preference, optional
    :return: prediction
    :rtype: Response[dict]
    """


def create_project(
    app_name: str,
    description: str = None,
    s3_url: str = None,
    prediction_preference: Preference = Preference.BALANCED,
) -> Response[Project]:
    """Creates a Sync project for tracking and optimizing Apache Spark applications

    :param app_name: Apache Spark application name
    :type app_name: str
    :param s3_url: S3 URL under which successive event logs & configurations will be stored
    :type s3_url: str
    :param app_id: an external identifier of the application, identifies nothing in Sync services, defaults to ID of new project
    :type app_id: str
    :param prediction_preference: default prediction preference for project, defaults to Preference.BALANCED
    :type prediction_preference: Preference
    :return: project
    :rtype: Response[Project]
    """
    return Response(
        **get_default_client().create_project(
            {
                "app_id": app_name,
                "description": description,
                "s3_url": s3_url,
                "prediction_preference": prediction_preference,
            }
        )
    )


def get_project(project_id: str) -> Response[Project]:
    """Retrieves a project

    :param project_id: porject ID
    :type project_id: str
    :return: project or error if none exists for the ID
    :rtype: Response[Project]
    """
    return Response(**get_default_client().get_project(project_id))


def update_project(
    project_id: str,
    description: str = None,
    s3_url: str = None,
    prediction_preference: Preference = None,
) -> Response[Project]:
    """Updates a projects mutable properties

    :param project_id: project ID
    :type project_id: str
    :param description: description, defaults to None
    :type description: str, optional
    :param s3_url: location of project event logs and configurations, defaults to None
    :type s3_url: str, optional
    :param prediction_preference: default preference for predictions, defaults to None
    :type prediction_preference: Preference, optional
    :return: updated project
    :rtype: Response[Project]
    """
    return Response(
        **get_default_client().update_project(
            project_id,
            {
                "description": description,
                "s3_url": s3_url,
                "prediction_preference": prediction_preference,
            },
        )
    )


def get_project_by_app_id(
    app_id: str,
) -> Response[Project]:
    """Retrieves a project by app ID

    :param app_id: app ID
    :type app_id: str
    :return: project or error if none exists for the app ID
    :rtype: Response[Project]
    """
    response = get_default_client().get_projects({"app_id": app_id})
    if result := response.get("result"):
        return Response(result=result[0])

    return Response(error=Error(code="Not found", message=f"Project not found for '{app_id}'"))


def get_projects() -> Response[list[Project]]:
    """Returns all projects authorized by the API key

    :return: projects
    :rtype: Response[list[Project]]
    """
    return Response(**get_default_client().get_projects())


def delete_project(project_id: str) -> Response[str]:
    """Deletes a project

    :param project_id: project ID
    :type project_id: str
    :return: project or error if none exists for the ID
    :rtype: Response[Project]
    """
    return Response(**get_default_client().delete_project(project_id))
