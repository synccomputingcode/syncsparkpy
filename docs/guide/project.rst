Projects
========

Projects are Sync-enabled Apache Spark applications. Once a project is created around an
application the performance and cost of that application can be continuously tracked to provide
analysis and recommendations.

On-boarding
-----------

There are varying degrees to which an Apache Spark application can be on-boarded. First however, a Sync project must be created:

.. autofunction:: sync.api.projects.create_project
  :noindex:

For a more robust experience add an S3 location under which to store event logs and application configurations. If the application has an event log configuration based on
that location only a project reference is needed to track it in a Sync project. This library function provides a full EMR configuration for the project:

.. autofunction:: sync.awsemr.get_project_job_flow
  :noindex:

At any point after at least 1 run of the project-configured application the latest prediction can be generated with :py:func:`~sync.awsemr.create_project_prediction`.

To get the most out of your project each application run should be recorded. This way Sync can provide the best recommendations. The library function to call is,

.. autofunction:: sync.awsemr.record_run
  :noindex:

Continuous Tuning
-----------------

Iterative Optimization
~~~~~~~~~~~~~~~~~~~~~~

Sync projects track the status of an Apache Spark application as predictions are applied to further optimize it.
The progress of a project is cyclical: with a configuration and log from the previous run, a prediction is generated
and applied yielding a new log & configuration.

.. image:: /_static/orchestration.png

Each run has its own location in S3 under the project location for event logs and configuration.
It is keyed by timestamp to make browsing in the AWS console easier, and run ID to guarantee uniqueness:

.. code-block:: text

  s3://{project bucket}/{project prefix}/{project ID}/{timestamp}/{run ID}

Example:

.. code-block:: text

  s3://megacorp-jobs/sync-projects/54129c79-ee4a-47cf-8bf3-3e2326443fbc/2022-11-15T13:51:29Z/01953ba2-ee4a-47cf-8bf3-80sbj2lapcn8


Iterative Tracking and Notification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rather than applying a prediction every time, a discretionary approach may be preferable. Project
configuration should still be applied to the cluster so that orchestration can create a prediction
after each run. If a prediction is compelling it is applied to the configuration of subsequent runs.

The flow goes like so,

Setup:

1. Create a project with application name/ID, S3 project location and optionally, a prediction preference

Orchestration:

1. Before an app is run the orchestrator updates the cluster configuration with the following
   either manually, or by calling :py:func:`~sync.awsemr.get_project_job_flow`

   1. Event log location:

    ``{project S3 URL}/{project ID}/{timestamp}/{run ID}``

   2. Sync tags: `sync:tenant-id`, `sync:project-id`, `sync:run-id`

2. Applies the updated job flow
3. Records the run when the cluster completes
