Projects
========

Projects are Sync-enabled Apache Spark applications. Once a project is created around an
application the performance and cost of that application can be continuously tuned to strike the right balance.

On-boarding
-----------

To on-board a Spark application it must be run with a configuration that includes Sync metadata and an event log destination.
2 functions are provided for this purpose:

.. autofunction:: sync.project.init_emr
  :noindex:

.. autofunction:: sync.project.init_databricks
  :noindex:

On successful completion the event log is at the S3 location for the first run based on the provided URL
alongside the configuration applied to either the EMR or Databricks cluster API. :py:func:`~sync.project.record_run`
should then be called to save the configuration required for a prediction. For convenience, the methods :py:func:`~sync.project.init_emr_and_wait` and :py:func:`~sync.project.init_databricks_and_wait`
provide a complete on-boarding transaction that returns after calling :py:func:`~sync.project.record_run` on completion of the run.

The above is just 1 avenue for on-boarding a Spark application to a Sync project. Other ways may be easier or otherwise better suited to a customer.
The only things that all onboarding solutions must have in common are,

1. A Sync project is created for the Apache Spark application
2. The project S3 location in the customer's account contains valid data from 1 run:

   1. Event log
   2. Applied cluster configuration
   3. Post cluster data (i.e. prediction configuration)

Continuous Tuning
-----------------

Iterative Optimization
~~~~~~~~~~~~~~~~~~~~~~

Sync projects track the state of an Apache Spark application as predictions are applied to further optimize it.
The progress of a project is cyclical: with a configuration and log from the previous run, a prediction is generated
and applied yielding a new log & configuration.

.. image:: /_static/orchestration.png

Each run has its own location in S3 under the project location for event logs and configuration.
It is keyed by timestamp to make browsing in the console easier, and run ID to guarantee uniqueness:

.. code-block:: text

  s3://{project bucket}/{project prefix}/{project ID}/{timestamp}/{run ID}

Example:

.. code-block:: text

  s3://megacorp-jobs/sync-projects/54129c79-ee4a-47cf-8bf3-3e2326443fbc/2022-11-15T13:51:29Z/01953ba2-ee4a-47cf-8bf3-80sbj2lapcn8


Iterative Tracking and Notification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rather than applying a prediction every time customers may be more likely to prefer a more discretionary approach.
In this approach cluster configurations are applied by orchestration with Sync tracking tags and event log configuration.
After each iteration event logs & cluster data are sent to Sync for optimizing predictions.
If a prediction is compelling it is applied such that the orchestration will use it in subsequent iterations.

This flow goes like so,

Setup:

1. customer creates project with application name/ID, s3 project location, and optionally a prediction preference

Orchestration:

1. before an app is run the orchestrator updates the cluster configuration with the following
   either manually, or by calling :py:func:`~sync.project.prepare_job_flow`

   1. event log location (format: {project S3 URL}/{project ID}/{timestamp}/{run ID})
   2. Sync tags: `sync:tenant-id`, `sync:project-id`, `sync:run-id`

2. applies the updated job flow
3. records the run when the cluster completes