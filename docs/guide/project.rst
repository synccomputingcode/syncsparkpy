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

Iterative Optimization
----------------------

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
