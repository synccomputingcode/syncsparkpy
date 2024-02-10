Getting Started
===============

Requirements
------------

1. Currently only linux-based systems are supported.
2. Python 3.7.x and above

Installation
------------

Install this library and CLI the same way you'd install a Python package. With a compatible version of Python (preferably in a virtual environment),

.. code-block:: shell

  pip install https://github.com/synccomputingcode/syncsparkpy/archive/latest.tar.gz

Configuration
-------------

This library requires access to the public Sync and AWS APIs (for Apache Spark event logs stored in S3). In addition, some Databricks features rely on the public Databricks API.
The CLI provided with this package makes it easy to configure access to the Sync API. In the same environment in which the SDK is installed run,

.. code-block:: shell

  sync-cli configure

You'll be prompted for an API key which can be had from the account page of the Sync web app.
This creates the Sync directory if it doesn't already exist and stores the API key in it at `~/.sync/credentials`.
You'll also be prompted for default values for and S3 location under which to store project data.

These are optional to help with setting up multiple Sync projects. However, in the context of an existing project they are superseded by the corresponding properties of that project.
They are stored in `~/.sync/config`.

AWS access is best configured with the AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-config
