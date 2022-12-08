Getting Started
===============

Requirements
------------

Currently, because of how the home directory under which required properties are stored is located only linux-based systems are supported.
This may change in future releases.

Installation
------------

Install this SDK the same way you'd install a Python package. With a compatible version of Python (preferably in a virtual environment),

.. code-block:: shell

  pip3 install https://github.com/synccomputingcode/sync_sdk/archive/main.tar.gz

Configuration
-------------

This SDK requires access to the Sync API and, depending on the types of Apache Spark applications you're concerned with, AWS and Databricks APIs.
The CLI provided with this SDK makes it easy to configure access to the Sync API. In the same environment in which the SDK is installed run,

.. code-block:: shell

  sync-cli configure

You'll be prompted for an API key which can be had from the account page of the Sync web app.
This creates the Sync directory if it doesn't already exist and stores the API key in it at `~/.sync/credentials`.
You'll also be prompted for default values for,
1. S3 location under which to store project data
2. Your preferred prediction
These are optional and may make setting up projects via the CLI easier. However, in the context of an existing project they are superseded by the project's required properties.
They are stored in `~/.sync/config`.

AWS access is best configured with the CLI: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-config