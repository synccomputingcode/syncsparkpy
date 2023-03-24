# Sync Library
The Sync Library provides drop-in functions facilitating integration between Sync's API and existing Apache Spark orchestrations. Integration enables tracking of high-level job metrics to provide optimized configurations to help meet SLAs while keeping costs down.

*Note*: This library is under active development and may contain features not yet fully supported.

* [Goals](#goals)
  * [Out of Scope](#out-of-scope)
* [Contribution Guidelines](#contribution-guidelines)
  * [Documentation](#documentation)
* [CLI](#cli)
* [Developer Interface](#developer-interface)
* [Configuration](#configuration)
* [Future Considerations](#future-considerations)

## Goals
This Library enables recording EMR and Databricks job results to track high-level metrics and offer run-time and cost optimizing configuration updates.

Specifically it supports,
1. Starting an EMR job from a [RunJobFlow](https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html) spec or a Databricks job from a [Clusters 2.0 Create](https://docs.databricks.com/dev-tools/api/latest/clusters.html#create) spec with the configuration to make the resources necessary for a prediction available, e.g. event log.
2. Tracking run data to provide analysis and predictions.
3. Starting an EMR or Databricks job with configuration from a Sync prediction.

### Out of Scope
Orchestration that supports either,
1. Running more than 1 job at a time
2. Continuous or scheduled iterations of a project

Though this repo is not meant to provide full-blown continuous tuning solutions, functions provided by this library are intended to be incorporated in such orchestration tools. For convenience however, functions that return only after a cluster completes to record the result are included.

## Contribution Guidelines
Only add what provides clear benefit - no speculative development. Contributions should be organized in well-defined orthogonal functions and classes - "building blocks" - with documentation and tests. Public functions are subject to the constraints of [semantic versioning](https://semver.org). And be nice!

### Documentation
Documentation for consumers of the library is built using [Sphinx](https://www.sphinx-doc.org/en/master/). Pages are defined in [reStructuredText](https://docutils.sourceforge.io/rst.html) which may pull in reStructuredText from docstrings in the code. The VS Code extension, autoDocstring, provides a convenient way to initialize function docstrings. For compatibility, set the docstring format in the extension's settings to "sphinx". Information is meaningless without context so sprinkle documentation liberally with refs: [Cross-referencing with Sphinx](https://docs.readthedocs.io/en/stable/guides/cross-referencing-with-sphinx.html), [Cross-referencing Python objects](https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#cross-referencing-python-objects).

When code is merged to `main` a Github action automatically deploys the documentation to Github Pages. Click "View deployment" in the "github-pages" environment of this repo to navigate to the latest documentation.

## CLI
The CLI is provided mainly for demonstration of what's possible when you integrate with Sync's API using this library. Explore available commands for EMR clusters with `sync-cli aws-emr --help`. You can also use it to interact directly with API resources: `sync-cli predictions --help`.

## Developer Interface
The developer interface consists of the public attributes of the `sync.api` package, and the `sync.awsemr` and `sync.awsdatabricks` modules. With each change the impact to the version of the next release must be considered in regard to semantic versioning [semantic versioning](https://semver.org). The developer interface is built using clients including those in `sync.clients`. Clients in that package provide a raw interface to their corresponding services and are intended to support the developer interface only.

This library is organized by functional domain as hinted by the names of the modules under the `sync` package. Utilities for interacting with EMR and Databricks are in `sync.awsemr` and `sync.awsdatabricks`, respectively. These modules will provide functionality for starting jobs and consolidating information required for Sync predictions. When starting jobs tags are applied and the event log location specified.

Successful responses and errors from the developer interface will be returned in an instance of the generic [Response](sync/models.py). Use of this model means that exceptions raised must be handled by this library to provide helpful information in the error response.
## Configuration
Configuration at the installation site is required before the library can be used. See the user guide for details.

## Future Considerations
A Spark application may be refactored to the point of invalidating the basis of a prediction. This could lead to a prediction being wildly off or altogether broken in that its configuration cannot yield a successful run. It may therefore be worthwhile to evaluate the state of the application code a key points. Concretely, a hash of the code could be persisted to facilitate tracking changes and warning users when a prediction may no longer be reliably applied.
