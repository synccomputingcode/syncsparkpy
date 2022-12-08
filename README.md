# Sync SDK
The Sync SDK is a library for building integrations with Sync Computing's API.

* [Goals](#goals)
  * [Out of Scope](#out-of-scope)
* [Contribution Guidelines](#contribution-guidelines)
  * [Documentation](#documentation)
* [CLI](#cli)
* [Developer Interface](#developer-interface)
* [Configuration](#configuration)
* [For Future Consideration](#for-future-consideration)

## Goals
This SDK supports executing EMR and Databricks jobs, generating predictions based on those jobs and applying those predictions.

Specifically,
1. Start an EMR job from a [RunJobFlow](https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html) spec or a Databricks job from a [Clusters 2.0 Create](https://docs.databricks.com/dev-tools/api/latest/clusters.html#create) spec with the configuration to make the resources necessary for a prediction available, e.g. event log.
2. Start an EMR or Databricks job from a Sync prediction.
3. Create a prediction.

### Out of Scope
Orchestration that supports either,
1. Running more than 1 job at a time
2. Continuous or scheduled iterations of a project

Though this repo is not meant to provide full-blown continuous tuning solutions, functions provided by this SDK are intended to be incorporated in such orchestration tools. For convenience however, functions that return only after a cluster completes to record the result are included.

## Contribution Guidelines
Only add what we know we'll need - no speculative development. Contributions should be organized in well-defined orthogonal functions and classes - "building blocks" - with documentation and tests. Public functions are subject to the constraints of [semantic versioning](https://semver.org).

### Documentation
Documentation for consumers of the SDK is build using [Sphinx](https://www.sphinx-doc.org/en/master/). Pages are defined in [reStructuredText](https://docutils.sourceforge.io/rst.html) which may pull in reStructuredText from docstrings in the code. The VS Code extension, autoDocstring, provides a convenient way to initialize function docstrings. For compatibility, set the docstring format in the extension's settings to "sphinx". Information is meaningless without context so sprinkle documentation liberally with refs: [Cross-referencing with Sphinx](https://docs.readthedocs.io/en/stable/guides/cross-referencing-with-sphinx.html), [Cross-referencing Python objects](https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#cross-referencing-python-objects).

When code is merged to `main` a Github action automatically deploys the documentation to Github Pages. Click "View deployment" in the (github-pages)[/synccomputingcode/sync_sdk/deployments/activity_log?environment=github-pages] environment to navigate to the latest documentation.

## CLI
The CLI in this repo is only here to support development of the SDK as an example consumer. It is not the primary concern of this repo and may be split out into a different repo at a later point in time.

## Developer Interface
The developer interface consists of the public attributes of modules and packages within the `sync` package, excluding `sync.cli`. With each change the impact to the version of the next release must be considered in regard to semantic versioning [semantic versioning](https://semver.org).

This SDK is organized by functional domain as hinted by the names of the modules under the `sync` package. Utilities for interacting with EMR and Databricks are in `sync.emr` and `sync.databricks`, respectively. These modules will provide functionality for starting jobs and collecting information required for Sync predictions. When starting jobs tags are applied and the event log location specified.

Successful responses and errors from public functions will be returned in an instance of the generic [Response](sync/models.py). Use of this model means that exceptions raised must be handled to provide helpful information in the error response.
## Configuration
Configuration at the installation site is required before the SDK can be used. See the user guide for details.

## For Future Consideration
At some point we may want to give users a choice of which platform to apply a prediction. This would likely entail an agnostic prediction configuration that could be translated to Databricks or EMR models.

A Spark application may be refactored to the point of invalidating the basis of a prediction. This could lead to a prediction being wildly off or altogether broken in that its configuration cannot yield a successful run. It may therefore be worthwhile to evaluate the state of the application code a key points. Concretely, a hash of the code could be persisted to facilitate tracking changes and warning users when a prediction may no longer be reliably applied.