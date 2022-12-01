# Sync SDK
The Sync SDK is a library for building integrations with Sync Computing's API.

* [Goals](#goals)
  * [Out of Scope](#out-of-scope)
* [CLI](#cli)
* [Contribution Rules](#contribution-rules)
* [Design](#design)
  * [Developer Interface](#developer-interface)
  * [Configuration](#configuration)
    * [Site Credentials](#site-credentials)
    * [Site Configuration](#site-configuration)
    * [Tenant Configuration](#tenant-configuration)
  * [Tenant State](#tenant-state)
  * [Applying a Prediction](#applying-a-prediction)
    * [Event Log Location](#event-log-location)
  * [For Future Consideration](#for-future-consideration)
* [SDK Interactions](#sdk-interactions)
  * [Single Iteration of On-boarded Job / Project](#single-iteration-of-on-boarded-job--project)

## Goals
This SDK supports executing EMR and Databricks jobs, generating predictions based on those jobs and applying those predictions.

Specifically,
1. Start an EMR job from a [RunJobFlow](https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html) spec or a Databricks job from a [Clusters 2.0 Create](https://docs.databricks.com/dev-tools/api/latest/clusters.html#create) spec with the configuration to make the resources necessary for a prediction available, e.g. event log.
2. Start an EMR or Databricks job from a Sync prediction.
3. Create a prediction.

### Out of Scope
Orchestration that supports either,
1. Running more than 1 job at a time
2. Monitoring a job (although job tracking is supported by tagging and configuration when using this SDK to submit a job)

Functions provided by this SDK are intended to be incorporated in orchestration tools to provide continuous improvement and tracking of Spark jobs.

## CLI
The CLI in this repo is only here to support development of the SDK as an example consumer. It is not the primary concern of this repo and may be split out into a different repo at a later point in time.

## Contribution Rules
Only add what we know we'll need - no speculative development. Contributions should be organized in well-defined orthogonal functions and classes - "building blocks" - with documentation and tests. Public functions are subject to the constraints of [semantic versioning](https://semver.org).

## Design
This SDK is organized by function as hinted by the names of the modules under the `sync` package. Utilities for interacting with EMR and Databricks are in `sync.emr` and `sync.databricks`, respectively. These modules will provide functionality for starting jobs and collecting information required for Sync predictions. When starting jobs tags will need to be applied, event log location will need to be specified and so will the steps.

First, some terminology:
1. Tenant: a Sync account (user for now, maybe org later)
2. Site: instance of SDK installation
3. Project: a Spark application for which there may be iterative predictions

### Developer Interface
The developer interface consists of the public attributes of modules and packages within the `sync` package, excluding `sync.cli`. With each change the impact to the version of the next release must be considered in regard to semantic versioning [semantic versioning](https://semver.org).

Successful responses and errors from public functions will be returned in an instance of the generic [Response](sync/models.py#L35-L45). Use of this model means that exceptions raised must be handled to provide helpful information in the error response.
### Configuration
Configuration is required before the SDK can be used. This configuration is broken down into 3 sections:
#### Site Credentials
This configuration contains only the API key for the site.
#### Site Configuration
This is for non-sensitive properties associated with the site. Including,
1. default prediction preference (see [Applying a Prediction](#applying-a-prediction))
2. default location for event logs and configuration required for a prediction
#### Tenant Configuration
This configuration contains the URL for the tenant "database" - see [Tenant State](#tenant-state), and any other preferences/settings spanning multiple jobs.

More documentation is in the `sync.config` module. To configure via the CLI install the SDK and run,
```
sync-cli configure
```
This will create the requisite files under ~/.sync (only Linux-based systems are supported).

### Tenant State
In order to continuously apply predictions we must know something about the Sync on-boarded jobs/projects. This will eventually be provided by the API, but for now a file - local or shared - will suffice. In this file, for each project will have,
1. Project ID (also called job ID) identifying a Spark application and tying together consecutive executions for continuous tuning
2. Cold start configuration
3. Location of data needed for predictions (S3 URL)
    1. Event log
    2. Other data collected from the cluster
4. Sync preferences pertaining to the project, e.g. performance vs economy


### Applying a Prediction
A key part of the SDK is getting and applying predictions. When getting a prediction configuration the preference, e.g. "balanced", can be specified. If it is not then the preference for the project will be used if present in the tenant state. After that it falls to the site configuration, then tenant configuration.

Things that currently need to be merged into the EMR prediction configuration before it can be applied, but can and should be applied by Sync's backend:
- Applications: "Spark"
- ReleaseLabel, e.g. "emr-6.2.0"
- Name - we're using the name from the event log, but should be using the name from the configuration provided by client tools instead
- Steps
- Tags
    - sync:project-id
    - sync:run-id
    - sync:prediction-id
    - sync:tenant-id
- Visible to all users (we're collecting this and should probably persist it across job runs)
- BootstrapActions
- Ec2SubnetIds

Things that need to be merged into the EMR prediction configuration by the SDK
- Event log configuration
- EMR log location

#### Event Log Location
When running a Spark job an S3 location for the event log will be specified based on a configured S3 bucket and prefix. These things can be specified at the project, site or tenant level - precedence flows in that order. In a cold-start scenario in which the configuration may not exist an error is raised. With the configuration the location for the event log is defined as
```
s3://{configured bucket}/{configured prefix}/{project ID}/{timestamp}/{run ID}
```
e.g. `s3://megacorp-jobs/sync-enabled/54129c79-ee4a-47cf-8bf3-3e2326443fbc/2022-11-15T13:51:29Z/01953ba2-ee4a-47cf-8bf3-80sbj2lapcn8`

The timestamp is there merely for convenience when browsing job history in the AWS console. A run ID is generated and added to the path to ensure a unique location.

### For Future Consideration
At some point we may want to give users a choice of which platform to apply a prediction. This would likely entail an agnostic prediction configuration that could be translated to Databricks or EMR models.

A Spark application may be refactored to the point of invalidating the basis of a prediction. This could lead to a prediction being wildly off or altogether broken in that its configuration cannot yield a successful run. It may therefore be worthwhile to evaluate the state of the application code a key points. Concretely, a hash of the code could be persisted to facilitate tracking changes and warning users when a prediction may no longer be reliably applied.

## SDK Interactions
### Single Iteration of On-boarded Job / Project
1. Agent invokes SDK with project ID and optionally a run ID to apply prediction.
    1. SDK looks up project from Sync API to get S3 location for event logs & client tools configuration
    2. With the S3 location SDK looks up event log & configuration from the latest run. If cluster configuration is missing SDK will attempt to get it.
    3. SDK creates a prediction based on the latest event log & configuration.
    4. If the prediction is successful it will be applied returning the cluster ID
    5. If the prediction is not successful SDK returns an error. It will not look for the next most recent event log. 
2. Agent invokes SDK with cluster ID on completion of cluster to save cluster information
    1. SDK gets cluster configuration and saves it alongside the event log
    2. SDK then updates project table with status of latest run
