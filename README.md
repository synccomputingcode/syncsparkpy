# Sync Library
The Sync Library provides drop-in functions facilitating integration between Sync's API and existing Apache Spark orchestrations. Integration enables tracking of high-level job metrics to provide optimized configurations to help meet SLAs while keeping costs down.

*Note*: This library is under active development and may contain features not yet fully supported.

* [Contribution Guidelines](#contribution-guidelines)
  * [Documentation](#documentation)
* [CLI](#cli)
* [Developer Interface](#developer-interface)
* [Configuration](#configuration)
* [Future Considerations](#future-considerations)


## Contribution Guidelines
Only add what provides clear benefit - no speculative development. Contributions should be organized in well-defined orthogonal functions and classes - "building blocks" - with documentation and tests. Public functions are subject to the constraints of [semantic versioning](https://semver.org). And be nice!

### Documentation
Documentation for consumers of the library is built using [Sphinx](https://www.sphinx-doc.org/en/master/). Pages are defined in [reStructuredText](https://docutils.sourceforge.io/rst.html) which may pull in reStructuredText from docstrings in the code. The VS Code extension, autoDocstring, provides a convenient way to initialize function docstrings. For compatibility, set the docstring format in the extension's settings to "sphinx". Information is meaningless without context so sprinkle documentation liberally with refs: [Cross-referencing with Sphinx](https://docs.readthedocs.io/en/stable/guides/cross-referencing-with-sphinx.html), [Cross-referencing Python objects](https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#cross-referencing-python-objects).

When code is merged to `main` a Github action automatically deploys the documentation to Github Pages. Click "View deployment" in the "github-pages" environment of this repo to navigate to the latest documentation. There's also a shortcut in the "About" section of the Github repo page.

To make sure documentation updates appear as intended before merging you can build and view it locally like,
```
% cd docs
% make html
% open _build/html/index.html
```

For troubleshooting documentation issues it may help to remove the "\_build" directory and all its content (`rm -rf _build`).

### Releases
Releases are semi-automated with the "Release new library version" Github workflow. To cut a new release update the version in [`sync/__init__.py`](sync/__init__.py) in a PR. Once it's merged run the "Release new library version" workflow on `main` from the "Actions" tab of the Github page for this repo. This will tag `main` with the new version, update the `latest` tag to match and create a Github release.

## CLI
The CLI is provided mainly for demonstration of what's possible when you integrate with Sync's API using this library. Explore available commands with `sync-cli --help`

## Developer Interface
The developer interface consists of the public attributes of the `sync.api` package, and the `sync.awsazure` and `sync.awsdatabricks` modules. With each change the impact to the version of the next release must be considered in regard to semantic versioning [semantic versioning](https://semver.org). The developer interface is built using clients including those in `sync.clients`. Clients in that package provide a raw interface to their corresponding services and are intended to support the developer interface only.

This library is organized by functional domain as hinted by the names of the modules under the `sync` package. Utilities for interacting with Databricks are in `sync.awsazure` and `sync.awsdatabricks`, respectively. These modules will provide functionality for starting jobs and consolidating information required for Sync predictions. When starting jobs tags are applied and the event log location specified.

Successful responses and errors from the developer interface will be returned in an instance of the generic [Response](sync/models.py). Use of this model means that exceptions raised must be handled by this library to provide helpful information in the error response.
## Configuration
Configuration at the installation site is required before the library can be used. See the user guide for details.