Mesos Fetcher
=============

Mesos has support for downloading resources into the sandbox through the
use of the [Mesos Fetcher](http://mesos.apache.org/documentation/latest/fetcher/)

Aurora supports passing URIs to the Mesos Fetcher dynamically by including
a list of URIs in job submissions.

How to use
----------
The scheduler flag `-enable_mesos_fetcher` must be set to true.

Currently only the scheduler side of this feature has been implemented
so a modification to the existing client, or a custom Thrift client are required
to make use of this feature.

If using a custom Thrift client, the list of URIs must be included in TaskConfig
as the `mesosFetcherUris` field.

Each Mesos Fetcher URI has the following data members:

|Property | Description|
|---------|------|
|value (required)  |Path to the resource needed in the sandbox.|
|extract (optional)|Extract files from packed or compressed archives into the sandbox.|
|cache (optional) | Use caching mechanism provided by Mesos for resources.|

Note that this structure is very similar to the one provided for downloading
resources needed for a [custom executor](../operations/configuration.md).

This is because both features use the Mesos fetcher to retrieve resources into
the sandbox. However, one, the custom executor feature, has a static set of URIs
set in the server side, and the other, the Mesos Fetcher feature, is a dynamic set
of URIs set at the time of job submission.

Security Implications
---------------------
There are security implications that must be taken into account when enabling this feature.
**Enabling this feature may potentially enable any job submitting user to perform a privilege escalation.**

Until a more through solution is created, one step that has been taken to mitigate this issue
is to statically mark every user submitted URI as non-executable. This is in contrast to the set of URIs
set in the custom executor feature which may mark any URI as executable.

If the need arises to mark a downloaded URI as executable, please consider using the custom executor feature.