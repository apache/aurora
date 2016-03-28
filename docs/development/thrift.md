Thrift
======

Aurora uses [Apache Thrift](https://thrift.apache.org/) for representing structured data in
client/server RPC protocol as well as for internal data storage. While Thrift is capable of
correctly handling additions and renames of the existing members, field removals must be done
carefully to ensure backwards compatibility and provide predictable deprecation cycle. This
document describes general guidelines for making Thrift schema changes to the existing fields in
[api.thrift](../../api/src/main/thrift/org/apache/aurora/gen/api.thrift).

It is highly recommended to go through the
[Thrift: The Missing Guide](http://diwakergupta.github.io/thrift-missing-guide/) first to refresh on
basic Thrift schema concepts.

Checklist
---------
Every existing Thrift schema modification is unique in its requirements and must be analyzed
carefully to identify its scope and expected consequences. The following checklist may help in that
analysis:
* Is this a new field/struct? If yes, go ahead
* Is this a pure field/struct rename without any type/structure change? If yes, go ahead and rename
* Anything else, read further to make sure your change is properly planned

Deprecation cycle
-----------------
Any time a breaking change (e.g.: field replacement or removal) is required, the following cycle
must be followed:

### vCurrent
Change is applied in a way that does not break scheduler/client with this version to
communicate with scheduler/client from vCurrent-1.
* Do not remove or rename the old field
* Add a new field as an eventual replacement of the old one and implement a dual read/write
anywhere the old field is used. If a thrift struct is mapped in the DB store make sure both columns
are marked as `NOT NULL`
* Check [storage.thrift](../../api/src/main/thrift/org/apache/aurora/gen/storage.thrift) to see if the
affected struct is stored in Aurora scheduler storage. If so, you most likely need to backfill
existing data to ensure both fields are populated eagerly on startup. See
[this patch](https://reviews.apache.org/r/43172) as a real-life example of thrift-struct
backfilling. IMPORTANT: backfilling implementation needs to ensure both fields are populated. This
is critical to enable graceful scheduler upgrade as well as rollback to the old version if needed.
* Add a deprecation jira ticket into the vCurrent+1 release candidate
* Add a TODO for the deprecated field mentioning the jira ticket

### vCurrent+1
Finalize the change by removing the deprecated fields from the Thrift schema.
* Drop any dual read/write routines added in the previous version
* Remove thrift backfilling in scheduler
* Remove the deprecated Thrift field

Testing
-------
It's always advisable to test your changes in the local vagrant environment to build more
confidence that you change is backwards compatible. It's easy to simulate different
client/scheduler versions by playing with `aurorabuild` command. See [this document](../getting-started/vagrant.md)
for more.

