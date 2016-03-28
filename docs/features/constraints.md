Scheduling Constraints
======================

By default, Aurora will pick any random slave with sufficient resources
in order to schedule a task. This scheduling choice can be further
restricted with the help of constraints.


Mesos Attributes
----------------

Data centers are often organized with hierarchical failure domains.  Common failure domains
include hosts, racks, rows, and PDUs.  If you have this information available, it is wise to tag
the Mesos slave with them as
[attributes](https://mesos.apache.org/documentation/attributes-resources/).

The Mesos slave `--attributes` command line argument can be used to mark slaves with
static key/value pairs, so called attributes (not to be confused with `--resources`, which are
dynamic and accounted).

For example, consider the host `cluster1-aaa-03-sr2` and its following attributes (given in
key:value format): `host:cluster1-aaa-03-sr2` and `rack:aaa`.

Aurora makes these attributes available for matching with scheduling constraints.


Limit Constraints
-----------------

Limit constraints allow to control machine diversity using constraints. The below
constraint ensures that no more than two instances of your job may run on a single host.
Think of this as a "group by" limit.

    Service(
      name = 'webservice',
      role = 'www-data',
      constraints = {
        'host': 'limit:2',
      }
      ...
    )


Likewise, you can use constraints to control rack diversity, e.g. at
most one task per rack:

    constraints = {
      'rack': 'limit:1',
    }

Use these constraints sparingly as they can dramatically reduce Tasks' schedulability.
Further details are available in the reference documentation on
[Scheduling Constraints](#specifying-scheduling-constraints).



Value Constraints
-----------------

Value constraints can be used to express that a certain attribute with a certain value
should be present on a Mesos slave. For example, the following job would only be
scheduled on nodes that claim to have an `SSD` as their disk.

    Service(
      name = 'webservice',
      role = 'www-data',
      constraints = {
        'disk': 'SSD',
      }
      ...
    )


Further details are available in the reference documentation on
[Scheduling Constraints](#specifying-scheduling-constraints).


Running stateful services
-------------------------

Aurora is best suited to run stateless applications, but it also accommodates for stateful services
like databases, or services that otherwise need to always run on the same machines.

### Dedicated attribute

Most of the Mesos attributes arbitrary and available for custom use.  There is one exception,
though: the `dedicated` attribute.  Aurora treats this specially, and only allows matching jobs to
run on these machines, and will only schedule matching jobs on these machines.


#### Syntax
The dedicated attribute has semantic meaning. The format is `$role(/.*)?`. When a job is created,
the scheduler requires that the `$role` component matches the `role` field in the job
configuration, and will reject the job creation otherwise.  The remainder of the attribute is
free-form. We've developed the idiom of formatting this attribute as `$role/$job`, but do not
enforce this. For example: a job `devcluster/www-data/prod/hello` with a dedicated constraint set as
`www-data/web.multi` will have its tasks scheduled only on Mesos slaves configured with:
`--attributes=dedicated:www-data/web.multi`.

A wildcard (`*`) may be used for the role portion of the dedicated attribute, which will allow any
owner to elect for a job to run on the host(s). For example: tasks from both
`devcluster/www-data/prod/hello` and `devcluster/vagrant/test/hello` with a dedicated constraint
formatted as `*/web.multi` will be scheduled only on Mesos slaves configured with
`--attributes=dedicated:*/web.multi`. This may be useful when assembling a virtual cluster of
machines sharing the same set of traits or requirements.

##### Example
Consider the following slave command line:

    mesos-slave --attributes="dedicated:db_team/redis" ...

And this job configuration:

    Service(
      name = 'redis',
      role = 'db_team',
      constraints = {
        'dedicated': 'db_team/redis'
      }
      ...
    )

The job configuration is indicating that it should only be scheduled on slaves with the attribute
`dedicated:db_team/redis`.  Additionally, Aurora will prevent any tasks that do _not_ have that
constraint from running on those slaves.

