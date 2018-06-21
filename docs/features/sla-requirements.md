SLA Requirements
================

- [Overview](#overview)
- [Default SLA](#default-sla)
- [Custom SLA](#custom-sla)
  - [Count-based](#count-based)
  - [Percentage-based](#percentage-based)
  - [Coordinator-based](#coordinator-based)

## Overview

Aurora guarantees SLA requirements for jobs. These requirements limit the impact of cluster-wide
maintenance operations on the jobs. For instance, when an operator upgrades
the OS on all the Mesos agent machines, the tasks scheduled on them needs to be drained.
By specifying the SLA requirements a job can make sure that it has enough instances to
continue operating safely without incurring downtime.

> SLA is defined as minimum number of active tasks required for a job every duration window.
A task is active if it was in `RUNNING` state during the last duration window.

There is a [default](#default-sla) SLA guarantee for
[preferred](../features/multitenancy.md#configuration-tiers) tier jobs and it is also possible to
specify [custom](#custom-sla) SLA requirements.

## Default SLA

Aurora guarantees a default SLA requirement for tasks in
[preferred](../features/multitenancy.md#configuration-tiers) tier.

> 95% of tasks in a job will be `active` for every 30 mins.


## Custom SLA

For jobs that require different SLA requirements, Aurora allows jobs to specify their own
SLA requirements via the `SlaPolicies`. There are 3 different ways to express SLA requirements.

### [Count-based](../reference/configuration.md#countslapolicy-objects)

For jobs that need a minimum `number` of instances to be running all the time,
[`CountSlaPolicy`](../reference/configuration.md#countslapolicy-objects)
provides the ability to express the minimum number of required active instances (i.e. number of
tasks that are `RUNNING` for at least `duration_secs`). For instance, if we have a
`replicated-service` that has 3 instances and needs at least 2 instances every 30 minutes to be
treated healthy, the SLA requirement can be expressed with a
[`CountSlaPolicy`](../reference/configuration.md#countslapolicy-objects) like below,

```python
Job(
  name = 'replicated-service',
  role = 'www-data',
  instances = 3,
  sla_policy = CountSlaPolicy(
    count = 2,
    duration_secs = 1800
  )
  ...
)
```

### [Percentage-based](../reference/configuration.md#percentageslapolicy-objects)

For jobs that need a minimum `percentage` of instances to be running all the time,
[`PercentageSlaPolicy`](../reference/configuration.md#percentageslapolicy-objects) provides the
ability to express the minimum percentage of required active instances (i.e. percentage of tasks
that are `RUNNING` for at least `duration_secs`). For instance, if we have a `webservice` that
has 10000 instances for handling peak load and cannot have more than 0.1% of the instances down
for every 1 hr, the SLA requirement can be expressed with a
[`PercentageSlaPolicy`](../reference/configuration.md#percentageslapolicy-objects) like below,

```python
Job(
  name = 'frontend-service',
  role = 'www-data',
  instances = 10000,
  sla_policy = PercentageSlaPolicy(
    percentage = 99.9,
    duration_secs = 3600
  )
  ...
)
```

### [Coordinator-based](../reference/configuration.md#coordinatorslapolicy-objects)

When none of the above methods are enough to describe the SLA requirements for a job, then the SLA
calculation can be off-loaded to a custom service called the `Coordinator`. The `Coordinator` needs
to expose an endpoint that will be called to check if removal of a task will affect the SLA
requirements for the job. This is useful to control the number of tasks that undergoes maintenance
at a time, without affected the SLA for the application.

Consider the example, where we have a `storage-service` stores 2 replicas of an object. Each replica
is distributed across the instances, such that replicas are stored on different hosts. In addition
a consistent-hash is used for distributing the data across the instances.

When an instance needs to be drained (say for host-maintenance), we have to make sure that at least 1 of
the 2 replicas remains available. In such a case, a `Coordinator` service can be used to maintain
the SLA guarantees required for the job.

The job can be configured with a
[`CoordinatorSlaPolicy`](../reference/configuration.md#coordinatorslapolicy-objects) to specify the
coordinator endpoint and the field in the response JSON that indicates if the SLA will be affected
or not affected, when the task is removed.

```python
Job(
  name = 'storage-service',
  role = 'www-data',
  sla_policy = CoordinatorSlaPolicy(
    coordinator_url = 'http://coordinator.example.com',
    status_key = 'drain'
  )
  ...
)
```


#### Coordinator Interface [Experimental]

When a [`CoordinatorSlaPolicy`](../reference/configuration.md#coordinatorslapolicy-objects) is
specified for a job, any action that requires removing a task
(such as drains) will be required to get approval from the `Coordinator` before proceeding. The
coordinator service needs to expose a HTTP endpoint, that can take a `task-key` param
(`<cluster>/<role>/<env>/<name>/<instance>`) and a json body describing the task
details, force maintenance countdown (ms) and other params and return a response json that will
contain the boolean status for allowing or disallowing the task's removal.

##### Request:
```javascript
POST /
  ?task=<cluster>/<role>/<env>/<name>/<instance>

{
  "forceMaintenanceCountdownMs": "604755646",
  "task": "cluster/role/devel/job/1",
  "taskConfig": {
    "assignedTask": {
      "taskId": "taskA",
      "slaveHost": "a",
      "task": {
        "job": {
          "role": "role",
          "environment": "devel",
          "name": "job"
        },
        ...
      },
      "assignedPorts": {
        "http": 1000
      },
      "instanceId": 1
      ...
    },
    ...
  }
}
```

##### Response:
```json
{
  "drain": true
}
```

If Coordinator allows removal of the task, then the task’s
[termination lifecycle](../reference/configuration.md#httplifecycleconfig-objects)
is triggered. If Coordinator does not allow removal, then the request will be retried again in the
future.

#### Coordinator Actions

Coordinator endpoint get its own lock and this is used to serializes calls to the Coordinator.
It guarantees that only one concurrent request is sent to a coordinator endpoint. This allows
coordinators to simply look the current state of the tasks to determine its SLA (without having
to worry about in-flight and pending requests). However if there are multiple coordinators,
maintenance can be done in parallel across all the coordinators.

_Note: Single concurrent request to a coordinator endpoint does not translate as exactly-once
guarantee. The coordinator must be able to handle duplicate drain
requests for the same task._



