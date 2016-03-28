Aurora SLA Measurement
======================

- [Overview](#overview)
- [Metric Details](#metric-details)
  - [Platform Uptime](#platform-uptime)
  - [Job Uptime](#job-uptime)
  - [Median Time To Assigned (MTTA)](#median-time-to-assigned-\(mtta\))
  - [Median Time To Running (MTTR)](#median-time-to-running-\(mttr\))
- [Limitations](#limitations)

## Overview

The primary goal of the feature is collection and monitoring of Aurora job SLA (Service Level
Agreements) metrics that defining a contractual relationship between the Aurora/Mesos platform
and hosted services.

The Aurora SLA feature is by default only enabled for service (non-cron)
production jobs (`"production=True"` in your `.aurora` config). It can be enabled for
non-production services by an operator via the scheduler command line flag `-sla_non_prod_metrics`.

Counters that track SLA measurements are computed periodically within the scheduler.
The individual instance metrics are refreshed every minute (configurable via
`sla_stat_refresh_interval`). The instance counters are subsequently aggregated by
relevant grouping types before exporting to scheduler `/vars` endpoint (when using `vagrant`
that would be `http://192.168.33.7:8081/vars`)


## Metric Details

### Platform Uptime

*Aggregate amount of time a job spends in a non-runnable state due to platform unavailability
or scheduling delays. This metric tracks Aurora/Mesos uptime performance and reflects on any
system-caused downtime events (tasks LOST or DRAINED). Any user-initiated task kills/restarts
will not degrade this metric.*

**Collection scope:**

* Per job - `sla_<job_key>_platform_uptime_percent`
* Per cluster - `sla_cluster_platform_uptime_percent`

**Units:** percent

A fault in the task environment may cause the Aurora/Mesos to have different views on the task state
or lose track of the task existence. In such cases, the service task is marked as LOST and
rescheduled by Aurora. For example, this may happen when the task stays in ASSIGNED or STARTING
for too long or the Mesos slave becomes unhealthy (or disappears completely). The time between
task entering LOST and its replacement reaching RUNNING state is counted towards platform downtime.

Another example of a platform downtime event is the administrator-requested task rescheduling. This
happens during planned Mesos slave maintenance when all slave tasks are marked as DRAINED and
rescheduled elsewhere.

To accurately calculate Platform Uptime, we must separate platform incurred downtime from user
actions that put a service instance in a non-operational state. It is simpler to isolate
user-incurred downtime and treat all other downtime as platform incurred.

Currently, a user can cause a healthy service (task) downtime in only two ways: via `killTasks`
or `restartShards` RPCs. For both, their affected tasks leave an audit state transition trail
relevant to uptime calculations. By applying a special "SLA meaning" to exposed task state
transition records, we can build a deterministic downtime trace for every given service instance.

A task going through a state transition carries one of three possible SLA meanings
(see [SlaAlgorithm.java](../../src/main/java/org/apache/aurora/scheduler/sla/SlaAlgorithm.java) for
sla-to-task-state mapping):

* Task is UP: starts a period where the task is considered to be up and running from the Aurora
  platform standpoint.

* Task is DOWN: starts a period where the task cannot reach the UP state for some
  non-user-related reason. Counts towards instance downtime.

* Task is REMOVED from SLA: starts a period where the task is not expected to be UP due to
  user initiated action or failure. We ignore this period for the uptime calculation purposes.

This metric is recalculated over the last sampling period (last minute) to account for
any UP/DOWN/REMOVED events. It ignores any UP/DOWN events not immediately adjacent to the
sampling interval as well as adjacent REMOVED events.

### Job Uptime

*Percentage of the job instances considered to be in RUNNING state for the specified duration
relative to request time. This is a purely application side metric that is considering aggregate
uptime of all RUNNING instances. Any user- or platform initiated restarts directly affect
this metric.*

**Collection scope:** We currently expose job uptime values at 5 pre-defined
percentiles (50th,75th,90th,95th and 99th):

* `sla_<job_key>_job_uptime_50_00_sec`
* `sla_<job_key>_job_uptime_75_00_sec`
* `sla_<job_key>_job_uptime_90_00_sec`
* `sla_<job_key>_job_uptime_95_00_sec`
* `sla_<job_key>_job_uptime_99_00_sec`

**Units:** seconds
You can also get customized real-time stats from aurora client. See `aurora sla -h` for
more details.

### Median Time To Assigned (MTTA)

*Median time a job spends waiting for its tasks to be assigned to a host. This is a combined
metric that helps track the dependency of scheduling performance on the requested resources
(user scope) as well as the internal scheduler bin-packing algorithm efficiency (platform scope).*

**Collection scope:**

* Per job - `sla_<job_key>_mtta_ms`
* Per cluster - `sla_cluster_mtta_ms`
* Per instance size (small, medium, large, x-large, xx-large). Size are defined in:
[ResourceAggregates.java](../../src/main/java/org/apache/aurora/scheduler/base/ResourceAggregates.java)
  * By CPU:
    * `sla_cpu_small_mtta_ms`
    * `sla_cpu_medium_mtta_ms`
    * `sla_cpu_large_mtta_ms`
    * `sla_cpu_xlarge_mtta_ms`
    * `sla_cpu_xxlarge_mtta_ms`
  * By RAM:
    * `sla_ram_small_mtta_ms`
    * `sla_ram_medium_mtta_ms`
    * `sla_ram_large_mtta_ms`
    * `sla_ram_xlarge_mtta_ms`
    * `sla_ram_xxlarge_mtta_ms`
  * By DISK:
    * `sla_disk_small_mtta_ms`
    * `sla_disk_medium_mtta_ms`
    * `sla_disk_large_mtta_ms`
    * `sla_disk_xlarge_mtta_ms`
    * `sla_disk_xxlarge_mtta_ms`

**Units:** milliseconds

MTTA only considers instances that have already reached ASSIGNED state and ignores those
that are still PENDING. This ensures straggler instances (e.g. with unreasonable resource
constraints) do not affect metric curves.

### Median Time To Running (MTTR)

*Median time a job waits for its tasks to reach RUNNING state. This is a comprehensive metric
reflecting on the overall time it takes for the Aurora/Mesos to start executing user content.*

**Collection scope:**

* Per job - `sla_<job_key>_mttr_ms`
* Per cluster - `sla_cluster_mttr_ms`
* Per instance size (small, medium, large, x-large, xx-large). Size are defined in:
[ResourceAggregates.java](../../src/main/java/org/apache/aurora/scheduler/base/ResourceAggregates.java)
  * By CPU:
    * `sla_cpu_small_mttr_ms`
    * `sla_cpu_medium_mttr_ms`
    * `sla_cpu_large_mttr_ms`
    * `sla_cpu_xlarge_mttr_ms`
    * `sla_cpu_xxlarge_mttr_ms`
  * By RAM:
    * `sla_ram_small_mttr_ms`
    * `sla_ram_medium_mttr_ms`
    * `sla_ram_large_mttr_ms`
    * `sla_ram_xlarge_mttr_ms`
    * `sla_ram_xxlarge_mttr_ms`
  * By DISK:
    * `sla_disk_small_mttr_ms`
    * `sla_disk_medium_mttr_ms`
    * `sla_disk_large_mttr_ms`
    * `sla_disk_xlarge_mttr_ms`
    * `sla_disk_xxlarge_mttr_ms`

**Units:** milliseconds

MTTR only considers instances in RUNNING state. This ensures straggler instances (e.g. with
unreasonable resource constraints) do not affect metric curves.

## Limitations

* The availability of Aurora SLA metrics is bound by the scheduler availability.

* All metrics are calculated at a pre-defined interval (currently set at 1 minute).
  Scheduler restarts may result in missed collections.
