# Cron Jobs

Aurora supports execution of scheduled jobs on a Mesos cluster using cron-style syntax.

- [Overview](#overview)
- [Collision Policies](#collision-policies)
- [Failure recovery](#failure-recovery)
- [Interacting with cron jobs via the Aurora CLI](#interacting-with-cron-jobs-via-the-aurora-cli)
	- [cron schedule](#cron-schedule)
	- [cron deschedule](#cron-deschedule)
	- [cron start](#cron-start)
	- [job killall, job restart, job kill](#job-killall-job-restart-job-kill)
- [Technical Note About Syntax](#technical-note-about-syntax)
- [Caveats](#caveats)
	- [Failovers](#failovers)
	- [Collision policy is best-effort](#collision-policy-is-best-effort)
	- [Timezone Configuration](#timezone-configuration)

## Overview

A job is identified as a cron job by the presence of a
`cron_schedule` attribute containing a cron-style schedule in the
[`Job`](../reference/configuration.md#job-objects) object. Examples of cron schedules
include "every 5 minutes" (`*/5 * * * *`), "Fridays at 17:00" (`* 17 * * FRI`), and
"the 1st and 15th day of the month at 03:00" (`0 3 1,15 *`).

Example (available in the [Vagrant environment](../getting-started/vagrant.md)):

    $ cat /vagrant/examples/jobs/cron_hello_world.aurora
    # A cron job that runs every 5 minutes.
    jobs = [
      Job(
        cluster = 'devcluster',
        role = 'www-data',
        environment = 'test',
        name = 'cron_hello_world',
        cron_schedule = '*/5 * * * *',
        task = SimpleTask(
          'cron_hello_world',
          'echo "Hello world from cron, the time is now $(date --rfc-822)"'),
      ),
    ]

## Collision Policies

The `cron_collision_policy` field specifies the scheduler's behavior when a new cron job is
triggered while an older run hasn't finished. The scheduler has two policies available:

* `KILL_EXISTING`: The default policy - on a collision the old instances are killed and a instances with the current
configuration are started.
* `CANCEL_NEW`: On a collision the new run is cancelled.

Note that the use of `CANCEL_NEW` is likely a code smell - interrupted cron jobs should be able
to recover their progress on a subsequent invocation, otherwise they risk having their work queue
grow faster than they can process it.

## Failure recovery

Unlike with services, which aurora will always re-execute regardless of exit status, instances of
cron jobs retry according to the `max_task_failures` attribute of the
[Task](../reference/configuration.md#task-objects) object. To get "run-until-success" semantics,
set `max_task_failures` to `-1`.

## Interacting with cron jobs via the Aurora CLI

Most interaction with cron jobs takes place using the `cron` subcommand. See `aurora cron -h`
for up-to-date usage instructions.

### cron schedule
Schedules a new cron job on the Aurora cluster for later runs or replaces the existing cron template
with a new one. Only future runs will be affected, any existing active tasks are left intact.

    $ aurora cron schedule devcluster/www-data/test/cron_hello_world /vagrant/examples/jobs/cron_hello_world.aurora

### cron deschedule
Deschedules a cron job, preventing future runs but allowing current runs to complete.

    $ aurora cron deschedule devcluster/www-data/test/cron_hello_world

### cron start
Start a cron job immediately, outside of its normal cron schedule.

    $ aurora cron start devcluster/www-data/test/cron_hello_world

### job killall, job restart, job kill
Cron jobs create instances running on the cluster that you can interact with like normal Aurora
tasks with `job kill` and `job restart`.


## Technical Note About Syntax

`cron_schedule` uses a restricted subset of BSD crontab syntax. While the
execution engine currently uses Quartz, the schedule parsing is custom, a subset of FreeBSD
[crontab(5)](http://www.freebsd.org/cgi/man.cgi?crontab(5)) syntax. See
[the source](https://github.com/apache/aurora/blob/master/src/main/java/org/apache/aurora/scheduler/cron/CrontabEntry.java#L106-L124)
for details.


## Caveats

### Failovers
No failover recovery. Aurora does not record the latest minute it fired
triggers for across failovers. Therefore it's possible to miss triggers
on failover. Note that this behavior may change in the future.

It's necessary to sync time between schedulers with something like `ntpd`.
Clock skew could cause double or missed triggers in the case of a failover.

### Collision policy is best-effort
Aurora aims to always have *at least one copy* of a given instance running at a time - it's
an AP system, meaning it chooses Availability and Partition Tolerance at the expense of
Consistency.

If your collision policy was `CANCEL_NEW` and a task has terminated but
Aurora has not noticed this Aurora will go ahead and create your new
task.

If your collision policy was `KILL_EXISTING` and a task was marked `LOST`
but not yet GCed Aurora will go ahead and create your new task without
attempting to kill the old one (outside the GC interval).

### Timezone Configuration
Cron timezone is configured indepdendently of JVM timezone with the `-cron_timezone` flag and
defaults to UTC.
