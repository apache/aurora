Aurora Job Updates
==================

`Job` configurations can be updated at any point in their lifecycle.
Usually updates are done incrementally using a process called a *rolling
upgrade*, in which Tasks are upgraded in small groups, one group at a
time.  Updates are done using various Aurora Client commands.


Rolling Job Updates
-------------------

There are several sub-commands to manage job updates:

    aurora update start <job key> <configuration file>
    aurora update info <job key>
    aurora update pause <job key>
    aurora update resume <job key>
    aurora update abort <job key>
    aurora update list <cluster>

When you `start` a job update, the command will return once it has sent the
instructions to the scheduler.  At that point, you may view detailed
progress for the update with the `info` subcommand, in addition to viewing
graphical progress in the web browser.  You may also get a full listing of
in-progress updates in a cluster with `list`.

Once an update has been started, you can `pause` to keep the update but halt
progress.  This can be useful for doing things like debug a  partially-updated
job to determine whether you would like to proceed.  You can `resume` to
proceed.

You may `abort` a job update regardless of the state it is in. This will
instruct the scheduler to completely abandon the job update and leave the job
in the current (possibly partially-updated) state.

For a configuration update, the Aurora Client calculates required changes
by examining the current job config state and the new desired job config.
It then starts a *rolling batched update process* by going through every batch
and performing these operations:

- If an instance is present in the scheduler but isn't in the new config,
  then that instance is killed.
- If an instance is not present in the scheduler but is present in
  the new config, then the instance is created.
- If an instance is present in both the scheduler and the new config, then
  the client diffs both task configs. If it detects any changes, it
  performs an instance update by killing the old config instance and adds
  the new config instance.

The Aurora client continues through the instance list until all tasks are
updated, in `RUNNING,` and healthy for a configurable amount of time.
If the client determines the update is not going well (a percentage of health
checks have failed), it cancels the update.

Update cancellation runs a procedure similar to the described above
update sequence, but in reverse order. New instance configs are swapped
with old instance configs and batch updates proceed backwards
from the point where the update failed. E.g.; (0,1,2) (3,4,5) (6,7,
8-FAIL) results in a rollback in order (8,7,6) (5,4,3) (2,1,0).

For details how to control a job update, please see the
[UpdateConfig](../reference/configuration.md#updateconfig-objects) configuration object.


Coordinated Job Updates
------------------------

Some Aurora services may benefit from having more control over updates by explicitly
acknowledging ("heartbeating") job update progress. This may be helpful for mission-critical
service updates where explicit job health monitoring is vital during the entire job update
lifecycle. Such job updates would rely on an external service (or a custom client) periodically
pulsing an active coordinated job update via a
[pulseJobUpdate RPC](../../api/src/main/thrift/org/apache/aurora/gen/api.thrift).

A coordinated update is defined by setting a positive
[pulse_interval_secs](../reference/configuration.md#updateconfig-objects) value in job configuration
file. If no pulses are received within specified interval the update will be blocked. A blocked
update is unable to continue rolling forward (or rolling back) but retains its active status.
It may only be unblocked by a fresh `pulseJobUpdate` call.

NOTE: A coordinated update starts in `ROLL_FORWARD_AWAITING_PULSE` state and will not make any
progress until the first pulse arrives. However, a paused update (`ROLL_FORWARD_PAUSED` or
`ROLL_BACK_PAUSED`) is still considered active and upon resuming will immediately make progress
provided the pulse interval has not expired.


Canary Deployments
------------------

Canary deployments are a pattern for rolling out updates to a subset of job instances,
in order to test different code versions alongside the actual production job.
It is a risk-mitigation strategy for job owners and commonly used in a form where
job instance 0 runs with a different configuration than the instances 1-N.

For example, consider a job with 4 instances that each
request 1 core of cpu, 1 GB of RAM, and 1 GB of disk space as specified
in the configuration file `hello_world.aurora`. If you want to
update it so it requests 2 GB of RAM instead of 1. You can create a new
configuration file to do that called `new_hello_world.aurora` and
issue

    aurora update start <job_key_value>/0-1 new_hello_world.aurora

This results in instances 0 and 1 having 1 cpu, 2 GB of RAM, and 1 GB of disk space,
while instances 2 and 3 have 1 cpu, 1 GB of RAM, and 1 GB of disk space. If instance 3
dies and restarts, it restarts with 1 cpu, 1 GB RAM, and 1 GB disk space.

So that means there are two simultaneous task configurations for the same job
at the same time, just valid for different ranges of instances. While this isn't a recommended
pattern, it is valid and supported by the Aurora scheduler.
