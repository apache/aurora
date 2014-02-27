Aurora + Thermos Configuration Reference
========================================

- [Aurora + Thermos Configuration Reference](#aurora--thermos-configuration-reference)
- [Introduction](#introduction)
- [Process Schema](#process-schema)
    - [Process Objects](#process-objects)
      - [name](#name)
      - [cmdline](#cmdline)
      - [max_failures](#max_failures)
      - [daemon](#daemon)
      - [ephemeral](#ephemeral)
      - [min_duration](#min_duration)
      - [final](#final)
- [Task Schema](#task-schema)
    - [Task Object](#task-object)
      - [name](#name-1)
      - [processes](#processes)
        - [constraints](#constraints)
      - [resources](#resources)
      - [max_failures](#max_failures-1)
      - [max_concurrency](#max_concurrency)
      - [finalization_wait](#finalization_wait)
    - [Constraint Object](#constraint-object)
    - [Resource Object](#resource-object)
- [Job Schema](#job-schema)
    - [Job Objects](#job-objects)
    - [Services](#services)
    - [UpdateConfig Objects](#updateconfig-objects)
    - [HealthCheckConfig Objects](#healthcheckconfig-objects)
- [Specifying Scheduling Constraints](#specifying-scheduling-constraints)
- [Template Namespaces](#template-namespaces)
    - [mesos Namespace](#mesos-namespace)
    - [thermos Namespace](#thermos-namespace)
- [Basic Examples](#basic-examples)
    - [hello_world.aurora](#hello_worldaurora)
    - [Environment Tailoring](#environment-tailoring)
      - [hello_world_productionized.aurora](#hello_world_productionizedaurora)

Introduction
============

Don't know where to start? The Aurora configuration schema is very
powerful, and configurations can become quite complex for advanced use
cases.

For examples of simple configurations to get something up and running
quickly, check out the [Tutorial](tutorial.md). When you feel comfortable with the basics, move
on to the [Configuration Tutorial](configurationtutorial.md) for more in-depth coverage of
configuration design.

For additional basic configuration examples, see [the end of this document](#BasicExamples).

Process Schema
==============

Process objects consist of required `name` and `cmdline` attributes. You can customize Process
behavior with its optional attributes. Remember, Processes are handled by Thermos.

### Process Objects

<table border=1>
  <tbody>
  <tr>
   <th>Attribute Name</td>
    <th>Type</td>
    <th>Description</td>
  </tr>
  <tr>
    <td><code><b>name</b></code></td>
    <td>String</td>
    <td>Process name (Required)</td>
  </tr>
  <tr>
    <td><code>cmdline</code></td>
    <td>String</td>
    <td>Command line (Required)</td>
  </tr>
  <tr>
    <td><code>max_failures</code></td>
    <td>Integer</td>
    <td>Maximum process failures (Default: 1)</td>
  </tr>
  <tr>
    <td><code>daemon</code></td>
    <td>Boolean</td>
    <td>When True, this is a daemon process. (Default: False)</td>
  </tr>
  <tr>
    <td><code>ephemeral</code></td>
    <td>Boolean</td>
    <td>When True, this is an ephemeral process. (Default: False)</td>
  </tr>
    <td><code>min_duration</code></td>
    <td>Integer</td>
    <td>Minimum duration between process restarts in seconds. (Default: 15)</td>
  </tr>
  <tr>
    <td><code>final</code></td>
    <td>Boolean</td>
    <td>When True, this process is a finalizing one that should run last. (Default: False)</td>
  </tr>
</tbody>
</table>

#### name

The name is any valid UNIX filename string (specifically no
slashes, NULLs or leading periods). Within a Task object, each Process name
must be unique.

#### cmdline

The command line run by the process. The command line is invoked in a bash
subshell, so can involve fully-blown bash scripts. However, nothing is
supplied for command-line arguments so `$*` is unspecified.

#### max_failures

The maximum number of failures (non-zero exit statuses) this process can
have before being marked permanently failed and not retried. If a
process permanently fails, Thermos looks at the failure limit of the task
containing the process (usually 1) to determine if the task has
failed as well.

Setting `max_failures` to 0 makes the process retry
indefinitely until it achieves a successful (zero) exit status.
It retries at most once every `min_duration` seconds to prevent
an effective denial of service attack on the coordinating Thermos scheduler.

#### daemon

By default, Thermos processes are non-daemon. If `daemon` is set to True, a
successful (zero) exit status does not prevent future process runs.
Instead, the process reinvokes after `min_duration` seconds.
However, the maximum failure limit still applies. A combination of
`daemon=True` and `max_failures=0` causes a process to retry
indefinitely regardless of exit status. This should be avoided
for very short-lived processes because of the accumulation of
checkpointed state for each process run. When running in Mesos
specifically, `max_failures` is capped at 100.

#### ephemeral

By default, Thermos processes are non-ephemeral. If `ephemeral` is set to
True, the process' status is not used to determine if its containing task
has completed. For example, consider a task with a non-ephemeral
webserver process and an ephemeral logsaver process
that periodically checkpoints its log files to a centralized data store.
The task is considered finished once the webserver process has
completed, regardless of the logsaver's current status.

#### min_duration

Processes may succeed or fail multiple times during a single task's
duration. Each of these is called a *process run*. `min_duration` is
the minimum number of seconds the scheduler waits before running the
same process.

#### final

Processes can be grouped into two classes: ordinary processes and
finalizing processes. By default, Thermos processes are ordinary. They
run as long as the task is considered healthy (i.e., no failure
limits have been reached.) But once all regular Thermos processes
finish or the task reaches a certain failure threshold, it
moves into a "finalization" stage and runs all finalizing
processes. These are typically processes necessary for cleaning up the
task, such as log checkpointers, or perhaps e-mail notifications that
the task completed.

Finalizing processes may not depend upon ordinary processes or
vice-versa, however finalizing processes may depend upon other
finalizing processes and otherwise run as a typical process
schedule.

Task Schema
===========

Tasks fundamentally consist of a `name` and a list of Process objects stored as the
value of the `processes` attribute. Processes can be further constrained with
`constraints`. By default, `name`'s value inherits from the first Process in the
`processes` list, so for simple `Task` objects with one Process, `name`
can be omitted. In Mesos, `resources` is also required.

### Task Object

<table border=1>
  <tbody>
  <tr>
   <th>param</td>
    <th>type</td>
    <th>description</td>
  </tr>
  <tr>
    <td><code><b>name</b></code></td>
    <td>String</td>
    <td>Process name (Required) (Default: <code>{{processes0.name}})</td>
  </tr>
  <tr>
    <td><code><b>processes<b></code></td>
    <td>List of <code>Process</code> objects</td>
    <td>List of <code>Process</code> objects bound to this task. (Required)</td>
  </tr>
  <tr>
    <td><code>constraints</code></td>
    <td>List of <code>Constraint<code> objects</td>
    <td>List of <code>Constraint</code> objects constraining processes.</td>
  </tr>
  <tr>
    <td><code><b>resources</b></code></td>
    <td><code>Resource</code> object</td>
    <td>Resource footprint. (Required)</td>
  </tr>
  <tr>
    <td><code>max_failures</code></td>
    <td>Integer</td>
    <td>Maximum process failures before being considered failed (Default: 1)</td>
  </tr>
    <td><code>max_concurrency</code></td>
    <td>Integer</td>
    <td>Maximum number of concurrent processes (Default: 0, unlimited concurrency.)
  </tr>
  <tr>
    <td><code>finalization_wait</code></td>
    <td>Integer</td>
    <td>Amount of time allocated for finalizing processes, in seconds. (Default: 30)
  </tr>
</tbody>
</table>

#### name

`name` is a string denoting the name of this task. It defaults to the name of the first Process in
the list of Processes associated with the `processes` attribute.

#### processes

`processes` is an unordered list of `Process` objects. To constrain the order
in which they run, use `constraints`.

##### constraints

A list of `Constraint` objects. Currently it supports only one type,
the `order` constraint. `order` is a list of process names
that should run in the order given. For example,

        process = Process(cmdline = "echo hello {{name}}")
        task = Task(name = "echoes",
                    processes = [process(name = "jim"), process(name = "bob")],
                    constraints = [Constraint(order = ["jim", "bob"]))

Constraints can be supplied ad-hoc and in duplicate. Not all
Processes need be constrained, however Tasks with cycles are
rejected by the Thermos scheduler.

Use the `order` function as shorthand to generate `Constraint` lists.
The following:

        order(process1, process2)

is shorthand for

        [Constraint(order = [process1.name(), process2.name()])]

#### resources

Takes a `Resource` object, which specifies the amounts of CPU, memory, and disk space resources
to allocate to the Task.

#### max_failures

`max_failures` is the number of times processes that are part of this
Task can fail before the entire Task is marked for failure.

For example:

        template = Process(max_failures=10)
        task = Task(
          name = "fail",
          processes = [
             template(name = "failing", cmdline = "exit 1"),
             template(name = "succeeding", cmdline = "exit 0")
          ],
          max_failures=2)

The `failing` Process could fail 10 times before being marked as
permanently failed, and the `succeeding` Process would succeed on the
first run. The task would succeed despite only allowing for two failed
processes. To be more specific, there would be 10 failed process runs
yet 1 failed process.

#### max_concurrency

For Tasks with a number of expensive but otherwise independent
processes, you may want to limit the amount of concurrency
the Thermos scheduler provides rather than artificially constraining
it via `order` constraints. For example, a test framework may
generate a task with 100 test run processes, but wants to run it on
a machine with only 4 cores. You can limit the amount of parallelism to
4 by setting `max_concurrency=4` in your task configuration.

For example, the following task spawns 180 Processes ("mappers")
to compute individual elements of a 180 degree sine table, all dependent
upon one final Process ("reducer") to tabulate the results:

    def make_mapper(id):
      return Process(
        name = "mapper%03d" % id,
        cmdline = "echo 'scale=50;s(%d\*4\*a(1)/180)' | bc -l >
                   temp.sine_table.%03d" % (id, id))

    def make_reducer():
      return Process(name = "reducer", cmdline = "cat temp.\* | nl \> sine\_table.txt
                     && rm -f temp.\*")

    processes = map(make_mapper, range(180))

    task = Task(
      name = "mapreduce",
      processes = processes + [make\_reducer()],
      constraints = [Constraint(order = [mapper.name(), 'reducer']) for mapper
                     in processes],
      max_concurrency = 8)

#### finalization_wait

Tasks have three active stages: `ACTIVE`, `CLEANING`, and `FINALIZING`. The
`ACTIVE` stage is when ordinary processes run. This stage lasts as
long as Processes are running and the Task is healthy. The moment either
all Processes have finished successfully or the Task has reached a
maximum Process failure limit, it goes into `CLEANING` stage and send
SIGTERMs to all currently running Processes and their process trees.
Once all Processes have terminated, the Task goes into `FINALIZING` stage
and invokes the schedule of all Processes with the "final" attribute set to True.

This whole process from the end of `ACTIVE` stage to the end of `FINALIZING`
must happen within `finalization_wait` seconds. If it does not
finish during that time, all remaining Processes are sent SIGKILLs
(or if they depend upon uncompleted Processes, are
never invoked.)

Client applications with higher priority may force a shorter
finalization wait (e.g. through parameters to `thermos kill`), so this
is mostly a best-effort signal.

### Constraint Object

Current constraint objects only support a single ordering constraint, `order`,
which specifies its processes run sequentially in the order given. By
default, all processes run in parallel when bound to a `Task` without
ordering constraints.

<table border=1>
  <tbody>
  <tr>
   <th>param</td>
    <th>type</td>
    <th>description</td>
  </tr>
  <tr>
    <td><code><b>order</b></code></td>
    <td>List of String</td>
    <td> List of processes by name (String) that should be run serially.</td>
  </tr>
  </tbody>
</table>

### Resource Object

Specifies the amount of CPU, Ram, and disk resources the task needs. See the
[Resource Isolation document](resourceisolation.md) for suggested values and to understand how
resources are allocated.

<table border=1>
  <tbody>
  <tr>
   <th>param</td>
    <th>type</td>
    <th>description</td>
  </tr>
  <tr>
    <td><code>cpu</code></td>
    <td>Float</td>
    <td>Fractional number of cores required by the task.</td>
  </tr>
  <tr>
    <td><code>ram</code></td>
    <td>Integer</td>
    <td>Bytes of RAM required by the task.</td>
  </tr>
  <tr>
    <td><code>disk</code></td>
    <td>Integer</td>
    <td>Bytes of disk required by the task.</td>
  </tr>
  </tbody>
</table>

Job Schema
==========

### Job Objects

<table border=1>
  <tbody>
    <tr>
      <td><code>task</code></td>
      <td>Task</td>
      <td>The Task object to bind to this job. Required.</td></td>
    </tr>
    <tr>
      <td><code>name</code></td>
      <td>String</td>
      <td>Job name. (Default: inherited from the task attribute's name)</td>
    </tr>
    <tr>
      <td><code>role</code></td>
      <td>String</td>
      <td>Job role account. Required.</td>
    </tr>
    <tr>
      <td><code>cluster</code></td>
      <td>String</td>
      <td>Cluster in which this job is scheduled. Required.</td>
    </tr>
    <tr>
      <td><code>environment</code></td></td>
      <td>String</td></td>
      <td>Job environment, default <code>devel</code>. Must be one of <code>prod</code>, <code>devel</code>, <code>test</code> or <code>staging&lt;number&gt;</code>.</td>
    </tr>
    <tr>
      <td><code>contact</code></td>
      <td>String</td>
      <td>Best email address to reach the owner of the job. For production jobs, this is usually a team mailing list.</td>
    </tr>
    <tr>
      <td><code>instances</code></td>
      <td>Integer</td>
      <td>Number of instances (sometimes referred to as replicas or shards) of the task to create. (Default: 1)</td>
    </tr>
    <tr>
      <td><code>cron_schedule</code> <strong>(Present, but not supported and a no-op)</strong></td>
      <td>String</td>
      <td>UTC Cron schedule in cron format. May only be used with non-service jobs. Default: None (not a cron job.)</td>
    </tr>
    <tr>
      <td><code>cron_collision_policy</code> <strong>(Present, but not supported and a no-op)</strong></td>
      <td>String</td>
      <td>Policy to use when a cron job is triggered while a previous run is still active. KILL_EXISTING Kill the previous run, and schedule the new run CANCEL_NEW Let the previous run continue, and cancel the new run. RUN_OVERLAP Let the previous run continue, and schedule the new run. (Default: KILL_EXISTING)</td>
    </tr>
    <tr>
      <td><code>update_config</code></td>
      <td><code>update_config</code> object</td>
      <td>Parameters for controlling the rate and policy of rolling updates. </td>
    </tr>
    <tr>
      <td><code>constraints</code></td>
      <td>dict</td>
      <td>Scheduling constraints for the tasks. See the section on the <a href="#SchedulingConstraints">constraint specification language</a></td>
    </tr>
    <tr>
      <td><code>service</code></td>
      <td>Boolean</td>
      <td>If True, restart tasks regardless of success or failure. (Default: False)</td>
    </tr>
    <tr>
      <td><code>daemon</code></td>
      <td>Boolean</td>
      <td>A DEPRECATED alias for "service". (Default: False) </td>
    </tr>
    <tr>
      <td><code>max_task_failures</code></td>
      <td>Integer</td>
      <td>Maximum number of failures after which the task is considered to have failed (Default: 1) Set to -1 to allow for infinite failures</td>
    </tr>
    <tr>
      <td><code>priority</code></td>
      <td>Integer</td>
      <td>Preemption priority to give the task (Default 0). Tasks with higher priorities may preempt tasks at lower priorities.</td>
    </tr>
    <tr>
      <td><code>production</code></td>
      <td>Boolean</td>
      <td>Whether or not this is a production task backed by quota (Default: False) Production jobs may preempt any non-production job, and may only be preempted by production jobs in the same role and of higher priority. To run jobs at this level, the job role must have the appropriate quota.</td>
    </tr>
    <tr>
      <td><code>health_check_config</code></td>
      <td><code>heath_check_config</code> object</td>
      <td>Parameters for controlling a task's health checks via HTTP. Only used if a  health port was assigned with a command line wildcard.</td>
    </tr>
  </tbody>
</table>

### Services

Jobs with the `service` flag set to True are called Services. The `Service`
alias can be used as shorthand for `Job` with `service=True`.
Services are differentiated from non-service Jobs in that tasks
always restart on completion, whether successful or unsuccessful.
Jobs without the service bit set only restart up to
`max_task_failures` times and only if they terminated unsuccessfully
either due to human error or machine failure.

### UpdateConfig Objects

Parameters for controlling the rate and policy of rolling updates.

<table border=1>
  <tbody>
    <tr>
      <td><code>batch_size</code></td>
      <td>Integer</td>
      <td>Maximum number of shards to be updated in one iteration (Default: 1)</td>
    </tr>
    <tr>
      <td><code>restart_threshold</code></td>
      <td>Integer</td>
      <td>Maximum number of seconds before a shard must move into the <code>RUNNING</code> state before considered a failure (Default: 60)</td>
    </tr>
    <tr>
      <td><code>watch_secs</code></td>
      <td>Integer</td>
      <td>Minimum number of seconds a shard must remain in <code>RUNNING</code> state before considered a success (Default: 30)</td>
    </tr>
    <tr>
      <td><code>max_per_shard_failures</code></td>
      <td>Integer</td>
      <td>Maximum number of restarts per shard during update. Increments total failure count when this limit is exceeded. (Default: 0)</td>
    </tr>
    <tr>
      <td><code>max_total_failures</code></td>
      <td>Integer</td>
      <td>Maximum number of shard failures to be tolerated in total during an update. Cannot be greater than or equal to the total number of tasks in a job. (Default: 0)</td>
    </tr>
  </tbody>
</table>

### HealthCheckConfig Objects

Parameters for controlling a task's health checks via HTTP.

<table border=1>
  <tbody>
    <tr>
      <td><code>initial_interval_secs</code></td>
      <td>Integer</td>
      <td>Initial delay for performing an HTTP health check. (Default: 60)</td>
    </tr>
    <tr>
      <td><code>interval_secs</code></td>
      <td>Integer</td>
      <td>Interval on which to check the task's health via HTTP. (Default: 30)</td>
    </tr>
    <tr>
      <td><code>timeout_secs</code></td>
      <td>Integer</td>
      <td>HTTP request timeout. (Default: 1)</td>
    </tr>
    <tr>
      <td><code>max_consecutive_failures</code></td>
      <td>Integer</td>
      <td>Maximum number of consecutive failures that tolerated before considering a task unhealthy (Default: 0)</td>
     </tr>
    </tbody>
</table>

Specifying Scheduling Constraints
=================================

Most users will not need to specify constraints explicitly, as the
scheduler automatically inserts reasonable defaults that attempt to
ensure reliability without impacting schedulability. For example, the
scheduler inserts a `host: limit:1` constraint, ensuring
that your shards run on different physical machines. Please do not
set this field unless you are sure of what you are doing.

In the `Job` object there is a map `constraints` from String to String
allowing the user to tailor the schedulability of tasks within the job.

Each slave in the cluster is assigned a set of string-valued
key/value pairs called attributes. For example, consider the host
`cluster1-aaa-03-sr2` and its following attributes (given in key:value
format): `host:cluster1-aaa-03-sr2` and `rack:aaa`.

The constraint map's key value is the attribute name in which we
constrain Tasks within our Job. The value is how we constrain them.
There are two types of constraints: *limit constraints* and *value
constraints*.

<table border=1>
  <tbody>
    <tr>
      <td>Limit Constraint</td>
      <td>A string that specifies a limit for a constraint. Starts with <code>'limit:</code> followed by an Integer and closing single quote, such as <code>'limit:1'</code>.</td>
    </tr>
    <tr>
      <td>Value Constraint</td>
      <td>A string that specifies a value for a constraint. To include a list of values, separate the values using commas. To negate the values of a constraint, start with a <code>!</code><code>.</code></td>
    </tr>
  </tbody>
</table>

You can also control machine diversity using constraints. The below
constraint ensures that no more than two instances of your job may run
on a single host. Think of this as a "group by" limit.

    constraints = {
      'host': 'limit:2',
    }

Likewise, you can use constraints to control rack diversity, e.g. at
most one task per rack:

    constraints = {
      'rack': 'limit:1',
    }

Use these constraints sparingly as they can dramatically reduce Tasks' schedulability.

Template Namespaces
===================

Currently, a few Pystachio namespaces have special semantics. Using them
in your configuration allow you to tailor application behavior
through environment introspection or interact in special ways with the
Aurora client or Aurora-provided services.

### mesos Namespace

The `mesos` namespace contains the `instance` variable that can be used
to distinguish between Task replicas.

<table border=1>
  <tbody>
    <tr>
      <td><code>instance</code></td>
      <td>Integer</td>
      <td>The instance number of the created task. A job with 5 replicas has instance numbers 0, 1, 2, 3, and 4.</td>
    </tr>
  </tbody>
</table>

### thermos Namespace

The `thermos` namespace contains variables that work directly on the
Thermos platform in addition to Aurora. This namespace is fully
compatible with Tasks invoked via the `thermos` CLI.

<table border=1>
  <tbody>
    <tr>
      <td><code>ports</code></td>
      <td>map of string to Integer</td>
      <td>A map of names to port numbers</td>
    </tr>
    <tr>
      <td><code>task_id</code></td>
      <td>string</td>
      <td>The task ID assigned to this task.</td>
    </tr>
  </tbody>
</table>

The `thermos.ports` namespace is automatically populated by Aurora when
invoking tasks on Mesos. When running the `thermos` command directly,
these ports must be explicitly mapped with the `-P` option.

For example, if '{{`thermos.ports[http]`}}' is specified in a `Process`
configuration, it is automatically extracted and auto-populated by
Aurora, but must be specified with, for example, `thermos -P http:12345`
to map `http` to port 12345 when running via the CLI.

Basic Examples
==============

These are provided to give a basic understanding of simple Aurora jobs.

### hello_world.aurora

Put the following in a file named `hello_world.aurora`, substituting your own values
for values such as `cluster`s.

    import os
    hello_world_process = Process(name = 'hello_world', cmdline = 'echo hello world')

    hello_world_task = Task(
      resources = Resources(cpu = 0.1, ram = 16 * MB, disk = 16 * MB),
      processes = [hello_world_process])

    hello_world_job = Job(
      cluster = 'cluster1',
      role = os.getenv('USER'),
      task = hello_world_task)

    jobs = [hello_world_job]

Then issue the following commands to create and kill the job, using your own values for the job key.

    aurora create cluster1/$USER/test/hello_world hello_world.aurora

    aurora kill cluster1/$USER/test/hello_world

### Environment Tailoring

#### hello_world_productionized.aurora

Put the following in a file named `hello_world_productionized.aurora`, substituting your own values
for values such as `cluster`s.

    include('hello_world.aurora')

    production_resources = Resources(cpu = 1.0, ram = 512 * MB, disk = 2 * GB)
    staging_resources = Resources(cpu = 0.1, ram = 32 * MB, disk = 512 * MB)
    hello_world_template = hello_world(
        name = "hello_world-{{cluster}}"
        task = hello_world(resources=production_resources))

    jobs = [
      # production jobs
      hello_world_template(cluster = 'cluster1', instances = 25),
      hello_world_template(cluster = 'cluster2', instances = 15),

      # staging jobs
      hello_world_template(
        cluster = 'local',
        instances = 1,
        task = hello_world(resources=staging_resources)),
    ]

Then issue the following commands to create and kill the job, using your own values for the job key

    aurora create cluster1/$USER/test/hello_world-cluster1 hello_world_productionized.aurora

    aurora kill cluster1/$USER/test/hello_world-cluster1
