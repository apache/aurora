Aurora Configuration Reference
==============================

Don't know where to start? The Aurora configuration schema is very
powerful, and configurations can become quite complex for advanced use
cases.

For examples of simple configurations to get something up and running
quickly, check out the [Tutorial](../getting-started/tutorial.md). When you feel comfortable with the basics, move
on to the [Configuration Tutorial](configuration-tutorial.md) for more in-depth coverage of
configuration design.

- [Process Schema](#process-schema)
    - [Process Objects](#process-objects)
- [Task Schema](#task-schema)
    - [Task Object](#task-object)
    - [Constraint Object](#constraint-object)
    - [Resource Object](#resource-object)
- [Job Schema](#job-schema)
    - [Job Objects](#job-objects)
    - [UpdateConfig Objects](#updateconfig-objects)
    - [HealthCheckConfig Objects](#healthcheckconfig-objects)
    - [Announcer Objects](#announcer-objects)
    - [Container Objects](#container)
    - [LifecycleConfig Objects](#lifecycleconfig-objects)
- [Specifying Scheduling Constraints](#specifying-scheduling-constraints)
- [Template Namespaces](#template-namespaces)
    - [mesos Namespace](#mesos-namespace)
    - [thermos Namespace](#thermos-namespace)


Process Schema
==============

Process objects consist of required `name` and `cmdline` attributes. You can customize Process
behavior with its optional attributes. Remember, Processes are handled by Thermos.

### Process Objects

  **Attribute Name**  | **Type**    | **Description**
  ------------------- | :---------: | ---------------------------------
   **name**           | String      | Process name (Required)
   **cmdline**        | String      | Command line (Required)
   **max_failures**   | Integer     | Maximum process failures (Default: 1)
   **daemon**         | Boolean     | When True, this is a daemon process. (Default: False)
   **ephemeral**      | Boolean     | When True, this is an ephemeral process. (Default: False)
   **min_duration**   | Integer     | Minimum duration between process restarts in seconds. (Default: 5)
   **final**          | Boolean     | When True, this process is a finalizing one that should run last. (Default: False)
   **logger**         | Logger      | Struct defining the log behavior for the process. (Default: Empty)

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

#### logger

The default behavior of Thermos is to store stderr/stdout logs in files which grow unbounded.
In the event that you have large log volume, you may want to configure Thermos to automatically
rotate logs after they grow to a certain size, which can prevent your job from using more than its
allocated disk space.

Logger objects specify a `destination` for Process logs which is, by default, `file` - a pair of
`stdout` and `stderr` files. Its also possible to specify `console` to get logs output to
the Process stdout and stderr streams, `none` to suppress any logs output or `both` to send logs to
files and console streams.

The default Logger `mode` is `standard` which lets the stdout and stderr streams grow without bound.

  **Attribute Name**  | **Type**          | **Description**
  ------------------- | :---------------: | ---------------------------------
   **destination**    | LoggerDestination | Destination of logs. (Default: `file`)
   **mode**           | LoggerMode        | Mode of the logger. (Default: `standard`)
   **rotate**         | RotatePolicy      | An optional rotation policy. (Default: `Empty`)

A RotatePolicy describes log rotation behavior for when `mode` is set to `rotate` and it is ignored
otherwise. If `rotate` is `Empty` or `RotatePolicy()` when the `mode` is set to `rotate` the
defaults below are used.

  **Attribute Name**  | **Type**     | **Description**
  ------------------- | :----------: | ---------------------------------
   **log_size**       | Integer      | Maximum size (in bytes) of an individual log file. (Default: 100 MiB)
   **backups**        | Integer      | The maximum number of backups to retain. (Default: 5)

An example process configuration is as follows:

        process = Process(
          name='process',
          logger=Logger(
            destination=LoggerDestination('both'),
            mode=LoggerMode('rotate'),
            rotate=RotatePolicy(log_size=5*MB, backups=5)
          )
        )

Task Schema
===========

Tasks fundamentally consist of a `name` and a list of Process objects stored as the
value of the `processes` attribute. Processes can be further constrained with
`constraints`. By default, `name`'s value inherits from the first Process in the
`processes` list, so for simple `Task` objects with one Process, `name`
can be omitted. In Mesos, `resources` is also required.

### Task Object

   **param**               | **type**                         | **description**
   ---------               | :---------:                      | ---------------
   ```name```              | String                           | Process name (Required) (Default: ```processes0.name```)
   ```processes```         | List of ```Process``` objects    | List of ```Process``` objects bound to this task. (Required)
   ```constraints```       | List of ```Constraint``` objects | List of ```Constraint``` objects constraining processes.
   ```resources```         | ```Resource``` object            | Resource footprint. (Required)
   ```max_failures```      | Integer                          | Maximum process failures before being considered failed (Default: 1)
   ```max_concurrency```   | Integer                          | Maximum number of concurrent processes (Default: 0, unlimited concurrency.)
   ```finalization_wait``` | Integer                          | Amount of time allocated for finalizing processes, in seconds. (Default: 30)

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

The `order` function accepts Process name strings `('foo', 'bar')` or the processes
themselves, e.g. `foo=Process(name='foo', ...)`, `bar=Process(name='bar', ...)`,
`constraints=order(foo, bar)`.

#### resources

Takes a `Resource` object, which specifies the amounts of CPU, memory, and disk space resources
to allocate to the Task.

#### max_failures

`max_failures` is the number of failed processes needed for the `Task` to be
marked as failed.

For example, assume a Task has two Processes and a `max_failures` value of `2`:

        template = Process(max_failures=10)
        task = Task(
          name = "fail",
          processes = [
             template(name = "failing", cmdline = "exit 1"),
             template(name = "succeeding", cmdline = "exit 0")
          ],
          max_failures=2)

The `failing` Process could fail 10 times before being marked as permanently
failed, and the `succeeding` Process could succeed on the first run. However,
the task would succeed despite only allowing for two failed processes. To be more
specific, there would be 10 failed process runs yet 1 failed process. Both processes
would have to fail for the Task to fail.

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

Process execution is organizued into three active stages: `ACTIVE`,
`CLEANING`, and `FINALIZING`. The `ACTIVE` stage is when ordinary processes run.
This stage lasts as long as Processes are running and the Task is healthy.
The moment either all Processes have finished successfully or the Task has reached a
maximum Process failure limit, it goes into `CLEANING` stage and send
SIGTERMs to all currently running Processes and their process trees.
Once all Processes have terminated, the Task goes into `FINALIZING` stage
and invokes the schedule of all Processes with the "final" attribute set to True.

This whole process from the end of `ACTIVE` stage to the end of `FINALIZING`
must happen within `finalization_wait` seconds. If it does not
finish during that time, all remaining Processes are sent SIGKILLs
(or if they depend upon uncompleted Processes, are
never invoked.)

When running on Aurora, the `finalization_wait` is capped at 60 seconds.

### Constraint Object

Current constraint objects only support a single ordering constraint, `order`,
which specifies its processes run sequentially in the order given. By
default, all processes run in parallel when bound to a `Task` without
ordering constraints.

   param | type           | description
   ----- | :----:         | -----------
   order | List of String | List of processes by name (String) that should be run serially.

### Resource Object

Specifies the amount of CPU, Ram, and disk resources the task needs. See the
[Resource Isolation document](../features/resource-isolation.md) for suggested values and to understand how
resources are allocated.

  param      | type    | description
  -----      | :----:  | -----------
  ```cpu```  | Float   | Fractional number of cores required by the task.
  ```ram```  | Integer | Bytes of RAM required by the task.
  ```disk``` | Integer | Bytes of disk required by the task.
  ```gpu```  | Integer | Number of GPU cores required by the task


Job Schema
==========

### Job Objects

*Note: Specifying a ```Container``` object as the value of the ```container``` property is
  deprecated in favor of setting its value directly to the appropriate ```Docker``` or ```Mesos```
  container type*

*Note: Specifying preemption behavior of tasks through `production` flag is deprecated in favor of
  electing appropriate task tier via `tier` attribute.*

   name | type | description
   ------ | :-------: | -------
  ```task``` | Task | The Task object to bind to this job. Required.
  ```name``` | String | Job name. (Default: inherited from the task attribute's name)
  ```role``` | String | Job role account. Required.
  ```cluster``` | String | Cluster in which this job is scheduled. Required.
  ```environment``` | String | Job environment, default ```devel```. By default must be one of ```prod```, ```devel```, ```test``` or ```staging<number>``` but it can be changed by the Cluster operator using the scheduler option `allowed_job_environments`.
  ```contact``` | String | Best email address to reach the owner of the job. For production jobs, this is usually a team mailing list.
  ```instances```| Integer | Number of instances (sometimes referred to as replicas or shards) of the task to create. (Default: 1)
  ```cron_schedule``` | String | Cron schedule in cron format. May only be used with non-service jobs. See [Cron Jobs](../features/cron-jobs.md) for more information. Default: None (not a cron job.)
  ```cron_collision_policy``` | String | Policy to use when a cron job is triggered while a previous run is still active. KILL_EXISTING Kill the previous run, and schedule the new run CANCEL_NEW Let the previous run continue, and cancel the new run. (Default: KILL_EXISTING)
  ```update_config``` | ```UpdateConfig``` object | Parameters for controlling the rate and policy of rolling updates.
  ```constraints``` | dict | Scheduling constraints for the tasks. See the section on the [constraint specification language](#specifying-scheduling-constraints)
  ```service``` | Boolean | If True, restart tasks regardless of success or failure. (Default: False)
  ```max_task_failures``` | Integer | Maximum number of failures after which the task is considered to have failed (Default: 1) Set to -1 to allow for infinite failures
  ```priority``` | Integer | Preemption priority to give the task (Default 0). Tasks with higher priorities may preempt tasks at lower priorities.
  ```production``` | Boolean |  (Deprecated) Whether or not this is a production task that may [preempt](../features/multitenancy.md#preemption) other tasks (Default: False). Production job role must have the appropriate [quota](../features/multitenancy.md#preemption).
  ```health_check_config``` | ```HealthCheckConfig``` object | Parameters for controlling a task's health checks. HTTP health check is only used if a  health port was assigned with a command line wildcard.
  ```container``` | Choice of ```Container```, ```Docker``` or ```Mesos``` object | An optional container to run all processes inside of.
  ```lifecycle``` | ```LifecycleConfig``` object | An optional task lifecycle configuration that dictates commands to be executed on startup/teardown.  HTTP lifecycle is enabled by default if the "health" port is requested.  See [LifecycleConfig Objects](#lifecycleconfig-objects) for more information.
  ```tier``` | String | Task tier type. The default scheduler tier configuration allows for 3 tiers: `revocable`, `preemptible`, and `preferred`. If a tier is not elected, Aurora assigns the task to a tier based on its choice of `production` (that is `preferred` for production and `preemptible` for non-production jobs). See the section on [Configuration Tiers](../features/multitenancy.md#configuration-tiers) for more information.
  ```announce``` | ```Announcer``` object | Optionally enable Zookeeper ServerSet announcements. See [Announcer Objects] for more information.
  ```enable_hooks``` | Boolean | Whether to enable [Client Hooks](client-hooks.md) for this job. (Default: False)
  ```partition_policy``` | ```PartitionPolicy``` object | An optional partition policy that allows job owners to define how to handle partitions for running tasks (in partition-aware Aurora clusters)
  ```metadata``` | list of ```Metadata``` objects | list of ```Metadata``` objects for user's customized metadata information.


### UpdateConfig Objects

Parameters for controlling the rate and policy of rolling updates.

| object                       | type     | description
| ---------------------------- | :------: | ------------
| ```batch_size```             | Integer  | Maximum number of shards to be updated in one iteration (Default: 1)
| ```watch_secs```             | Integer  | Minimum number of seconds a shard must remain in ```RUNNING``` state before considered a success (Default: 45)
| ```max_per_shard_failures``` | Integer  | Maximum number of restarts per shard during update. Increments total failure count when this limit is exceeded. (Default: 0)
| ```max_total_failures```     | Integer  | Maximum number of shard failures to be tolerated in total during an update. Cannot be greater than or equal to the total number of tasks in a job. (Default: 0)
| ```rollback_on_failure```    | boolean  | When False, prevents auto rollback of a failed update (Default: True)
| ```wait_for_batch_completion```| boolean | When True, all threads from a given batch will be blocked from picking up new instances until the entire batch is updated. This essentially simulates the legacy sequential updater algorithm. (Default: False)
| ```pulse_interval_secs```    | Integer  |  Indicates a [coordinated update](../features/job-updates.md#coordinated-job-updates). If no pulses are received within the provided interval the update will be blocked. Beta-updater only. Will fail on submission when used with client updater. (Default: None)

### HealthCheckConfig Objects

Parameters for controlling a task's health checks via HTTP or a shell command.

| param                          | type      | description
| -------                        | :-------: | --------
| ```health_checker```           | HealthCheckerConfig | Configure what kind of health check to use.
| ```initial_interval_secs```    | Integer   | Initial grace period (during which health-check failures are ignored) while performing health checks. (Default: 15)
| ```interval_secs```            | Integer   | Interval on which to check the task's health. (Default: 10)
| ```max_consecutive_failures``` | Integer   | Maximum number of consecutive failures that will be tolerated before considering a task unhealthy (Default: 0)
| ```min_consecutive_successes``` | Integer   | Minimum number of consecutive successful health checks required before considering a task healthy (Default: 1)
| ```timeout_secs```             | Integer   | Health check timeout. (Default: 1)

### HealthCheckerConfig Objects
| param                          | type                | description
| -------                        | :-------:           | --------
| ```http```                     | HttpHealthChecker  | Configure health check to use HTTP. (Default)
| ```shell```                    | ShellHealthChecker | Configure health check via a shell command.

### HttpHealthChecker Objects
| param                          | type      | description
| -------                        | :-------: | --------
| ```endpoint```                 | String    | HTTP endpoint to check (Default: /health)
| ```expected_response```        | String    | If not empty, fail the HTTP health check if the response differs. Case insensitive. (Default: ok)
| ```expected_response_code```   | Integer   | If not zero, fail the HTTP health check if the response code differs. (Default: 0)

### ShellHealthChecker Objects
| param                          | type      | description
| -------                        | :-------: | --------
| ```shell_command```            | String    | An alternative to HTTP health checking. Specifies a shell command that will be executed. Any non-zero exit status will be interpreted as a health check failure.

### PartitionPolicy Objects
| param                          | type      | description
| -------                        | :-------: | --------
| ```reschedule```               | Boolean   | Whether or not to reschedule when running tasks become partitioned (Default: True)
| ```delay_secs```               | Integer   | How long to delay transitioning to LOST when running tasks are partitioned. (Default: 0)

### Metadata Objects

Describes a piece of user metadata in a key value pair

  param            | type            | description
  -----            | :----:          | -----------
  ```key```        | String          | Indicate which metadata the user provides
  ```value```      | String          | Provide the metadata content for corresponding key

### Announcer Objects

If the `announce` field in the Job configuration is set, each task will be
registered in the ServerSet `/aurora/role/environment/jobname` in the
zookeeper ensemble configured by the executor (which can be optionally overriden by specifying
`zk_path` parameter).  If no Announcer object is specified,
no announcement will take place.  For more information about ServerSets, see the [Service Discover](../features/service-discovery.md)
documentation.

By default, the hostname in the registered endpoints will be the `--hostname` parameter
that is passed to the mesos agent. To override the hostname value, the executor can be started
with `--announcer-hostname=<overriden_value>`. If you decide to use `--announcer-hostname` and if
the overriden value needs to change for every executor, then the executor has to be started inside a wrapper, see [Executor Wrapper](../operations/configuration.md#thermos-executor-wrapper).

For example, if you want the hostname in the endpoint to be an IP address instead of the hostname,
the `--hostname` parameter to the mesos agent can be set to the machine IP or the executor can
be started with `--announcer-hostname=<host_ip>` while wrapping the executor inside a script.

| object                         | type      | description
| -------                        | :-------: | --------
| ```primary_port```             | String    | Which named port to register as the primary endpoint in the ServerSet (Default: `http`)
| ```portmap```                  | dict      | A mapping of additional endpoints to be announced in the ServerSet (Default: `{ 'aurora': '{{primary_port}}' }`)
| ```zk_path```                  | String    | Zookeeper serverset path override (executor must be started with the `--announcer-allow-custom-serverset-path` parameter)

#### Port aliasing with the Announcer `portmap`

The primary endpoint registered in the ServerSet is the one allocated to the port
specified by the `primary_port` in the `Announcer` object, by default
the `http` port.  This port can be referenced from anywhere within a configuration
as `{{thermos.ports[http]}}`.

Without the port map, each named port would be allocated a unique port number.
The `portmap` allows two different named ports to be aliased together.  The default
`portmap` aliases the `aurora` port (i.e. `{{thermos.ports[aurora]}}`) to
the `http` port.  Even though the two ports can be referenced independently,
only one port is allocated by Mesos.  Any port referenced in a `Process` object
but which is not in the portmap will be allocated dynamically by Mesos and announced as well.

It is possible to use the portmap to alias names to static port numbers, e.g.
`{'http': 80, 'https': 443, 'aurora': 'http'}`.  In this case, referencing
`{{thermos.ports[aurora]}}` would look up `{{thermos.ports[http]}}` then
find a static port 80.  No port would be requested of or allocated by Mesos.

Static ports should be used cautiously as Aurora does nothing to prevent two
tasks with the same static port allocations from being co-scheduled.
External constraints such as agent attributes should be used to enforce such
guarantees should they be needed.


### Container Objects

Describes the container the job's processes will run inside. If not using Docker or the Mesos
unified-container, the container can be omitted from your job config.

  param          | type           | description
  -----          | :----:         | -----------
  ```mesos```    | Mesos          | A native Mesos container to use.
  ```docker```   | Docker         | A Docker container to use (via Docker engine)

### Mesos Object

  param            | type                           | description
  -----            | :----:                         | -----------
  ```image```      | Choice(AppcImage, DockerImage) | An optional filesystem image to use within this container.
  ```volumes```    | List(Volume)                   | An optional list of volume mounts for this container.

### Volume Object

  param                  | type     | description
  -----                  | :----:   | -----------
  ```container_path```   | String   | Path on the host to mount.
  ```host_path```        | String   | Mount point in the container.
  ```mode```             | Enum     | Mode of the mount, can be 'RW' or 'RO'.

### AppcImage

Describes an AppC filesystem image.

  param          | type   | description
  -----          | :----: | -----------
  ```name```     | String | The name of the appc image.
  ```image_id``` | String | The [image id](https://github.com/appc/spec/blob/master/spec/aci.md#image-id) of the appc image.

### DockerImage

Describes a Docker filesystem image.

  param      | type   | description
  -----      | :----: | -----------
  ```name``` | String | The name of the docker image.
  ```tag```  | String | The tag that identifies the docker image.


### Docker Object

*Note: In order to correctly execute processes inside a job, the Docker container must have Python 2.7 installed.*
*Note: For private docker registry, mesos mandates the docker credential file to be named as `.dockercfg`, even though docker may create a credential file with a different name on various platforms. Also, the `.dockercfg` file needs to be copied into the sandbox using the `-thermos_executor_resources` flag, specified while starting Aurora.*

  param            | type            | description
  -----            | :----:          | -----------
  ```image```      | String          | The name of the docker image to execute.  If the image does not exist locally it will be pulled with ```docker pull```.
  ```parameters``` | List(Parameter) | Additional parameters to pass to the Docker engine.

### Docker Parameter Object

Docker CLI parameters. This needs to be enabled by the scheduler `-allow_docker_parameters` option.
See [Docker Command Line Reference](https://docs.docker.com/reference/commandline/run/) for valid parameters.

  param            | type            | description
  -----            | :----:          | -----------
  ```name```       | String          | The name of the docker parameter. E.g. volume
  ```value```      | String          | The value of the parameter. E.g. /usr/local/bin:/usr/bin:rw


### LifecycleConfig Objects

*Note: The only lifecycle configuration supported is the HTTP lifecycle via the HttpLifecycleConfig.*

  param          | type                | description
  -----          | :----:              | -----------
  ```http```     | HttpLifecycleConfig | Configure the lifecycle manager to send lifecycle commands to the task via HTTP.

### HttpLifecycleConfig Objects

*Note: The combined `graceful_shutdown_wait_secs` and `shutdown_wait_secs` is implicitly upper bounded by the `--stop_timeout_in_secs` flag exposed by the executor (see options [here](https://github.com/apache/aurora/blob/master/src/main/python/apache/aurora/executor/bin/thermos_executor_main.py), default is 2 minutes). Therefore, if the user specifies values that add up to more than `--stop_timeout_in_secs`, the task will be killed earlier than the user anticipates (see the termination lifecycle [here](https://aurora.apache.org/documentation/latest/reference/task-lifecycle/#forceful-termination-killing-restarting)). Furthermore, `stop_timeout_in_secs` itself is implicitly upper bounded by two scheduler options: `transient_task_state_timeout` and `preemption_slot_hold_time` (see reference [here](http://aurora.apache.org/documentation/latest/reference/scheduler-configuration/). If the `stop_timeout_in_secs` exceeds either of these scheduler options, tasks could be designated as LOST or tasks utilizing preemption could lose their desired slot respectively. Cluster operators should be aware of these timings should they change the defaults.*

  param                             | type    | description
  -----                             | :----:  | -----------
  ```port```                        | String  | The named port to send POST commands. (Default: health)
  ```graceful_shutdown_endpoint```  | String  | Endpoint to hit to indicate that a task should gracefully shutdown. (Default: /quitquitquit)
  ```shutdown_endpoint```           | String  | Endpoint to hit to give a task its final warning before being killed. (Default: /abortabortabort)
  ```graceful_shutdown_wait_secs``` | Integer | The amount of time (in seconds) to wait after hitting the ```graceful_shutdown_endpoint``` before proceeding with the [task termination lifecycle](https://aurora.apache.org/documentation/latest/reference/task-lifecycle/#forceful-termination-killing-restarting). (Default: 5)
  ```shutdown_wait_secs```          | Integer | The amount of time (in seconds) to wait after hitting the ```shutdown_endpoint``` before proceeding with the [task termination lifecycle](https://aurora.apache.org/documentation/latest/reference/task-lifecycle/#forceful-termination-killing-restarting). (Default: 5)

#### graceful_shutdown_endpoint

If the Job is listening on the port as specified by the HttpLifecycleConfig
(default: `health`), a HTTP POST request will be sent over localhost to this
endpoint to request that the task gracefully shut itself down.  This is a
courtesy call before the `shutdown_endpoint` is invoked
`graceful_shutdown_wait_secs` seconds later.

#### shutdown_endpoint

If the Job is listening on the port as specified by the HttpLifecycleConfig
(default: `health`), a HTTP POST request will be sent over localhost to this
endpoint to request as a final warning before being shut down.  If the task
does not shut down on its own after `shutdown_wait_secs` seconds, it will be
forcefully killed.


Specifying Scheduling Constraints
=================================

In the `Job` object there is a map `constraints` from String to String
allowing the user to tailor the schedulability of tasks within the job.

The constraint map's key value is the attribute name in which we
constrain Tasks within our Job. The value is how we constrain them.
There are two types of constraints: *limit constraints* and *value
constraints*.

| constraint    | description
| ------------- | --------------
| Limit         | A string that specifies a limit for a constraint. Starts with <code>'limit:</code> followed by an Integer and closing single quote, such as ```'limit:1'```.
| Value         | A string that specifies a value for a constraint. To include a list of values, separate the values using commas. To negate the values of a constraint, start with a ```!``` ```.```

Further details can be found in the [Scheduling Constraints](../features/constraints.md) feature
description.


Template Namespaces
===================

Currently, a few Pystachio namespaces have special semantics. Using them
in your configuration allow you to tailor application behavior
through environment introspection or interact in special ways with the
Aurora client or Aurora-provided services.

### mesos Namespace

The `mesos` namespace contains variables which relate to the `mesos` agent
which launched the task. The `instance` variable can be used
to distinguish between Task replicas.

| variable name     | type       | description
| --------------- | :--------: | -------------
| ```instance```    | Integer    | The instance number of the created task. A job with 5 replicas has instance numbers 0, 1, 2, 3, and 4.
| ```hostname``` | String | The instance hostname that the task was launched on.

Please note, there is no uniqueness guarantee for `instance` in the presence of
network partitions. If that is required, it should be baked in at the application
level using a distributed coordination service such as Zookeeper.

### thermos Namespace

The `thermos` namespace contains variables that work directly on the
Thermos platform in addition to Aurora. This namespace is fully
compatible with Tasks invoked via the `thermos` CLI.

| variable      | type                     | description                        |
| :----------:  | ---------                | ------------                       |
| ```ports```   | map of string to Integer | A map of names to port numbers     |
| ```task_id``` | string                   | The task ID assigned to this task. |

The `thermos.ports` namespace is automatically populated by Aurora when
invoking tasks on Mesos. When running the `thermos` command directly,
these ports must be explicitly mapped with the `-P` option.

For example, if '{{`thermos.ports[http]`}}' is specified in a `Process`
configuration, it is automatically extracted and auto-populated by
Aurora, but must be specified with, for example, `thermos -P http:12345`
to map `http` to port 12345 when running via the CLI.
