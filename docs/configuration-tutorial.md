Aurora Configuration Tutorial
=============================

How to write Aurora configuration files, including feature descriptions
and best practices. When writing a configuration file, make use of
`aurora job inspect`. It takes the same job key and configuration file
arguments as `aurora job create` or `aurora job update`. It first ensures the
configuration parses, then outputs it in human-readable form.

You should read this after going through the general [Aurora Tutorial](tutorial.md).

- [Aurora Configuration Tutorial](#user-content-aurora-configuration-tutorial)
	- [The Basics](#user-content-the-basics)
		- [Use Bottom-To-Top Object Ordering](#user-content-use-bottom-to-top-object-ordering)
	- [An Example Configuration File](#user-content-an-example-configuration-file)
	- [Defining Process Objects](#user-content-defining-process-objects)
	- [Getting Your Code Into The Sandbox](#user-content-getting-your-code-into-the-sandbox)
	- [Defining Task Objects](#user-content-defining-task-objects)
		- [SequentialTask: Running Processes in Parallel or Sequentially](#user-content-sequentialtask-running-processes-in-parallel-or-sequentially)
		- [SimpleTask](#user-content-simpletask)
		- [Combining tasks](#user-content-combining-tasks)
	- [Defining Job Objects](#user-content-defining-job-objects)
	- [The jobs List](#user-content-the-jobs-list)
	- [Templating](#user-content-templating)
		- [Templating 1: Binding in Pystachio](#user-content-templating-1-binding-in-pystachio)
		- [Structurals in Pystachio / Aurora](#user-content-structurals-in-pystachio--aurora)
			- [Mustaches Within Structurals](#user-content-mustaches-within-structurals)
		- [Templating 2: Structurals Are Factories](#user-content-templating-2-structurals-are-factories)
			- [A Second Way of Templating](#user-content-a-second-way-of-templating)
		- [Advanced Binding](#user-content-advanced-binding)
			- [Bind Syntax](#user-content-bind-syntax)
			- [Binding Complex Objects](#user-content-binding-complex-objects)
				- [Lists](#user-content-lists)
				- [Maps](#user-content-maps)
				- [Structurals](#user-content-structurals)
		- [Structural Binding](#user-content-structural-binding)
	- [Configuration File Writing Tips And Best Practices](#user-content-configuration-file-writing-tips-and-best-practices)
		- [Use As Few .aurora Files As Possible](#user-content-use-as-few-aurora-files-as-possible)
		- [Avoid Boilerplate](#user-content-avoid-boilerplate)
		- [Thermos Uses bash, But Thermos Is Not bash](#user-content-thermos-uses-bash-but-thermos-is-not-bash)
			- [Bad](#user-content-bad)
			- [Good](#user-content-good)
		- [Rarely Use Functions In Your Configurations](#user-content-rarely-use-functions-in-your-configurations)
			- [Bad](#user-content-bad-1)
			- [Good](#user-content-good-1)

The Basics
----------

To run a job on Aurora, you must specify a configuration file that tells
Aurora what it needs to know to schedule the job, what Mesos needs to
run the tasks the job is made up of, and what Thermos needs to run the
processes that make up the tasks. This file must have
a`.aurora` suffix.

A configuration file defines a collection of objects, along with parameter
values for their attributes. An Aurora configuration file contains the
following three types of objects:

- Job
- Task
- Process

A configuration also specifies a list of `Job` objects assigned
to the variable `jobs`.

- jobs (list of defined Jobs to run)

The `.aurora` file format is just Python. However, `Job`, `Task`,
`Process`, and other classes are defined by a type-checked dictionary
templating library called *Pystachio*, a powerful tool for
configuration specification and reuse. Pystachio objects are tailored
via {{}} surrounded templates.

When writing your `.aurora` file, you may use any Pystachio datatypes, as
well as any objects shown in the [*Aurora+Thermos Configuration
Reference*](configuration-reference.md), without `import` statements - the
Aurora config loader injects them automatically. Other than that, an `.aurora`
file works like any other Python script.

[*Aurora+Thermos Configuration Reference*](configuration-reference.md)
has a full reference of all Aurora/Thermos defined Pystachio objects.

### Use Bottom-To-Top Object Ordering

A well-structured configuration starts with structural templates (if
any). Structural templates encapsulate in their attributes all the
differences between Jobs in the configuration that are not directly
manipulated at the `Job` level, but typically at the `Process` or `Task`
level. For example, if certain processes are invoked with slightly
different settings or input.

After structural templates, define, in order, `Process`es, `Task`s, and
`Job`s.

Structural template names should be *UpperCamelCased* and their
instantiations are typically *UPPER\_SNAKE\_CASED*. `Process`, `Task`,
and `Job` names are typically *lower\_snake\_cased*. Indentation is typically 2
spaces.

An Example Configuration File
-----------------------------

The following is a typical configuration file. Don't worry if there are
parts you don't understand yet, but you may want to refer back to this
as you read about its individual parts. Note that names surrounded by
curly braces {{}} are template variables, which the system replaces with
bound values for the variables.

    # --- templates here ---
	class Profile(Struct):
	  package_version = Default(String, 'live')
	  java_binary = Default(String, '/usr/lib/jvm/java-1.7.0-openjdk/bin/java')
	  extra_jvm_options = Default(String, '')
	  parent_environment = Default(String, 'prod')
	  parent_serverset = Default(String,
                                 '/foocorp/service/bird/{{parent_environment}}/bird')

	# --- processes here ---
	main = Process(
	  name = 'application',
	  cmdline = '{{profile.java_binary}} -server -Xmx1792m '
	            '{{profile.extra_jvm_options}} '
	            '-jar application.jar '
	            '-upstreamService {{profile.parent_serverset}}'
	)

	# --- tasks ---
	base_task = SequentialTask(
	  name = 'application',
	  processes = [
	    Process(
	      name = 'fetch',
	      cmdline = 'curl -O
                  https://packages.foocorp.com/{{profile.package_version}}/application.jar'),
	  ]
	)

        # not always necessary but often useful to have separate task
        # resource classes
        staging_task = base_task(resources =
                         Resources(cpu = 1.0,
                                   ram = 2048*MB,
                                   disk = 1*GB))
	production_task = base_task(resources =
                            Resources(cpu = 4.0,
                                      ram = 2560*MB,
                                      disk = 10*GB))

	# --- job template ---
	job_template = Job(
	  name = 'application',
	  role = 'myteam',
	  contact = 'myteam-team@foocorp.com',
	  instances = 20,
	  service = True,
	  task = production_task
	)

	# -- profile instantiations (if any) ---
	PRODUCTION = Profile()
	STAGING = Profile(
	  extra_jvm_options = '-Xloggc:gc.log',
	  parent_environment = 'staging'
	)

	# -- job instantiations --
	jobs = [
          job_template(cluster = 'cluster1', environment = 'prod')
	               .bind(profile = PRODUCTION),

          job_template(cluster = 'cluster2', environment = 'prod')
	                .bind(profile = PRODUCTION),

          job_template(cluster = 'cluster1',
                        environment = 'staging',
			service = False,
			task = staging_task,
			instances = 2)
			.bind(profile = STAGING),
	]

## Defining Process Objects

Processes are handled by the Thermos system. A process is a single
executable step run as a part of an Aurora task, which consists of a
bash-executable statement.

The key (and required) `Process` attributes are:

-   `name`: Any string which is a valid Unix filename (no slashes,
    NULLs, or leading periods). The `name` value must be unique relative
    to other Processes in a `Task`.
-   `cmdline`: A command line run in a bash subshell, so you can use
    bash scripts. Nothing is supplied for command-line arguments,
    so `$*` is unspecified.

Many tiny processes make managing configurations more difficult. For
example, the following is a bad way to define processes.

    copy = Process(
      name = 'copy',
      cmdline = 'curl -O https://packages.foocorp.com/app.zip'
    )
    unpack = Process(
      name = 'unpack',
      cmdline = 'unzip app.zip'
    )
    remove = Process(
      name = 'remove',
      cmdline = 'rm -f app.zip'
    )
    run = Process(
      name = 'app',
      cmdline = 'java -jar app.jar'
    )
    run_task = Task(
      processes = [copy, unpack, remove, run],
      constraints = order(copy, unpack, remove, run)
    )

Since `cmdline` runs in a bash subshell, you can chain commands
with `&&` or `||`.

When defining a `Task` that is just a list of Processes run in a
particular order, use `SequentialTask`, as described in the [*Defining*
`Task` *Objects*](#Task) section. The following simplifies and combines the
above multiple `Process` definitions into just two.

    stage = Process(
      name = 'stage',
      cmdline = 'curl -O https://packages.foocorp.com/app.zip && '
                'unzip app.zip && rm -f app.zip')

    run = Process(name = 'app', cmdline = 'java -jar app.jar')

    run_task = SequentialTask(processes = [stage, run])

`Process` also has five optional attributes, each with a default value
if one isn't specified in the configuration:

-   `max_failures`: Defaulting to `1`, the maximum number of failures
    (non-zero exit statuses) before this `Process` is marked permanently
    failed and not retried. If a `Process` permanently fails, Thermos
    checks the `Process` object's containing `Task` for the task's
    failure limit (usually 1) to determine whether or not the `Task`
    should be failed. Setting `max_failures`to `0` means that this
    process will keep retrying until a successful (zero) exit status is
    achieved. Retries happen at most once every `min_duration` seconds
    to prevent effectively mounting a denial of service attack against
    the coordinating scheduler.

-   `daemon`: Defaulting to `False`, if `daemon` is set to `True`, a
    successful (zero) exit status does not prevent future process runs.
    Instead, the `Process` reinvokes after `min_duration` seconds.
    However, the maximum failure limit (`max_failures`) still
    applies. A combination of `daemon=True` and `max_failures=0` retries
    a `Process` indefinitely regardless of exit status. This should
    generally be avoided for very short-lived processes because of the
    accumulation of checkpointed state for each process run. When
    running in Aurora, `max_failures` is capped at
    100.

-   `ephemeral`: Defaulting to `False`, if `ephemeral` is `True`, the
    `Process`' status is not used to determine if its bound `Task` has
    completed. For example, consider a `Task` with a
    non-ephemeral webserver process and an ephemeral logsaver process
    that periodically checkpoints its log files to a centralized data
    store. The `Task` is considered finished once the webserver process
    finishes, regardless of the logsaver's current status.

-   `min_duration`: Defaults to `15`. Processes may succeed or fail
    multiple times during a single Task. Each result is called a
    *process run* and this value is the minimum number of seconds the
    scheduler waits before re-running the same process.

-   `final`: Defaulting to `False`, this is a finalizing `Process` that
    should run last. Processes can be grouped into two classes:
    *ordinary* and *finalizing*. By default, Thermos Processes are
    ordinary. They run as long as the `Task` is considered
    healthy (i.e. hasn't reached a failure limit). But once all regular
    Thermos Processes have either finished or the `Task` has reached a
    certain failure threshold, Thermos moves into a *finalization* stage
    and runs all finalizing Processes. These are typically necessary for
    cleaning up after the `Task`, such as log checkpointers, or perhaps
    e-mail notifications of a completed Task. Finalizing processes may
    not depend upon ordinary processes or vice-versa, however finalizing
    processes may depend upon other finalizing processes and will
    otherwise run as a typical process schedule.

## Getting Your Code Into The Sandbox

When using Aurora, you need to get your executable code into its "sandbox", specifically
the Task sandbox where the code executes for the Processes that make up that Task.

Each Task has a sandbox created when the Task starts and garbage
collected when it finishes. All of a Task's processes run in its
sandbox, so processes can share state by using a shared current
working directory.

Typically, you save this code somewhere. You then need to define a Process
in your `.aurora` configuration file that fetches the code from that somewhere
to where the slave can see it. For a public cloud, that can be anywhere public on
the Internet, such as S3. For a private cloud internal storage, you need to put in
on an accessible HDFS cluster or similar storage.

The template for this Process is:

    <name> = Process(
      name = '<name>'
      cmdline = '<command to copy and extract code archive into current working directory>'
    )

Note: Be sure the extracted code archive has an executable.

## Defining Task Objects

Tasks are handled by Mesos. A task is a collection of processes that
runs in a shared sandbox. It's the fundamental unit Aurora uses to
schedule the datacenter; essentially what Aurora does is find places
in the cluster to run tasks.

The key (and required) parts of a Task are:

-   `name`: A string giving the Task's name. By default, if a Task is
    not given a name, it inherits the first name in its Process list.

-   `processes`: An unordered list of Process objects bound to the Task.
    The value of the optional `constraints` attribute affects the
    contents as a whole. Currently, the only constraint, `order`, determines if
    the processes run in parallel or sequentially.

-   `resources`: A `Resource` object defining the Task's resource
        footprint. A `Resource` object has three attributes:
        -   `cpu`: A Float, the fractional number of cores the Task
        requires.
        -   `ram`: An Integer, RAM bytes the Task requires.
        -   `disk`: An integer, disk bytes the Task requires.

A basic Task definition looks like:

    Task(
        name="hello_world",
        processes=[Process(name = "hello_world", cmdline = "echo hello world")],
        resources=Resources(cpu = 1.0,
                            ram = 1*GB,
                            disk = 1*GB))

There are four optional Task attributes:

-   `constraints`: A list of `Constraint` objects that constrain the
    Task's processes. Currently there is only one type, the `order`
    constraint. For example the following requires that the processes
    run in the order `foo`, then `bar`.

        constraints = [Constraint(order=['foo', 'bar'])]

    There is an `order()` function that takes `order('foo', 'bar', 'baz')`
    and converts it into `[Constraint(order=['foo', 'bar', 'baz'])]`.
    `order()` accepts Process name strings `('foo', 'bar')` or the processes
    themselves, e.g. `foo=Process(name='foo', ...)`, `bar=Process(name='bar', ...)`,
    `constraints=order(foo, bar)`

    Note that Thermos rejects tasks with process cycles.

-   `max_failures`: Defaulting to `1`, the number of failed processes
    needed for the `Task` to be marked as failed. Note how this
    interacts with individual Processes' `max_failures` values. Assume a
    Task has two Processes and a `max_failures` value of `2`. So both
    Processes must fail for the Task to fail. Now, assume each of the
    Task's Processes has its own `max_failures` value of `10`. If
    Process "A" fails 5 times before succeeding, and Process "B" fails
    10 times and is then marked as failing, their parent Task succeeds.
    Even though there were 15 individual failures by its Processes, only
    1 of its Processes was finally marked as failing. Since 1 is less
    than the 2 that is the Task's `max_failures` value, the Task does
    not fail.

-   `max_concurrency`: Defaulting to `0`, the maximum number of
    concurrent processes in the Task. `0` specifies unlimited
    concurrency. For Tasks with many expensive but otherwise independent
    processes, you can limit the amount of concurrency Thermos schedules
    instead of artificially constraining them through `order`
    constraints. For example, a test framework may generate a Task with
    100 test run processes, but runs it in a Task with
    `resources.cpus=4`. Limit the amount of parallelism to 4 by setting
    `max_concurrency=4`.

-   `finalization_wait`: Defaulting to `30`, the number of seconds
    allocated for finalizing the Task's processes. A Task starts in
    `ACTIVE` state when Processes run and stays there as long as the Task
    is healthy and Processes run. When all Processes finish successfully
    or the Task reaches its maximum process failure limit, it goes into
    `CLEANING` state. In `CLEANING`, it sends `SIGTERMS` to any still running
    Processes. When all Processes terminate, the Task goes into
    `FINALIZING` state and invokes the schedule of all processes whose
    final attribute has a True value. Everything from the end of `ACTIVE`
    to the end of `FINALIZING` must happen within `finalization_wait`
    number of seconds. If not, all still running Processes are sent
    `SIGKILL`s (or if dependent on yet to be completed Processes, are
    never invoked).

### SequentialTask: Running Processes in Parallel or Sequentially

By default, a Task with several Processes runs them in parallel. There
are two ways to run Processes sequentially:

-   Include an `order` constraint in the Task definition's `constraints`
    attribute whose arguments specify the processes' run order:

        Task( ... processes=[process1, process2, process3],
	          constraints = order(process1, process2, process3), ...)

-   Use `SequentialTask` instead of `Task`; it automatically runs
    processes in the order specified in the `processes` attribute. No
    `constraint` parameter is needed:

        SequentialTask( ... processes=[process1, process2, process3] ...)

### SimpleTask

For quickly creating simple tasks, use the `SimpleTask` helper. It
creates a basic task from a provided name and command line using a
default set of resources. For example, in a .`aurora` configuration
file:

    SimpleTask(name="hello_world", command="echo hello world")

is equivalent to

    Task(name="hello_world",
         processes=[Process(name = "hello_world", cmdline = "echo hello world")],
         resources=Resources(cpu = 1.0,
                             ram = 1*GB,
                             disk = 1*GB))

The simplest idiomatic Job configuration thus becomes:

    import os
    hello_world_job = Job(
      task=SimpleTask(name="hello_world", command="echo hello world"),
      role=os.getenv('USER'),
      cluster="cluster1")

When written to `hello_world.aurora`, you invoke it with a simple
`aurora job create cluster1/$USER/test/hello_world hello_world.aurora`.

### Combining tasks

`Tasks.concat`(synonym,`concat_tasks`) and
`Tasks.combine`(synonym,`combine_tasks`) merge multiple Task definitions
into a single Task. It may be easier to define complex Jobs
as smaller constituent Tasks. But since a Job only includes a single
Task, the subtasks must be combined before using them in a Job.
Smaller Tasks can also be reused between Jobs, instead of having to
repeat their definition for multiple Jobs.

With both methods, the merged Task takes the first Task's name. The
difference between the two is the result Task's process ordering.

-   `Tasks.combine` runs its subtasks' processes in no particular order.
    The new Task's resource consumption is the sum of all its subtasks'
    consumption.

-   `Tasks.concat` runs its subtasks in the order supplied, with each
    subtask's processes run serially between tasks. It is analogous to
    the `order` constraint helper, except at the Task level instead of
    the Process level. The new Task's resource consumption is the
    maximum value specified by any subtask for each Resource attribute
    (cpu, ram and disk).

For example, given the following:

    setup_task = Task(
      ...
      processes=[download_interpreter, update_zookeeper],
      # It is important to note that {{Tasks.concat}} has
      # no effect on the ordering of the processes within a task;
      # hence the necessity of the {{order}} statement below
      # (otherwise, the order in which {{download_interpreter}}
      # and {{update_zookeeper}} run will be non-deterministic)
      constraints=order(download_interpreter, update_zookeeper),
      ...
    )

    run_task = SequentialTask(
      ...
      processes=[download_application, start_application],
      ...
    )

    combined_task = Tasks.concat(setup_task, run_task)

The `Tasks.concat` command merges the two Tasks into a single Task and
ensures all processes in `setup_task` run before the processes
in `run_task`. Conceptually, the task is reduced to:

    task = Task(
      ...
      processes=[download_interpreter, update_zookeeper,
                 download_application, start_application],
      constraints=order(download_interpreter, update_zookeeper,
                        download_application, start_application),
      ...
    )

In the case of `Tasks.combine`, the two schedules run in parallel:

    task = Task(
      ...
      processes=[download_interpreter, update_zookeeper,
                 download_application, start_application],
      constraints=order(download_interpreter, update_zookeeper) +
                        order(download_application, start_application),
      ...
    )

In the latter case, each of the two sequences may operate in parallel.
Of course, this may not be the intended behavior (for example, if
the `start_application` Process implicitly relies
upon `download_interpreter`). Make sure you understand the difference
between using one or the other.

## Defining Job Objects

A job is a group of identical tasks that Aurora can run in a Mesos cluster.

A `Job` object is defined by the values of several attributes, some
required and some optional. The required attributes are:

-   `task`: Task object to bind to this job. Note that a Job can
    only take a single Task.

-   `role`: Job's role account; in other words, the user account to run
    the job as on a Mesos cluster machine. A common value is
    `os.getenv('USER')`; using a Python command to get the user who
    submits the job request. The other common value is the service
    account that runs the job, e.g. `www-data`.

-   `environment`: Job's environment, typical values
    are `devel`, `test`, or `prod`.

-   `cluster`: Aurora cluster to schedule the job in, defined in
    `/etc/aurora/clusters.json` or `~/.clusters.json`. You can specify
    jobs where the only difference is the `cluster`, then at run time
    only run the Job whose job key includes your desired cluster's name.

You usually see a `name` parameter. By default, `name` inherits its
value from the Job's associated Task object, but you can override this
default. For these four parameters, a Job definition might look like:

    foo_job = Job( name = 'foo', cluster = 'cluster1',
              role = os.getenv('USER'), environment = 'prod',
              task = foo_task)

In addition to the required attributes, there are several optional
attributes. The first (strongly recommended) optional attribute is:

-   `contact`: An email address for the Job's owner. For production
    jobs, it is usually a team mailing list.

Two more attributes deal with how to handle failure of the Job's Task:

-   `max_task_failures`: An integer, defaulting to `1`, of the maximum
    number of Task failures after which the Job is considered failed.
    `-1` allows for infinite failures.

-   `service`: A boolean, defaulting to `False`, which if `True`
    restarts tasks regardless of whether they succeeded or failed. In
    other words, if `True`, after the Job's Task completes, it
    automatically starts again. This is for Jobs you want to run
    continuously, rather than doing a single run.

Three attributes deal with configuring the Job's Task:

-   `instances`: Defaulting to `1`, the number of
    instances/replicas/shards of the Job's Task to create.

-   `priority`: Defaulting to `0`, the Job's Task's preemption priority,
    for which higher values may preempt Tasks from Jobs with lower
    values.

-   `production`: a Boolean, defaulting to `False`, specifying that this
    is a [production](configuration-reference.md#job-objects) job.

The final three Job attributes each take an object as their value.

-   `update_config`: An `UpdateConfig`
    object provides parameters for controlling the rate and policy of
    rolling updates. The `UpdateConfig` parameters are:
    -   `batch_size`: An integer, defaulting to `1`, specifying the
        maximum number of shards to update in one iteration.
    -   `restart_threshold`: An integer, defaulting to `60`, specifying
        the maximum number of seconds before a shard must move into the
        `RUNNING` state before considered a failure.
    -   `watch_secs`: An integer, defaulting to `45`, specifying the
        minimum number of seconds a shard must remain in the `RUNNING`
        state before considered a success.
    -   `max_per_shard_failures`: An integer, defaulting to `0`,
        specifying the maximum number of restarts per shard during an
        update. When the limit is exceeded, it increments the total
        failure count.
    -   `max_total_failures`: An integer, defaulting to `0`, specifying
        the maximum number of shard failures tolerated during an update.
        Cannot be equal to or greater than the job's total number of
        tasks.
-   `health_check_config`: A `HealthCheckConfig` object that provides
    parameters for controlling a Task's health checks via HTTP. Only
    used if a health port was assigned with a command line wildcard. The
    `HealthCheckConfig` parameters are:
    -   `initial_interval_secs`: An integer, defaulting to `15`,
        specifying the initial delay for doing an HTTP health check.
    -   `interval_secs`: An integer, defaulting to `10`, specifying the
        number of seconds in the interval between checking the Task's
        health.
    -   `timeout_secs`: An integer, defaulting to `1`, specifying the
        number of seconds the application must respond to an HTTP health
        check with `OK` before it is considered a failure.
    -   `max_consecutive_failures`: An integer, defaulting to `0`,
        specifying the maximum number of consecutive failures before a
        task is unhealthy.
-   `constraints`: A `dict` Python object, specifying Task scheduling
    constraints. Most users will not need to specify constraints, as the
    scheduler automatically inserts reasonable defaults. Please do not
    set this field unless you are sure of what you are doing. See the
    section in the Aurora + Thermos Reference manual on [Specifying
    Scheduling Constraints](configuration-reference.md) for more information.

## The jobs List

At the end of your `.aurora` file, you need to specify a list of the
file's defined Jobs to run in the order listed. For example, the
following runs first `job1`, then `job2`, then `job3`.

jobs = [job1, job2, job3]

Templating
----------

The `.aurora` file format is just Python. However, `Job`, `Task`,
`Process`, and other classes are defined by a templating library called
*Pystachio*, a powerful tool for configuration specification and reuse.

[Aurora+Thermos Configuration Reference](configuration-reference.md)
has a full reference of all Aurora/Thermos defined Pystachio objects.

When writing your `.aurora` file, you may use any Pystachio datatypes, as
well as any objects shown in the *Aurora+Thermos Configuration
Reference* without `import` statements - the Aurora config loader
injects them automatically. Other than that the `.aurora` format
works like any other Python script.

### Templating 1: Binding in Pystachio

Pystachio uses the visually distinctive {{}} to indicate template
variables. These are often called "mustache variables" after the
similarly appearing variables in the Mustache templating system and
because the curly braces resemble mustaches.

If you are familiar with the Mustache system, templates in Pystachio
have significant differences. They have no nesting, joining, or
inheritance semantics. On the other hand, when evaluated, templates
are evaluated iteratively, so this affords some level of indirection.

Let's start with the simplest template; text with one
variable, in this case `name`;

    Hello {{name}}

If we evaluate this as is, we'd get back:

    Hello

If a template variable doesn't have a value, when evaluated it's
replaced with nothing. If we add a binding to give it a value:

    { "name" : "Tom" }

We'd get back:

    Hello Tom

Every Pystachio object has an associated `.bind` method that can bind
values to {{}} variables. Bindings are not immediately evaluated.
Instead, they are evaluated only when the interpolated value of the
object is necessary, e.g. for performing equality or serializing a
message over the wire.

Objects with and without mustache templated variables behave
differently:

    >>> Float(1.5)
    Float(1.5)

    >>> Float('{{x}}.5')
    Float({{x}}.5)

    >>> Float('{{x}}.5').bind(x = 1)
    Float(1.5)

    >>> Float('{{x}}.5').bind(x = 1) == Float(1.5)
    True

    >>> contextual_object = String('{{metavar{{number}}}}').bind(
    ... metavar1 = "first", metavar2 = "second")

    >>> contextual_object
    String({{metavar{{number}}}})

    >>> contextual_object.bind(number = 1)
    String(first)

    >>> contextual_object.bind(number = 2)
    String(second)

You usually bind simple key to value pairs, but you can also bind three
other objects: lists, dictionaries, and structurals. These will be
described in detail later.

### Structurals in Pystachio / Aurora

Most Aurora/Thermos users don't ever (knowingly) interact with `String`,
`Float`, or `Integer` Pystashio objects directly. Instead they interact
with derived structural (`Struct`) objects that are collections of
fundamental and structural objects. The structural object components are
called *attributes*. Aurora's most used structural objects are `Job`,
`Task`, and `Process`:

    class Process(Struct):
      cmdline = Required(String)
      name = Required(String)
      max_failures = Default(Integer, 1)
      daemon = Default(Boolean, False)
      ephemeral = Default(Boolean, False)
      min_duration = Default(Integer, 5)
      final = Default(Boolean, False)

Construct default objects by following the object's type with (). If you
want an attribute to have a value different from its default, include
the attribute name and value inside the parentheses.

    >>> Process()
    Process(daemon=False, max_failures=1, ephemeral=False,
      min_duration=5, final=False)

Attribute values can be template variables, which then receive specific
values when creating the object.

    >>> Process(cmdline = 'echo {{message}}')
    Process(daemon=False, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo {{message}}, final=False)

    >>> Process(cmdline = 'echo {{message}}').bind(message = 'hello world')
    Process(daemon=False, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo hello world, final=False)

A powerful binding property is that all of an object's children inherit its
bindings:

    >>> List(Process)([
    ... Process(name = '{{prefix}}_one'),
    ... Process(name = '{{prefix}}_two')
    ... ]).bind(prefix = 'hello')
    ProcessList(
      Process(daemon=False, name=hello_one, max_failures=1, ephemeral=False, min_duration=5, final=False),
      Process(daemon=False, name=hello_two, max_failures=1, ephemeral=False, min_duration=5, final=False)
      )

Remember that an Aurora Job contains Tasks which contain Processes. A
Job level binding is inherited by its Tasks and all their Processes.
Similarly a Task level binding is available to that Task and its
Processes but is *not* visible at the Job level (inheritance is a
one-way street.)

#### Mustaches Within Structurals

When you define a `Struct` schema, one powerful, but confusing, feature
is that all of that structure's attributes are Mustache variables within
the enclosing scope *once they have been populated*.

For example, when `Process` is defined above, all its attributes such as
{{`name`}}, {{`cmdline`}}, {{`max_failures`}} etc., are all immediately
defined as Mustache variables, implicitly bound into the `Process`, and
inherit all child objects once they are defined.

Thus, you can do the following:

    >>> Process(name = "installer", cmdline = "echo {{name}} is running")
    Process(daemon=False, name=installer, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo installer is running, final=False)

WARNING: This binding only takes place in one direction. For example,
the following does NOT work and does not set the `Process` `name`
attribute's value.

    >>> Process().bind(name = "installer")
    Process(daemon=False, max_failures=1, ephemeral=False, min_duration=5, final=False)

The following is also not possible and results in an infinite loop that
attempts to resolve `Process.name`.

    >>> Process(name = '{{name}}').bind(name = 'installer')

Do not confuse Structural attributes with bound Mustache variables.
Attributes are implicitly converted to Mustache variables but not vice
versa.

### Templating 2: Structurals Are Factories

#### A Second Way of Templating

A second templating method is both as powerful as the aforementioned and
often confused with it. This method is due to automatic conversion of
Struct attributes to Mustache variables as described above.

Suppose you create a Process object:

    >>> p = Process(name = "process_one", cmdline = "echo hello world")

    >>> p
    Process(daemon=False, name=process_one, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo hello world, final=False)

This `Process` object, "`p`", can be used wherever a `Process` object is
needed. It can also be reused by changing the value(s) of its
attribute(s). Here we change its `name` attribute from `process_one` to
`process_two`.

    >>> p(name = "process_two")
    Process(daemon=False, name=process_two, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo hello world, final=False)

Template creation is a common use for this technique:

    >>> Daemon = Process(daemon = True)
    >>> logrotate = Daemon(name = 'logrotate', cmdline = './logrotate conf/logrotate.conf')
    >>> mysql = Daemon(name = 'mysql', cmdline = 'bin/mysqld --safe-mode')

### Advanced Binding

As described above, `.bind()` binds simple strings or numbers to
Mustache variables. In addition to Structural types formed by combining
atomic types, Pystachio has two container types; `List` and `Map` which
can also be bound via `.bind()`.

#### Bind Syntax

The `bind()` function can take Python dictionaries or `kwargs`
interchangeably (when "`kwargs`" is in a function definition, `kwargs`
receives a Python dictionary containing all keyword arguments after the
formal parameter list).

    >>> String('{{foo}}').bind(foo = 'bar') == String('{{foo}}').bind({'foo': 'bar'})
    True

Bindings done "closer" to the object in question take precedence:

    >>> p = Process(name = '{{context}}_process')
    >>> t = Task().bind(context = 'global')
    >>> t(processes = [p, p.bind(context = 'local')])
    Task(processes=ProcessList(
      Process(daemon=False, name=global_process, max_failures=1, ephemeral=False, final=False,
              min_duration=5),
      Process(daemon=False, name=local_process, max_failures=1, ephemeral=False, final=False,
              min_duration=5)
    ))

#### Binding Complex Objects

##### Lists

    >>> fibonacci = List(Integer)([1, 1, 2, 3, 5, 8, 13])
    >>> String('{{fib[4]}}').bind(fib = fibonacci)
    String(5)

##### Maps

    >>> first_names = Map(String, String)({'Kent': 'Clark', 'Wayne': 'Bruce', 'Prince': 'Diana'})
    >>> String('{{first[Kent]}}').bind(first = first_names)
    String(Clark)

##### Structurals

    >>> String('{{p.cmdline}}').bind(p = Process(cmdline = "echo hello world"))
    String(echo hello world)

### Structural Binding

Use structural templates when binding more than two or three individual
values at the Job or Task level. For fewer than two or three, standard
key to string binding is sufficient.

Structural binding is a very powerful pattern and is most useful in
Aurora/Thermos for doing Structural configuration. For example, you can
define a job profile. The following profile uses `HDFS`, the Hadoop
Distributed File System, to designate a file's location. `HDFS` does
not come with Aurora, so you'll need to either install it separately
or change the way the dataset is designated.

    class Profile(Struct):
      version = Required(String)
      environment = Required(String)
      dataset = Default(String, hdfs://home/aurora/data/{{environment}}')

    PRODUCTION = Profile(version = 'live', environment = 'prod')
    DEVEL = Profile(version = 'latest',
                    environment = 'devel',
                    dataset = 'hdfs://home/aurora/data/test')
    TEST = Profile(version = 'latest', environment = 'test')

    JOB_TEMPLATE = Job(
      name = 'application',
      role = 'myteam',
      cluster = 'cluster1',
      environment = '{{profile.environment}}',
      task = SequentialTask(
        name = 'task',
        resources = Resources(cpu = 2, ram = 4*GB, disk = 8*GB),
        processes = [
	  Process(name = 'main', cmdline = 'java -jar application.jar -hdfsPath
                 {{profile.dataset}}')
        ]
       )
     )

    jobs = [
      JOB_TEMPLATE(instances = 100).bind(profile = PRODUCTION),
      JOB_TEMPLATE.bind(profile = DEVEL),
      JOB_TEMPLATE.bind(profile = TEST),
     ]

In this case, a custom structural "Profile" is created to self-document
the configuration to some degree. This also allows some schema
"type-checking", and for default self-substitution, e.g. in
`Profile.dataset` above.

So rather than a `.bind()` with a half-dozen substituted variables, you
can bind a single object that has sensible defaults stored in a single
place.

Configuration File Writing Tips And Best Practices
--------------------------------------------------

### Use As Few .aurora Files As Possible

When creating your `.aurora` configuration, try to keep all versions of
a particular job within the same `.aurora` file. For example, if you
have separate jobs for `cluster1`, `cluster1` staging, `cluster1`
testing, and`cluster2`, keep them as close together as possible.

Constructs shared across multiple jobs owned by your team (e.g.
team-level defaults or structural templates) can be split into separate
`.aurora`files and included via the `include` directive.

### Avoid Boilerplate

If you see repetition or find yourself copy and pasting any parts of
your configuration, it's likely an opportunity for templating. Take the
example below:

`redundant.aurora` contains:

    download = Process(
      name = 'download',
      cmdline = 'wget http://www.python.org/ftp/python/2.7.3/Python-2.7.3.tar.bz2',
      max_failures = 5,
      min_duration = 1)

    unpack = Process(
      name = 'unpack',
      cmdline = 'rm -rf Python-2.7.3 && tar xzf Python-2.7.3.tar.bz2',
      max_failures = 5,
      min_duration = 1)

    build = Process(
      name = 'build',
      cmdline = 'pushd Python-2.7.3 && ./configure && make && popd',
      max_failures = 1)

    email = Process(
      name = 'email',
      cmdline = 'echo Success | mail feynman@tmc.com',
      max_failures = 5,
      min_duration = 1)

    build_python = Task(
      name = 'build_python',
      processes = [download, unpack, build, email],
      constraints = [Constraint(order = ['download', 'unpack', 'build', 'email'])])

As you'll notice, there's a lot of repetition in the `Process`
definitions. For example, almost every process sets a `max_failures`
limit to 5 and a `min_duration` to 1. This is an opportunity for factoring
into a common process template.

Furthermore, the Python version is repeated everywhere. This can be
bound via structural templating as described in the [Advanced Binding](#AdvancedBinding)
section.

`less_redundant.aurora` contains:

    class Python(Struct):
      version = Required(String)
      base = Default(String, 'Python-{{version}}')
      package = Default(String, '{{base}}.tar.bz2')

    ReliableProcess = Process(
      max_failures = 5,
      min_duration = 1)

    download = ReliableProcess(
      name = 'download',
      cmdline = 'wget http://www.python.org/ftp/python/{{python.version}}/{{python.package}}')

    unpack = ReliableProcess(
      name = 'unpack',
      cmdline = 'rm -rf {{python.base}} && tar xzf {{python.package}}')

    build = ReliableProcess(
      name = 'build',
      cmdline = 'pushd {{python.base}} && ./configure && make && popd',
      max_failures = 1)

    email = ReliableProcess(
      name = 'email',
      cmdline = 'echo Success | mail {{role}}@foocorp.com')

    build_python = SequentialTask(
      name = 'build_python',
      processes = [download, unpack, build, email]).bind(python = Python(version = "2.7.3"))

### Thermos Uses bash, But Thermos Is Not bash

#### Bad

Many tiny Processes makes for harder to manage configurations.

    copy = Process(
      name = 'copy',
      cmdline = 'rcp user@my_machine:my_application .'
     )

     unpack = Process(
       name = 'unpack',
       cmdline = 'unzip app.zip'
     )

     remove = Process(
       name = 'remove',
       cmdline = 'rm -f app.zip'
     )

     run = Process(
       name = 'app',
       cmdline = 'java -jar app.jar'
     )

     run_task = Task(
       processes = [copy, unpack, remove, run],
       constraints = order(copy, unpack, remove, run)
     )

#### Good

Each `cmdline` runs in a bash subshell, so you have the full power of
bash. Chaining commands with `&&` or `||` is almost always the right
thing to do.

Also for Tasks that are simply a list of processes that run one after
another, consider using the `SequentialTask` helper which applies a
linear ordering constraint for you.

    stage = Process(
      name = 'stage',
      cmdline = 'rcp user@my_machine:my_application . && unzip app.zip && rm -f app.zip')

    run = Process(name = 'app', cmdline = 'java -jar app.jar')

    run_task = SequentialTask(processes = [stage, run])

### Rarely Use Functions In Your Configurations

90% of the time you define a function in a `.aurora` file, you're
probably Doing It Wrong(TM).

#### Bad

    def get_my_task(name, user, cpu, ram, disk):
      return Task(
        name = name,
        user = user,
        processes = [STAGE_PROCESS, RUN_PROCESS],
        constraints = order(STAGE_PROCESS, RUN_PROCESS),
        resources = Resources(cpu = cpu, ram = ram, disk = disk)
     )

     task_one = get_my_task('task_one', 'feynman', 1.0, 32*MB, 1*GB)
     task_two = get_my_task('task_two', 'feynman', 2.0, 64*MB, 1*GB)

#### Good

This one is more idiomatic. Forced keyword arguments prevents accidents,
e.g. constructing a task with "32*MB" when you mean 32MB of ram and not
disk. Less proliferation of task-construction techniques means
easier-to-read, quicker-to-understand, and a more composable
configuration.

    TASK_TEMPLATE = SequentialTask(
      user = 'wickman',
      processes = [STAGE_PROCESS, RUN_PROCESS],
    )

    task_one = TASK_TEMPLATE(
      name = 'task_one',
      resources = Resources(cpu = 1.0, ram = 32*MB, disk = 1*GB) )

    task_two = TASK_TEMPLATE(
      name = 'task_two',
      resources = Resources(cpu = 2.0, ram = 64*MB, disk = 1*GB)
    )
