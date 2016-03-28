Aurora Configuration Tutorial
=============================

How to write Aurora configuration files, including feature descriptions
and best practices. When writing a configuration file, make use of
`aurora job inspect`. It takes the same job key and configuration file
arguments as `aurora job create` or `aurora update start`. It first ensures the
configuration parses, then outputs it in human-readable form.

You should read this after going through the general [Aurora Tutorial](../getting-started/tutorial.md).

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
- [Basic Examples](#basic-examples)


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

[*Aurora Configuration Reference*](configuration.md)
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

`Process` also has optional attributes to customize its behaviour. Details can be found in the [Aurora Configuration Reference](configuration.md#process-objects).


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

A Task has optional attributes to customize its behaviour. Details can be found in the [Aurora Configuration Reference](configuration.md#task-object)


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
attributes. Details can be found in the [Aurora Configuration Reference](configuration.md#job-objects).


## The jobs List

At the end of your `.aurora` file, you need to specify a list of the
file's defined Jobs. For example, the following exports the jobs `job1`,
`job2`, and `job3`.

    jobs = [job1, job2, job3]

This allows the aurora client to invoke commands on those jobs, such as
starting, updating, or killing them.



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

    aurora job create cluster1/$USER/test/hello_world hello_world.aurora

    aurora job kill cluster1/$USER/test/hello_world

### Environment Tailoring

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

    aurora job create cluster1/$USER/test/hello_world-cluster1 hello_world_productionized.aurora

    aurora job kill cluster1/$USER/test/hello_world-cluster1
