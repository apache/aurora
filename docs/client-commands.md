Aurora Client Commands
======================

The most up-to-date reference is in the client itself: use the
`aurora help` subcommand (for example, `aurora help` or
`aurora help create`) to find the latest information on parameters and
functionality. Note that `aurora help open` does not work, due to underlying issues with
reflection.

- [Introduction](#introduction)
- [Cluster Configuration](#cluster-configuration)
- [Job Keys](#job-keys)
- [Modifying Aurora Client Commands](#modifying-aurora-client-commands)
- [Regular Jobs](#regular-jobs)
    - [Creating and Running a Job](#creating-and-running-a-job)
    - [Running a Command On a Running Job](#running-a-command-on-a-running-job)
    - [Killing a Job](#killing-a-job)
    - [Updating a Job](#updating-a-job)
    - [Renaming a Job](#renaming-a-job)
    - [Restarting Jobs](#restarting-jobs)
- [Cron Jobs](#cron-jobs)
- [Comparing Jobs](#comparing-jobs)
- [Viewing/Examining Jobs](#viewingexamining-jobs)
    - [Listing Jobs](#listing-jobs)
    - [Inspecting a Job](#inspecting-a-job)
    - [Versions](#versions)
    - [Checking Your Quota](#checking-your-quota)
    - [Finding a Job on Web UI](#finding-a-job-on-web-ui)
    - [Getting Job Status](#getting-job-status)
    - [Opening the Web UI](#opening-the-web-ui)
    - [SSHing to a Specific Task Machine](#sshing-to-a-specific-task-machine)
    - [Templating Command Arguments](#templating-command-arguments)

Introduction
------------

Once you have written an `.aurora` configuration file that describes
your Job and its parameters and functionality, you interact with Aurora
using Aurora Client commands. This document describes all of these commands
and how and when to use them. All Aurora Client commands start with
`aurora`, followed by the name of the specific command and its
arguments.

*Job keys* are a very common argument to Aurora commands, as well as the
gateway to useful information about a Job. Before using Aurora, you
should read the next section which describes them in detail. The section
after that briefly describes how you can modify the behavior of certain
Aurora Client commands, linking to a detailed document about how to do
that.

This is followed by the Regular Jobs section, which describes the basic
Client commands for creating, running, and manipulating Aurora Jobs.
After that are sections on Comparing Jobs and Viewing/Examining Jobs. In
other words, various commands for getting information and metadata about
Aurora Jobs.

Cluster Configuration
---------------------

The client must be able to find a configuration file that specifies available clusters. This file
declares shorthand names for clusters, which are in turn referenced by job configuration files
and client commands.

The client will load at most two configuration files, making both of their defined clusters
available. The first is intended to be a system-installed cluster, using the path specified in
the environment variable `AURORA_CONFIG_ROOT`, defaulting to `/etc/aurora/clusters.json` if the
environment variable is not set. The second is a user-installed file, located at
`~/.aurora/clusters.json`.

A cluster configuration is formatted as JSON.  The simplest cluster configuration is one that
communicates with a single (non-leader-elected) scheduler.  For example:

```javascript
[{
  "name": "example",
  "scheduler_uri": "localhost:55555",
}]
```

A configuration for a leader-elected scheduler would contain something like:

```javascript
[{
  "name": "example",
  "zk": "192.168.33.7",
  "scheduler_zk_path": "/aurora/scheduler"
}]
```

For more details on cluster configuration see the
[Client Cluster Configuration](client-cluster-configuration.md) documentation.

Job Keys
--------

A job key is a unique system-wide identifier for an Aurora-managed
Job, for example `cluster1/web-team/test/experiment204`. It is a 4-tuple
consisting of, in order, *cluster*, *role*, *environment*, and
*jobname*, separated by /s. Cluster is the name of an Aurora
cluster. Role is the Unix service account under which the Job
runs. Environment is a namespace component like `devel`, `test`,
`prod`, or `stagingN.` Jobname is the Job's name.

The combination of all four values uniquely specifies the Job. If any
one value is different from that of another job key, the two job keys
refer to different Jobs. For example, job key
`cluster1/tyg/prod/workhorse` is different from
`cluster1/tyg/prod/workcamel` is different from
`cluster2/tyg/prod/workhorse` is different from
`cluster2/foo/prod/workhorse` is different from
`cluster1/tyg/test/workhorse.`

Role names are user accounts existing on the slave machines. If you don't know what accounts
are available, contact your sysadmin.

Environment names are namespaces; you can count on `prod`, `devel` and `test` existing.

Modifying Aurora Client Commands
--------------------------------

For certain Aurora Client commands, you can define hook methods that run
either before or after an action that takes place during the command's
execution, as well as based on whether the action finished successfully or failed
during execution. Basically, a hook is code that lets you extend the
command's actions. The hook executes on the client side, specifically on
the machine executing Aurora commands.

Hooks can be associated with these Aurora Client commands.

  - `cancel_update`
  - `create`
  - `kill`
  - `restart`
  - `update`

The process for writing and activating them is complex enough
that we explain it in a devoted document, [Hooks for Aurora Client API](hooks.md).

Regular Jobs
------------

This section covers Aurora commands related to running, killing,
renaming, updating, and restarting a basic Aurora Job.

### Creating and Running a Job

    aurora create <job key> <configuration file>

Creates and then runs a Job with the specified job key based on a `.aurora` configuration file.
The configuration file may also contain and activate hook definitions.

`create` can take four named parameters:

- `-E NAME=VALUE` Bind a Thermos mustache variable name to a
  value. Multiple flags specify multiple values. Defaults to `[]`.
- ` -o, --open_browser` Open a browser window to the scheduler UI Job
  page after a job changing operation happens. When `False`, the Job
  URL prints on the console and the user has to copy/paste it
  manually. Defaults to `False`. Does not work when running in Vagrant.
- ` -j, --json` If specified, configuration argument is read as a
  string in JSON format. Defaults to False.
- ` --wait_until=STATE` Block the client until all the Tasks have
  transitioned into the requested state. Possible values are: `PENDING`,
  `RUNNING`, `FINISHED`. Default: `PENDING`

### Running a Command On a Running Job

    aurora run <job_key> <cmd>

Runs a shell command on all machines currently hosting shards of a
single Job.

`run` supports the same command line wildcards used to populate a Job's
commands; i.e. anything in the `{{mesos.*}}` and `{{thermos.*}}`
namespaces.

`run` can take three named parameters:

- `-t NUM_THREADS`, `--threads=NUM_THREADS `The number of threads to
  use, defaulting to `1`.
- `--user=SSH_USER` ssh as this user instead of the given role value.
  Defaults to None.
- `-e, --executor_sandbox`  Run the command in the executor sandbox
  instead of the Task sandbox. Defaults to False.

### Killing a Job

    aurora kill <job key> <configuration file>

Kills all Tasks associated with the specified Job, blocking until all
are terminated. Defaults to killing all shards in the Job.

The `<configuration file>` argument for `kill` is optional. Use it only
if it contains hook definitions and activations that affect the
kill command.

`kill` can take two named parameters:

- `-o, --open_browser` Open a browser window to the scheduler UI Job
  page after a job changing operation happens. When `False`, the Job
  URL prints on the console and the user has to copy/paste it
  manually. Defaults to `False`. Does not work when running in Vagrant.
- `--shards=SHARDS` A list of shard ids to act on. Can either be a
  comma-separated list (e.g. 0,1,2) or a range (e.g. 0-2) or  any
  combination of the two (e.g. 0-2,5,7-9). Defaults to acting on all
  shards.

### Updating a Job

    aurora update [--shards=ids] <job key> <configuration file>
    aurora cancel_update <job key> <configuration file>

Given a running job, does a rolling update to reflect a new
configuration version. Only updates Tasks in the Job with a changed
configuration. You can further restrict the operated on Tasks by
using `--shards` and specifying a comma-separated list of job shard ids.

You may want to run `aurora diff` beforehand to validate which Tasks
have different configurations.

Updating jobs are locked to be sure the update finishes without
disruption. If the update abnormally terminates, the lock may stay
around and cause failure of subsequent update attempts.
 `aurora cancel_update `unlocks the Job specified by
its `job_key` argument. Be sure you don't issue `cancel_update` when
another user is working with the specified Job.

The `<configuration file>` argument for `cancel_update` is optional. Use
it only if it contains hook definitions and activations that affect the
`cancel_update` command. The `<configuration file>` argument for
`update` is required, but in addition to a new configuration it can be
used to define and activate hooks for `update`.

`update` can take four named parameters:

- `--shards=SHARDS` A list of shard ids to update. Can either be a
  comma-separated list (e.g. 0,1,2) or a range (e.g. 0-2) or  any
  combination of the two (e.g. 0-2,5,7-9). If not  set, all shards are
  acted on. Defaults to None.
- `-E NAME=VALUE` Binds a Thermos mustache variable name to a value.
  Use multiple flags to specify multiple values. Defaults to `[]`.
- `-j, --json` If specified, configuration is read in JSON format.
  Defaults to `False`.
- `--updater_health_check_interval_seconds=HEALTH_CHECK_INTERVAL_SECONDS`
  Time interval between subsequent shard status checks. Defaults to `3`.

### Renaming a Job

Renaming is a tricky operation as downstream clients must be informed of
the new name. A conservative approach
to renaming suitable for production services is:

1.  Modify the Aurora configuration file to change the role,
    environment, and/or name as appropriate to the standardized naming
    scheme.
2.  Check that only these naming components have changed
    with `aurora diff`.

        aurora diff <job_key> <job_configuration>

3.  Create the (identical) job at the new key. You may need to request a
    temporary quota increase.

        aurora create <new_job_key> <job_configuration>

4.  Migrate all clients over to the new job key. Update all links and
    dashboards. Ensure that both job keys run identical versions of the
    code while in this state.
5.  After verifying that all clients have successfully moved over, kill
    the old job.

        aurora kill <old_job_key>

6.  If you received a temporary quota increase, be sure to let the
    powers that be know you no longer need the additional capacity.

### Restarting Jobs

`restart` restarts all of a job key identified Job's shards:

    aurora restart <job_key> <configuration file>

Restarts are controlled on the client side, so aborting
the `restart` command halts the restart operation.

`restart` does a rolling restart. You almost always want to do this, but
not if all shards of a service are misbehaving and are
completely dysfunctional. To not do a rolling restart, use
the `-shards` option described below.

**Note**: `restart` only applies its command line arguments and does not
use or is affected by `update.config`. Restarting
does ***not*** involve a configuration change. To update the
configuration, use `update.config`.

The `<configuration file>` argument for restart is optional. Use it only
if it contains hook definitions and activations that affect the
`restart` command.

In addition to the required job key argument, there are eight
`restart` specific optional arguments:

- `--updater_health_check_interval_seconds`: Defaults to `3`, the time
  interval between subsequent shard status checks.
- `--shards=SHARDS`: Defaults to None, which restarts all shards.
  Otherwise, only the specified-by-id shards restart. They can be
  comma-separated `(0, 8, 9)`, a range `(3-5)` or a
  combination `(0, 3-5, 8, 9-11)`.
- `--batch_size`: Defaults to `1`, the number of shards to be started
  in one iteration. So, for example, for value 3, it tries to restart
  the first three shards specified by `--shards` simultaneously, then
  the next three, and so on.
- `--max_per_shard_failures=MAX_PER_SHARD_FAILURES`: Defaults to `0`,
  the maximum number of restarts per shard during restart. When
  exceeded, it increments the total failure count.
- `--max_total_failures=MAX_TOTAL_FAILURES`: Defaults to `0`, the
  maximum total number of shard failures tolerated during restart.
- `-o, --open_browser` Open a browser window to the scheduler UI Job
  page after a job changing operation happens. When `False`, the Job
  url prints on the console and the user has to copy/paste it
  manually. Defaults to `False`. Does not work when running in Vagrant.
- `--restart_threshold`: Defaults to `60`, the maximum number of
  seconds before a shard must move into the `RUNNING` state before
  it's considered a failure.
- `--watch_secs`: Defaults to `45`, the minimum number of seconds a
  shard must remain in `RUNNING` state before considered a success.

Cron Jobs
---------

You will see various commands and options relating to cron jobs in
`aurora -help` and similar. Ignore them, as they're not yet implemented.
You might be able to use them without causing an error, but nothing happens
if you do.

Comparing Jobs
--------------

    aurora diff <job_key> config

Compares a job configuration against a running job. By default the diff
is determined using `diff`, though you may choose an alternate
 diff program by specifying the `DIFF_VIEWER` environment variable.

There are two named parameters:

- `-E NAME=VALUE` Bind a Thermos mustache variable name to a
  value. Multiple flags may be used to specify multiple values.
  Defaults to `[]`.
- `-j, --json` Read the configuration argument in JSON format.
  Defaults to `False`.

Viewing/Examining Jobs
----------------------

Above we discussed creating, killing, and updating Jobs. Here we discuss
how to view and examine Jobs.

### Listing Jobs

    aurora list_jobs
    Usage: `aurora list_jobs cluster/role

Lists all Jobs registered with the Aurora scheduler in the named cluster for the named role.

It has two named parameters:

- `--pretty`: Displays job information in prettyprinted format.
  Defaults to `False`.
- `-c`, `--show-cron`: Shows cron schedule for jobs. Defaults to
  `False`. Do not use, as it's not yet implemented.

### Inspecting a Job

    aurora inspect <job_key> config

`inspect` verifies that its specified job can be parsed from a
configuration file, and displays the parsed configuration. It has four
named parameters:

- `--local`: Inspect the configuration that the  `spawn` command would
  create, defaulting to `False`.
- `--raw`: Shows the raw configuration. Defaults to `False`.
- `-j`, `--json`: If specified, configuration is read in JSON format.
  Defaults to `False`.
- `-E NAME=VALUE`: Bind a Thermos Mustache variable name to a value.
  You can use multiple flags to specify multiple values. Defaults
  to `[]`

### Versions

    aurora version

Lists client build information and what Aurora API version it supports.

### Checking Your Quota

    aurora get_quota --cluster=CLUSTER role

  Prints the production quota allocated to the role's value at the given
cluster.

### Finding a Job on Web UI

When you create a job, part of the output response contains a URL that goes
to the job's scheduler UI page. For example:

    vagrant@precise64:~$ aurora create devcluster/www-data/prod/hello /vagrant/examples/jobs/hello_world.aurora
    INFO] Creating job hello
    INFO] Response from scheduler: OK (message: 1 new tasks pending for job www-data/prod/hello)
    INFO] Job url: http://precise64:8081/scheduler/www-data/prod/hello

You can go to the scheduler UI page for this job via `http://precise64:8081/scheduler/www-data/prod/hello`
You can go to the overall scheduler UI page by going to the part of that URL that ends at `scheduler`; `http://precise64:8081/scheduler`

Once you click through to a role page, you see Jobs arranged
separately by pending jobs, active jobs and finished jobs.
Jobs are arranged by role, typically a service account for
production jobs and user accounts for test or development jobs.

### Getting Job Status

    aurora status <job_key>

Returns the status of recent tasks associated with the
`job_key` specified Job in its supplied cluster. Typically this includes
a mix of active tasks (running or assigned) and inactive tasks
(successful, failed, and lost.)

### Opening the Web UI

Use the Job's web UI scheduler URL or the `aurora status` command to find out on which
machines individual tasks are scheduled. You can open the web UI via the
`open` command line command if invoked from your machine:

    aurora open [<cluster>[/<role>[/<env>/<job_name>]]]

If only the cluster is specified, it goes directly to that cluster's
scheduler main page. If the role is specified, it goes to the top-level
role page. If the full job key is specified, it goes directly to the job
page where you can inspect individual tasks.

### SSHing to a Specific Task Machine

    aurora ssh <job_key> <shard number>

You can have the Aurora client ssh directly to the machine that has been
assigned a particular Job/shard number. This may be useful for quickly
diagnosing issues such as performance issues or abnormal behavior on a
particular machine.

It can take three named parameters:

- `-e`, `--executor_sandbox`:  Run `ssh` in the executor sandbox
  instead of the  task sandbox. Defaults to `False`.
- `--user=SSH_USER`: `ssh` as the given user instead of as the role in
  the `job_key` argument. Defaults to none.
- `-L PORT:NAME`: Add tunnel from local port `PORT` to the remote
  named port  `NAME`. Defaults to `[]`.

### Templating Command Arguments

    aurora run [-e] [-t THREADS] <job_key> -- <<command-line>>

Given a job specification, run the supplied command on all hosts and
return the output. You may use the standard Mustache templating rules:

- `{{thermos.ports[name]}}` substitutes the specific named port of the
  task assigned to this machine
- `{{mesos.instance}}` substitutes the shard id of the job's task
  assigned to this machine
- `{{thermos.task_id}}` substitutes the task id of the job's task
  assigned to this machine

For example, the following type of pattern can be a powerful diagnostic
tool:

    aurora run -t5 cluster1/tyg/devel/seizure -- \
      'curl -s -m1 localhost:{{thermos.ports[http]}}/vars | grep uptime'

By default, the command runs in the Task's sandbox. The `-e` option can
run the command in the executor's sandbox. This is mostly useful for
Aurora administrators.

You can parallelize the runs by using the `-t` option.
