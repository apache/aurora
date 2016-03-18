# Deploying the Aurora Scheduler

When setting up your cluster, you will install the scheduler on a small number (usually 3 or 5) of
machines.  This guide helps you get the scheduler set up and troubleshoot some common hurdles.

- [Installing Aurora](#installing-aurora)
  - [Creating the Distribution .zip File (Optional)](#creating-the-distribution-zip-file-optional)
  - [Installing Aurora](#installing-aurora-1)
- [Configuring Aurora](#configuring-aurora)
  - [A Note on Configuration](#a-note-on-configuration)
  - [Replicated Log Configuration](#replicated-log-configuration)
  - [Initializing the Replicated Log](#initializing-the-replicated-log)
  - [Storage Performance Considerations](#storage-performance-considerations)
  - [Network considerations](#network-considerations)
  - [Considerations for running jobs in docker](#considerations-for-running-jobs-in-docker)
  - [Security Considerations](#security-considerations)
  - [Configuring Resource Oversubscription](#configuring-resource-oversubscription)
  - [Process Logs](#process-logs)
- [Running Aurora](#running-aurora)
  - [Maintaining an Aurora Installation](#maintaining-an-aurora-installation)
  - [Monitoring](#monitoring)
  - [Running stateful services](#running-stateful-services)
    - [Dedicated attribute](#dedicated-attribute)
      - [Syntax](#syntax)
      - [Example](#example)
- [Best practices](#best-practices)
  - [Diversity](#diversity)
- [Common problems](#common-problems)
  - [Replicated log not initialized](#replicated-log-not-initialized)
    - [Symptoms](#symptoms)
    - [Solution](#solution)
  - [Scheduler not registered](#scheduler-not-registered)
    - [Symptoms](#symptoms-1)
    - [Solution](#solution-1)
- [Changing Scheduler Quorum Size](#changing-scheduler-quorum-size)
    - [Preparation](#preparation)
    - [Adding New Schedulers](#adding-new-schedulers)

## Installing Aurora
The Aurora scheduler is a standalone Java server. As part of the build process it creates a bundle
of all its dependencies, with the notable exceptions of the JVM and libmesos. Each target server
should have a JVM (Java 8 or higher) and libmesos (0.25.0) installed.

### Creating the Distribution .zip File (Optional)
To create a distribution for installation you will need build tools installed. On Ubuntu this can be
done with `sudo apt-get install build-essential default-jdk`.

    git clone http://git-wip-us.apache.org/repos/asf/aurora.git
    cd aurora
    ./gradlew distZip

Copy the generated `dist/distributions/aurora-scheduler-*.zip` to each node that will run a scheduler.

### Installing Aurora
Extract the aurora-scheduler zip file. The example configurations assume it is extracted to
`/usr/local/aurora-scheduler`.

    sudo unzip dist/distributions/aurora-scheduler-*.zip -d /usr/local
    sudo ln -nfs "$(ls -dt /usr/local/aurora-scheduler-* | head -1)" /usr/local/aurora-scheduler

## Configuring Aurora

### A Note on Configuration
Like Mesos, Aurora uses command-line flags for runtime configuration. As such the Aurora
"configuration file" is typically a `scheduler.sh` shell script of the form.

    #!/bin/bash
    AURORA_HOME=/usr/local/aurora-scheduler

    # Flags controlling the JVM.
    JAVA_OPTS=(
      -Xmx2g
      -Xms2g
      # GC tuning, etc.
    )

    # Flags controlling the scheduler.
    AURORA_FLAGS=(
      -http_port=8081
      # Log configuration, etc.
    )

    # Environment variables controlling libmesos
    export JAVA_HOME=...
    export GLOG_v=1
    export LIBPROCESS_PORT=8083

    JAVA_OPTS="${JAVA_OPTS[*]}" exec "$AURORA_HOME/bin/aurora-scheduler" "${AURORA_FLAGS[@]}"

That way Aurora's current flags are visible in `ps` and in the `/vars` admin endpoint.

Examples are available under `examples/scheduler/`. For a list of available Aurora flags and their
documentation, see [this document](scheduler-configuration.md).

### Replicated Log Configuration
All Aurora state is persisted to a replicated log. This includes all jobs Aurora is running
including where in the cluster they are being run and the configuration for running them, as
well as other information such as metadata needed to reconnect to the Mesos master, resource
quotas, and any other locks in place.

Aurora schedulers use ZooKeeper to discover log replicas and elect a leader. Only one scheduler is
leader at a given time - the other schedulers follow log writes and prepare to take over as leader
but do not communicate with the Mesos master. Either 3 or 5 schedulers are recommended in a
production deployment depending on failure tolerance and they must have persistent storage.

In a cluster with `N` schedulers, the flag `-native_log_quorum_size` should be set to
`floor(N/2) + 1`. So in a cluster with 1 scheduler it should be set to `1`, in a cluster with 3 it
should be set to `2`, and in a cluster of 5 it should be set to `3`.

  Number of schedulers (N) | ```-native_log_quorum_size``` setting (```floor(N/2) + 1```)
  ------------------------ | -------------------------------------------------------------
  1                        | 1
  3                        | 2
  5                        | 3
  7                        | 4

*Incorrectly setting this flag will cause data corruption to occur!*

See [this document](storage-config.md#scheduler-storage-configuration-flags) for more replicated
log and storage configuration options.

## Initializing the Replicated Log
Before you start Aurora you will also need to initialize the log on a majority of the schedulers.

    mesos-log initialize --path="/path/to/native/log"

The `--path` flag should match the `--native_log_file_path` flag to the scheduler.
Failing to do this will result the following message when you try to start the scheduler.

    Replica in EMPTY status received a broadcasted recover request

### Storage Performance Considerations

See [this document](scheduler-storage.md) for scheduler storage performance considerations.

### Network considerations
The Aurora scheduler listens on 2 ports - an HTTP port used for client RPCs and a web UI,
and a libprocess (HTTP+Protobuf) port used to communicate with the Mesos master and for the log
replication protocol. These can be left unconfigured (the scheduler publishes all selected ports
to ZooKeeper) or explicitly set in the startup script as follows:

    # ...
    AURORA_FLAGS=(
      # ...
      -http_port=8081
      # ...
    )
    # ...
    export LIBPROCESS_PORT=8083
    # ...

### Considerations for running jobs in docker containers
In order for Aurora to launch jobs using docker containers, a few extra configuration options
must be set.  The [docker containerizer](http://mesos.apache.org/documentation/latest/docker-containerizer/)
must be enabled on the mesos slaves by launching them with the `--containerizers=docker,mesos` option.

By default, Aurora will configure Mesos to copy the file specified in `-thermos_executor_path`
into the container's sandbox.  If using a wrapper script to launch the thermos executor,
specify the path to the wrapper in that argument. In addition, the path to the executor pex itself
must be included in the `-thermos_executor_resources` option. Doing so will ensure that both the
wrapper script and executor are correctly copied into the sandbox. Finally, ensure the wrapper
script does not access resources outside of the sandbox, as when the script is run from within a
docker container those resources will not exist.

In order to correctly execute processes inside a job, the docker container must have python 2.7
installed.

A scheduler flag, `-global_container_mounts` allows mounting paths from the host (i.e., the slave)
into all containers on that host. The format is a comma separated list of host_path:container_path[:mode]
tuples. For example `-global_container_mounts=/opt/secret_keys_dir:/mnt/secret_keys_dir:ro` mounts
`/opt/secret_keys_dir` from the slaves into all launched containers. Valid modes are `ro` and `rw`.

If you would like to supply your own parameters to `docker run` when launching jobs in docker
containers, you may use the following flags:

    -allow_docker_parameters
    -default_docker_parameters

`-allow_docker_parameters` controls whether or not users may pass their own configuration parameters
through the job configuration files. If set to `false` (the default), the scheduler will reject
jobs with custom parameters. *NOTE*: this setting should be used with caution as it allows any job
owner to specify any parameters they wish, including those that may introduce security concerns
(`privileged=true`, for example).

`-default_docker_parameters` allows a cluster operator to specify a universal set of parameters that
should be used for every container that does not have parameters explicitly configured at the job
level. The argument accepts a multimap format:

    -default_docker_parameters="read-only=true,tmpfs=/tmp,tmpfs=/run"

### Process Logs

#### Log destination
By default, Thermos will write process stdout/stderr to log files in the sandbox. Process object configuration
allows specifying alternate log file destinations like streamed stdout/stderr or suppression of all log output.
Default behavior can be configured for the entire cluster with the following flag (through the `-thermos_executor_flags`
argument to the Aurora scheduler):

    --runner-logger-destination=both

`both` configuration will send logs to files and stream to parent stdout/stderr outputs.

See [this document](configuration-reference.md#logger) for all destination options.

#### Log rotation
By default, Thermos will not rotate the stdout/stderr logs from child processes and they will grow
without bound. An individual user may change this behavior via configuration on the Process object,
but it may also be desirable to change the default configuration for the entire cluster.
In order to enable rotation by default, the following flags can be applied to Thermos (through the
-thermos_executor_flags argument to the Aurora scheduler):

    --runner-logger-mode=rotate
    --runner-rotate-log-size-mb=100
    --runner-rotate-log-backups=10

In the above example, each instance of the Thermos runner will rotate stderr/stdout logs once they
reach 100 MiB in size and keep a maximum of 10 backups. If a user has provided a custom setting for
their process, it will override these default settings.

## Running Aurora
Configure a supervisor like [Monit](http://mmonit.com/monit/) or
[supervisord](http://supervisord.org/) to run the created `scheduler.sh` file and restart it
whenever it fails. Aurora expects to be restarted by an external process when it fails. Aurora
supports an active health checking protocol on its admin HTTP interface - if a `GET /health` times
out or returns anything other than `200 OK` the scheduler process is unhealthy and should be
restarted.

For example, monit can be configured with

    if failed port 8081 send "GET /health HTTP/1.0\r\n" expect "OK\n" with timeout 2 seconds for 10 cycles then restart

assuming you set `-http_port=8081`.

## Security Considerations

See [security.md](security.md).

## Configuring Resource Oversubscription

**WARNING**: This feature is currently in alpha status. Do not use it in production clusters!
See [this document](configuration-reference.md#revocable-jobs) for more feature details.

Set these scheduler flag to allow receiving revocable Mesos offers:

    -receive_revocable_resources=true

Specify a tier configuration file path:

    -tier_config=path/to/tiers/config.json

Default [tier configuration file](../src/main/resources/org/apache/aurora/scheduler/tiers.json).

### Maintaining an Aurora Installation

### Monitoring
Please see our dedicated [monitoring guide](monitoring.md) for in-depth discussion on monitoring.

### Running stateful services
Aurora is best suited to run stateless applications, but it also accommodates for stateful services
like databases, or services that otherwise need to always run on the same machines.

#### Dedicated attribute
The Mesos slave has the `--attributes` command line argument which can be used to mark a slave with
static attributes (not to be confused with `--resources`, which are dynamic and accounted).

Aurora makes these attributes available for matching with scheduling
[constraints](configuration-reference.md#specifying-scheduling-constraints).  Most of these
constraints are arbitrary and available for custom use.  There is one exception, though: the
`dedicated` attribute.  Aurora treats this specially, and only allows matching jobs to run on these
machines, and will only schedule matching jobs on these machines.

See the [section](resources.md#resource-quota) about resource quotas to learn how quotas apply to
dedicated jobs.

##### Syntax
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

## Best practices
### Diversity
Data centers are often organized with hierarchical failure domains.  Common failure domains
include hosts, racks, rows, and PDUs.  If you have this information available, it is wise to tag
the mesos-slave with them as
[attributes](https://mesos.apache.org/documentation/attributes-resources/).

When it comes time to schedule jobs, Aurora will automatically spread them across the failure
domains as specified in the
[job configuration](configuration-reference.md#specifying-scheduling-constraints).

Note: in virtualized environments like EC2, the only attribute that usually makes sense for this
purpose is `host`.

## Common problems
So you've started your first cluster and are running into some issues? We've collected some common
stumbling blocks and solutions here to help get you moving.

### Replicated log not initialized

#### Symptoms
- Scheduler RPCs and web interface claim `Storage is not READY`
- Scheduler log repeatedly prints messages like

  ```
  I1016 16:12:27.234133 26081 replica.cpp:638] Replica in EMPTY status
  received a broadcasted recover request
  I1016 16:12:27.234256 26084 recover.cpp:188] Received a recover response
  from a replica in EMPTY status
  ```

#### Solution
When you create a new cluster, you need to inform a quorum of schedulers that they are safe to
consider their database to be empty by [initializing](#initializing-the-replicated-log) the
replicated log. This is done to prevent the scheduler from modifying the cluster state in the event
of multiple simultaneous disk failures or, more likely, misconfiguration of the replicated log path.

### Scheduler not registered

#### Symptoms
Scheduler log contains

    Framework has not been registered within the tolerated delay.

#### Solution
Double-check that the scheduler is configured correctly to reach the master. If you are registering
the master in ZooKeeper, make sure command line argument to the master:

    --zk=zk://$ZK_HOST:2181/mesos/master

is the same as the one on the scheduler:

    -mesos_master_address=zk://$ZK_HOST:2181/mesos/master

## Changing Scheduler Quorum Size
Special care needs to be taken when changing the size of the Aurora scheduler quorum.
Since Aurora uses a Mesos replicated log, similar steps need to be followed as when
[changing the mesos quorum size](http://mesos.apache.org/documentation/latest/operational-guide).

### Preparation
Increase [-native_log_quorum_size](storage-config.md#-native_log_quorum_size) on each
existing scheduler and restart them. When updating from 3 to 5 schedulers, the quorum size
would grow from 2 to 3.

### Adding New Schedulers
Start the new schedulers with `-native_log_quorum_size` set to the new value. Failing to
first increase the quorum size on running schedulers can in some cases result in corruption
or truncating of the replicated log used by Aurora. In that case, see the documentation on
[recovering from backup](storage-config.md#recovering-from-a-scheduler-backup).
