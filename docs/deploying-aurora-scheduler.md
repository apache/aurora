The Aurora scheduler is responsible for scheduling new jobs, rescheduling failed jobs, and killing
old jobs.

Installing Aurora
=================
Aurora is a standalone Java server. As part of the build process it creates a bundle of all its
dependencies, with the notable exceptions of the JVM and libmesos. Each target server should have
a JVM (Java 7 or higher) and libmesos (0.18.0) installed.

Creating the Distribution .zip File (Optional)
----------------------------------------------
To create a distribution for installation you will need build tools installed. On Ubuntu this can be
done with `sudo apt-get install build-essential default-jdk`.

    git clone http://git-wip-us.apache.org/repos/asf/incubator-aurora.git
    cd incubator-aurora
    ./gradlew distZip

Copy the generated `dist/distributions/aurora-scheduler-*.zip` to each node that will run a scheduler.

Installing Aurora
-----------------
Extract the aurora-scheduler zip file. The example configurations assume it is extracted to
`/usr/local/aurora-scheduler`.

    sudo unzip dist/distributions/aurora-scheduler-*.zip -d /usr/local
    sudo ln -nfs "$(ls -dt /usr/local/aurora-scheduler-* | head -1)" /usr/local/aurora-scheduler

Configuring Aurora
==================

A Note on Configuration
-----------------------
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
documentation run

    /usr/local/aurora-scheduler/bin/aurora-scheduler -help

Replicated Log Configuration
----------------------------
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

Initializing the Replicated Log
-------------------------------

Before you start Aurora you will also need to initialize the log on the first master.

    mesos-log initialize --path="$AURORA_HOME/scheduler/db"

Failing to do this will result the following message when you try to start the scheduler.

    Replica in EMPTY status received a broadcasted recover request

Network considerations
----------------------
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

Running Aurora
==============
Configure a supervisor like [Monit](http://mmonit.com/monit/) or
[supervisord](http://supervisord.org/) to run the created `scheduler.sh` file and restart it
whenever it fails. Aurora expects to be restarted by an external process when it fails. Aurora
supports an active health checking protocol on its admin HTTP interface - if a `GET /health` times
out or returns anything other than `200 OK` the scheduler process is unhealthy and should be
restarted.

For example, monit can be configured with

    if failed port 8081 send "GET /health HTTP/1.0\r\n" expect "OK\n" with timeout 2 seconds for 10 cycles then restart

assuming you set `-http_port=8081`.

Maintaining an Aurora Installation
==================================

Monitoring
----------
Please see our dedicated [monitoring guide](monitoring.md) for in-depth discussion on monitoring.
