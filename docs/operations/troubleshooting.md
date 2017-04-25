# Troubleshooting

So you've started your first cluster and are running into some issues? We've collected some common
stumbling blocks and solutions here to help get you moving.

## Replicated log not initialized

### Symptoms
- Scheduler RPCs and web interface claim `Storage is not READY`
- Scheduler log repeatedly prints messages like

  ```
  I1016 16:12:27.234133 26081 replica.cpp:638] Replica in EMPTY status
  received a broadcasted recover request
  I1016 16:12:27.234256 26084 recover.cpp:188] Received a recover response
  from a replica in EMPTY status
  ```

### Solution
When you create a new cluster, you need to inform a quorum of schedulers that they are safe to
consider their database to be empty by [initializing](installation.md#finalizing) the
replicated log. This is done to prevent the scheduler from modifying the cluster state in the event
of multiple simultaneous disk failures or, more likely, misconfiguration of the replicated log path.


## No distinct leader elected

### Symptoms
Either no scheduler or multiple scheduler believe to be leading.

### Solution
Verify the [network configuration](configuration.md#network-configuration) of the Aurora
scheduler is correct:

* The `LIBPROCESS_IP:LIBPROCESS_PORT` endpoints must be reachable from all coordinator nodes running
  a scheduler or a Mesos master.
* Hostname lookups have to resolve to public ips rather than local ones that cannot be reached
  from another node.

In addition, double-check the [quota settings](configuration.md#replicated-log-configuration) of the
replicated log.


## Scheduler not registered

### Symptoms
Scheduler log contains

    Framework has not been registered within the tolerated delay.

### Solution
Double-check that the scheduler is configured correctly to reach the Mesos master. If you are registering
the master in ZooKeeper, make sure command line argument to the master:

    --zk=zk://$ZK_HOST:2181/mesos/master

is the same as the one on the scheduler:

    -mesos_master_address=zk://$ZK_HOST:2181/mesos/master


## Scheduler not running

### Symptoms
The scheduler process commits suicide regularly. This happens under error conditions, but
also on purpose in regular intervals.

### Solution
Aurora is meant to be run under supervision. You have to configure a supervisor like
[Monit](http://mmonit.com/monit/), [supervisord](http://supervisord.org/), or systemd to run the
scheduler and restart it whenever it fails or exists on purpose.

Aurora supports an active health checking protocol on its admin HTTP interface - if a `GET /health`
times out or returns anything other than `200 OK` the scheduler process is unhealthy and should be
restarted.

For example, monit can be configured with

    if failed port 8081 send "GET /health HTTP/1.0\r\n" expect "OK\n" with timeout 2 seconds for 10 cycles then restart

assuming you set `-http_port=8081`.


## Executor crashing or hanging

### Symptoms
Launched task instances never transition to `STARTING` or `RUNNING` but immediately transition
to `FAILED` or `LOST`.

### Solution
The executor might be failing due to unknown internal errors such as a missing native dependency
of the Mesos executor library. Open the Mesos UI and navigate to the failing
task in question. Inspect the various log files in order to learn about what is going on.


## Observer does not discover tasks

### Symptoms
The observer UI does not list any tasks. When navigating from the scheduler UI to the state of
a particular task instance the observer returns `Error: 404 Not Found`.

### Solution
The observer is refreshing its internal state every couple of seconds. If waiting a few seconds
does not resolve the issue, check that the `--mesos-root` setting of the observer and the
`--work_dir` option of the Mesos agent are in sync. For details, see our
[Install instructions](installation.md#worker-configuration).
