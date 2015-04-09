# Monitoring your Aurora cluster

Before you start running important services in your Aurora cluster, it's important to set up
monitoring and alerting of Aurora itself.  Most of your monitoring can be against the scheduler,
since it will give you a global view of what's going on.

## Reading stats
The scheduler exposes a *lot* of instrumentation data via its HTTP interface. You can get a quick
peek at the first few of these in our vagrant image:

    $ vagrant ssh -c 'curl -s localhost:8081/vars | head'
    async_tasks_completed 1004
    attribute_store_fetch_all_events 15
    attribute_store_fetch_all_events_per_sec 0.0
    attribute_store_fetch_all_nanos_per_event 0.0
    attribute_store_fetch_all_nanos_total 3048285
    attribute_store_fetch_all_nanos_total_per_sec 0.0
    attribute_store_fetch_one_events 3391
    attribute_store_fetch_one_events_per_sec 0.0
    attribute_store_fetch_one_nanos_per_event 0.0
    attribute_store_fetch_one_nanos_total 454690753

These values are served as `Content-Type: text/plain`, with each line containing a space-separated metric
name and value. Values may be integers, doubles, or strings (note: strings are static, others
may be dynamic).

If your monitoring infrastructure prefers JSON, the scheduler exports that as well:

    $ vagrant ssh -c 'curl -s localhost:8081/vars.json | python -mjson.tool | head'
    {
        "async_tasks_completed": 1009,
        "attribute_store_fetch_all_events": 15,
        "attribute_store_fetch_all_events_per_sec": 0.0,
        "attribute_store_fetch_all_nanos_per_event": 0.0,
        "attribute_store_fetch_all_nanos_total": 3048285,
        "attribute_store_fetch_all_nanos_total_per_sec": 0.0,
        "attribute_store_fetch_one_events": 3409,
        "attribute_store_fetch_one_events_per_sec": 0.0,
        "attribute_store_fetch_one_nanos_per_event": 0.0,

This will be the same data as above, served with `Content-Type: application/json`.

## Viewing live stat samples on the scheduler
The scheduler uses the Twitter commons stats library, which keeps an internal time-series database
of exported variables - nearly everything in `/vars` is available for instant graphing.  This is
useful for debugging, but is not a replacement for an external monitoring system.

You can view these graphs on a scheduler at `/graphview`.  It supports some composition and
aggregation of values, which can be invaluable when triaging a problem.  For example, if you have
the scheduler running in vagrant, check out these links:
[simple graph](http://192.168.33.7:8081/graphview?query=jvm_uptime_secs)
[complex composition](http://192.168.33.7:8081/graphview?query=rate\(scheduler_log_native_append_nanos_total\)%2Frate\(scheduler_log_native_append_events\)%2F1e6)

### Counters and gauges
Among numeric stats, there are two fundamental types of stats exported: _counters_ and _gauges_.
Counters are guaranteed to be monotonically-increasing for the lifetime of a process, while gauges
may decrease in value.  Aurora uses counters to represent things like the number of times an event
has occurred, and gauges to capture things like the current length of a queue.  Counters are a
natural fit for accurate composition into [rate ratios](http://en.wikipedia.org/wiki/Rate_ratio)
(useful for sample-resistant latency calculation), while gauges are not.

# Alerting

## Quickstart
If you are looking for just bare-minimum alerting to get something in place quickly, set up alerting
on `framework_registered` and `task_store_LOST`. These will give you a decent picture of overall
health.

## A note on thresholds
One of the most difficult things in monitoring is choosing alert thresholds. With many of these
stats, there is no value we can offer as a threshold that will be guaranteed to work for you. It
will depend on the size of your cluster, number of jobs, churn of tasks in the cluster, etc. We
recommend you start with a strict value after viewing a small amount of collected data, and then
adjust thresholds as you see fit. Feel free to ask us if you would like to validate that your alerts
and thresholds make sense.

## Important stats

### `jvm_uptime_secs`
Type: integer counter

The number of seconds the JVM process has been running. Comes from
[RuntimeMXBean#getUptime()](http://docs.oracle.com/javase/7/docs/api/java/lang/management/RuntimeMXBean.html#getUptime\(\))

Detecting resets (decreasing values) on this stat will tell you that the scheduler is failing to
stay alive.

Look at the scheduler logs to identify the reason the scheduler is exiting.

### `system_load_avg`
Type: double gauge

The current load average of the system for the last minute. Comes from
[OperatingSystemMXBean#getSystemLoadAverage()](http://docs.oracle.com/javase/7/docs/api/java/lang/management/OperatingSystemMXBean.html?is-external=true#getSystemLoadAverage\(\)).

A high sustained value suggests that the scheduler machine may be over-utilized.

Use standard unix tools like `top` and `ps` to track down the offending process(es).

### `process_cpu_cores_utilized`
Type: double gauge

The current number of CPU cores in use by the JVM process. This should not exceed the number of
logical CPU cores on the machine. Derived from
[OperatingSystemMXBean#getProcessCpuTime()](http://docs.oracle.com/javase/7/docs/jre/api/management/extension/com/sun/management/OperatingSystemMXBean.html)

A high sustained value indicates that the scheduler is overworked. Due to current internal design
limitations, if this value is sustained at `1`, there is a good chance the scheduler is under water.

There are two main inputs that tend to drive this figure: task scheduling attempts and status
updates from Mesos.  You may see activity in the scheduler logs to give an indication of where
time is being spent.  Beyond that, it really takes good familiarity with the code to effectively
triage this.  We suggest engaging with an Aurora developer.

### `task_store_LOST`
Type: integer gauge

The number of tasks stored in the scheduler that are in the `LOST` state, and have been rescheduled.

If this value is increasing at a high rate, it is a sign of trouble.

There are many sources of `LOST` tasks in Mesos: the scheduler, master, slave, and executor can all
trigger this.  The first step is to look in the scheduler logs for `LOST` to identify where the
state changes are originating.

### `scheduler_resource_offers`
Type: integer counter

The number of resource offers that the scheduler has received.

For a healthy scheduler, this value must be increasing over time.

Assuming the scheduler is up and otherwise healthy, you will want to check if the master thinks it
is sending offers. You should also look at the master's web interface to see if it has a large
number of outstanding offers that it is waiting to be returned.

### `framework_registered`
Type: binary integer counter

Will be `1` for the leading scheduler that is registered with the Mesos master, `0` for passive
schedulers,

A sustained period without a `1` (or where `sum() != 1`) warrants investigation.

If there is no leading scheduler, look in the scheduler and master logs for why.  If there are
multiple schedulers claiming leadership, this suggests a split brain and warrants filing a critical
bug.

### `rate(scheduler_log_native_append_nanos_total)/rate(scheduler_log_native_append_events)`
Type: rate ratio of integer counters

This composes two counters to compute a windowed figure for the latency of replicated log writes.

A hike in this value suggests disk bandwidth contention.

Look in scheduler logs for any reported oddness with saving to the replicated log. Also use
standard tools like `vmstat` and `iotop` to identify whether the disk has become slow or
over-utilized. We suggest using a dedicated disk for the replicated log to mitigate this.

### `timed_out_tasks`
Type: integer counter

Tracks the number of times the scheduler has given up while waiting
(for `-transient_task_state_timeout`) to hear back about a task that is in a transient state
(e.g. `ASSIGNED`, `KILLING`), and has moved to `LOST` before rescheduling.

This value is currently known to increase occasionally when the scheduler fails over
([AURORA-740](https://issues.apache.org/jira/browse/AURORA-740)). However, any large spike in this
value warrants investigation.

The scheduler will log when it times out a task. You should trace the task ID of the timed out
task into the master, slave, and/or executors to determine where the message was dropped.

### `http_500_responses_events`
Type: integer counter

The total number of HTTP 500 status responses sent by the scheduler. Includes API and asset serving.

An increase warrants investigation.

Look in scheduler logs to identify why the scheduler returned a 500, there should be a stack trace.
