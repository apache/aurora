0.17.0 (Not yet released)
=========================

### New/updated:
- A task's tier is now mapped to a label on the Mesos `TaskInfo` proto.
- The Aurora client is now using the Thrift binary protocol to communicate with the scheduler.
- Introduce a new `--ip` option to bind the Thermos observer to a specific rather than all
  interfaces.
- Fix error that prevents the scheduler from being launched with `-enable_revocable_ram`.
- The Aurora Scheduler API supports volume mounts per task for the Mesos
  Containerizer if the scheduler is running with the `-allow_container_volumes`
  flag.
* The executor will send SIGTERM to processes that self daemonize via double forking.
- Resolve docker tags to concrete identifiers for DockerContainerizer, so that job configuration
  is immutable across restarts. The feature introduces a new `{{docker.image[name][tag]}}` binder that
  can be used in the Aurora job configuration to resolve a docker image specified by its `name:tag`
  to a concrete identifier specified by its `registry/name@digest`. It requires version 2 of the
  Docker Registry.
- Use `RUNNING` state to indicate that the task is healthy and behaving as expected. Job updates
  can now rely purely on health checks rather than `watch_secs` timeout when deciding an individial
  instance update state, by setting `watch_secs` to 0. A service will remain in `STARTING` state
  util `min_consecutive_successes` consecutive health checks have passed.
- The default logging output has been changed to remove line numbers and inner class information in
  exchange for faster logging.

### Deprecations and removals:

- The scheduler flag `-zk_use_curator` has been removed. If you have never set the flag and are
  upgrading you should take care as described in the [note](#zk_use_curator_upgrade) below.

=======
0.16.0
======

### New/updated:

- Upgraded Mesos to 1.0.0. Note: as part of this upgrade we have switched from depending on
  the mesos.native egg for Thermos in favor of the stripped down mesos.executor egg. This means
  users launching Docker tasks with the Mesos DockerContainerizer are no longer required to use
  images that include all of Mesos's dependencies.
- Scheduler command line behavior has been modified to warn users of the deprecation of `production`
  attribute in `Job` thrift struct. The scheduler is queried for tier configurations and the user's
  choice of `tier` and `production` attributes is revised, if necessary. If `tier` is already set,
  the `production` attribute might be adjusted to match the `tier` selection. Otherwise, `tier` is
  selected based on the value of `production` attribute. If a matching tier is not found, the
  `default` tier from tier configuration file (`tiers.json`) is used.
- The `/offers` endpoint has been modified to display attributes of resource offers as received
  from Mesos. This has affected rendering of some of the existing attributes. Furthermore, it now
  dumps additional offer attributes including [reservations](http://mesos.apache.org/documentation/latest/reservation/)
  and [persistent volumes](http://mesos.apache.org/documentation/latest/persistent-volume/).
- The scheduler API now accepts both thrift JSON and binary thrift. If a request is sent without a
  `Content-Type` header, or a `Content-Type` header of `application/x-thrift` or `application/json`
  or `application/vnd.apache.thrift.json` the request is treated as thrift JSON. If a request is
  sent with a `Content-Type` header of `application/vnd.apache.thrift.binary` the request is treated
  as binary thrift. If the `Accept` header of the request is `application/vnd.apache.thrift.binary`
  then the response will be binary thrift. Any other value for `Accept` will result in thrift JSON.
- Scheduler is now able to launch jobs using more than one executor at a time. To use this feature
  the `-custom_executor_config` flag must point to a JSON file which contains at least one valid
  executor configuration as detailed in the [configuration](docs/features/custom-executors.md)
  documentation.
- Add rollback API to the scheduler and new client command to support rolling back
  active update jobs to their initial state.
- <a name="zk_use_curator_upgrade"></a> The scheduler flag `-zk_use_curator` now defaults to `true`
  and care should be taken when upgrading from a configuration that does not pass the flag. The
  scheduler upgrade should be performed by bringing all schedulers down, and then bringing upgraded
  schedulers up. A rolling upgrade would result in no leading scheduler for the duration of the
  roll which could be confusing to monitor and debug.
- A new command `aurora_admin reconcile_tasks` is now available on the Aurora admin client that can trigger
  implicit and explicit task reconciliations.
- Add a new MTTS (Median Time To Starting) metric in addition to MTTA and MTTR.
- In addition to CPU resources, RAM resources can now be treated as revocable via the scheduler
  commandline flag `-enable_revocable_ram`.
- Introduce UpdateMetadata fields in JobUpdateRequest to allow clients to store metadata on update.
- Changed cronSchedule field inside of JobConfiguration schema to be optional for compatibility with Go.
- Update default value of command line option `-framework_name` to 'Aurora'.
- Tasks launched with filesystem images and the Mesos unified containerizer are now fully isolated from
  the host's filesystem. As such they are no longer required to include any of the executor's
  dependencies (e.g. Python 2.7) within the task's filesystem.

### Deprecations and removals:

- The job configuration flag `production` is now deprecated. To achieve the same scheduling behavior
  that `production=true` used to provide, users should elect a `tier` for the job with attributes
  `preemptible=false` and `revocable=false`. For example, the `preferred` tier in the default tier
  configuration file (`tiers.json`) matches the above criteria.
- The `ExecutorInfo.source` field is deprecated and has been replaced with a label named `source`.
  It will be removed from Mesos in a future release.
- The scheduler flag `-zk_use_curator` has been deprecated. If you have never set the flag and are
  upgrading you should take care as described in the [note](#zk_use_curator_upgrade) above.
- The `key` argument of `getJobUpdateDetails` has been deprecated. Use the `query` argument instead.
- The --release-threshold option on `aurora job restart` has been removed.

0.15.0
======

### New/updated:

- New scheduler commandline argument -enable_mesos_fetcher to allow job submissions
to contain URIs which will be passed to the Mesos Fetcher and subsequently downloaded into
the sandbox. Please note that enabling job submissions to download resources from arbitrary
URIs may have security implications.
- Upgraded Mesos to 0.28.2.

### Deprecations and removals:

0.14.0
======

### New/updated:

- Upgraded Mesos to 0.27.2
- Added a new optional [Apache Curator](https://curator.apache.org/) backend for performing
  scheduler leader election. You can enable this with the new `-zk_use_curator` scheduler argument.
- Adding --nosetuid-health-checks flag to control whether the executor runs health checks as the
  job's role's user.
- New scheduler command line argument `-offer_filter_duration` to control the time after which we
  expect Mesos to re-offer unused resources. A short duration improves scheduling performance in
  smaller clusters, but might lead to resource starvation for other frameworks if you run multiple
  ones in your cluster. Uses the Mesos default of 5s.
- New scheduler command line option `-framework_name`  to change the name used for registering
  the Aurora framework with Mesos. The current default value is 'TwitterScheduler'.
- Added experimental support for launching tasks using filesystem images and the Mesos [unified
  containerizer](https://github.com/apache/mesos/blob/master/docs/container-image.md). See that
  linked documentation for details on configuring Mesos to use the unified containerizer. Note that
  earlier versions of Mesos do not fully support the unified containerizer. Mesos 0.28.x or later is
  recommended for anyone adopting task images via the Mesos containerizer.
- Upgraded to pystachio 0.8.1 to pick up support for the new [Choice type](https://github.com/wickman/pystachio/blob/v0.8.1/README.md#choices).
- The `container` property of a `Job` is now a Choice of either a `Container` holder, or a direct
  reference to either a `Docker` or `Mesos` container.
- New scheduler command line argument `-ip` to control what ip address to bind the schedulers http
  server to.
- Added experimental support for Mesos GPU resource. This feature will be available in Mesos 1.0
  and is disabled by default. Use `-allow_gpu_resource` flag to enable it.

  **IMPORTANT: once this feature is enabled, creating jobs with GPU resource will make scheduler
  snapshot backwards incompatible. Scheduler will be unable to read snapshot if rolled back to
  previous version. If rollback is absolutely necessary, perform the following steps:**
  1. Set `-allow_gpu_resource` to false
  2. Delete all jobs with GPU resource (including cron job schedules if applicable)
  3. Wait until GPU task history is pruned. You may speed it up by changing the history retention
    flags, e.g.: `-history_prune_threshold=1mins` and `-history_max_per_job_threshold=0`
  4. In case there were GPU job updates created, prune job update history for affected jobs from
    `/h2console` endpoint or reduce job update pruning thresholds, e.g.:
    `-job_update_history_pruning_threshold=1mins` and `-job_update_history_per_job_threshold=0`
  5. Ensure a new snapshot is created by running `aurora_admin scheduler_snapshot <cluster>`
  6. Rollback to previous version
- Experimental support for a webhook feature which POSTs all task state changes to a user defined
  endpoint.

- Added support for specifying the default tier name in tier configuration file (`tiers.json`). The
  `default` property is required and is initialized with the `preemptible` tier (`preemptible` tier
  tasks can be preempted but their resources cannot be revoked).

### Deprecations and removals:

- Deprecated `--restart-threshold` option in the `aurora job restart` command to match the job
  updater behavior. This option has no effect now and will be removed in the future release.
- Deprecated `-framework_name` default argument 'TwitterScheduler'. In a future release this
  will change to 'aurora'. Please be aware that depending on your usage of Mesos, this will
  be a backward incompatible change. For details, see MESOS-703.
- The `-thermos_observer_root` command line arg has been removed from the scheduler. This was a
  relic from the time when executor checkpoints were written globally, rather than into a task's
  sandbox.
- Setting the `container` property of a `Job` to a `Container` holder is deprecated in favor of
  setting it directly to the appropriate (i.e. `Docker` or `Mesos`) container type.
- Deprecated `numCpus`, `ramMb` and `diskMb` fields in `TaskConfig` and `ResourceAggregate` thrift
  structs. Use `set<Resource> resources` to specify task resources or quota values.
- The endpoint `/slaves` is deprecated. Please use `/agents` instead.
- Deprecated `production` field in `TaskConfig` thrift struct. Use `tier` field to specify task
  scheduling and resource handling behavior.
- The scheduler `resources_*_ram_gb` and `resources_*_disk_gb` metrics have been renamed to
  `resources_*_ram_mb` and `resources_*_disk_mb` respectively. Note the unit change: GB -> MB.

0.13.0
------

### New/updated:

- Upgraded Mesos to 0.26.0
- Added a new health endpoint (/leaderhealth) which can be used for load balancer health
  checks to always forward requests to the leading scheduler.
- Added a new `aurora job add` client command to scale out an existing job.
- Upgraded the scheduler ZooKeeper client from 3.4.6 to 3.4.8.
- Added support for dedicated constraints not exclusive to a particular role.
  See [here](docs/features/constraints.md#dedicated-attribute) for more details.
- Added a new argument `--announcer-hostname` to thermos executor to override hostname in service
  registry endpoint. See [here](docs/reference/configuration.md#announcer-objects) for details.
- Descheduling a cron job that was not actually scheduled will no longer return an error.
- Added a new argument `-thermos_home_in_sandbox` to the scheduler for optionally changing
  HOME to the sandbox during thermos executor/runner execution. This is useful in cases
  where the root filesystem inside of the container is read-only, as it moves PEX extraction into
  the sandbox. See [here](docs/operations/configuration.md#docker-containers)
  for more detail.
- Support for ZooKeeper authentication in the executor announcer. See
  [here](docs/operations/security.md#announcer-authentication) for details.
- Scheduler H2 in-memory database is now using
  [MVStore](http://www.h2database.com/html/mvstore.html)
  In addition, scheduler thrift snapshots are now supporting full DB dumps for faster restarts.
- Added scheduler argument `-require_docker_use_executor` that indicates whether the scheduler
  should accept tasks that use the Docker containerizer without an executor (experimental).
- Jobs referencing invalid tier name will be rejected by the scheduler.
- Added a new scheduler argument `--populate_discovery_info`. If set to true, Aurora will start
  to populate DiscoveryInfo field on TaskInfo of Mesos. This could be used for alternative
  service discovery solution like Mesos-DNS.
- Added support for automatic schema upgrades and downgrades when restoring a snapshot that contains
  a DB dump.

### Deprecations and removals:

- Removed deprecated (now redundant) fields:
  - `Identity.role`
  - `TaskConfig.environment`
  - `TaskConfig.jobName`
  - `TaskQuery.owner`
- Removed deprecated `AddInstancesConfig` parameter to `addInstances` RPC.
- Removed deprecated executor argument `-announcer-enable`, which was a no-op in 0.12.0.
- Removed deprecated API constructs related to Locks:
  - removed RPCs that managed locks
    - `acquireLock`
    - `releaseLock`
    - `getLocks`
  - removed `Lock` parameters to RPCs
    - `createJob`
    - `scheduleCronJob`
    - `descheduleCronJob`
    - `restartShards`
    - `killTasks`
    - `addInstances`
    - `replaceCronTemplate`
- Task ID strings are no longer prefixed by a timestamp.
- Changes to the way the scheduler reads command line arguments
  - Removed support for reading command line argument values from files.
  - Removed support for specifying command line argument names with fully-qualified class names.

0.12.0
------

### New/updated:

- Upgraded Mesos to 0.25.0.
- Upgraded the scheduler ZooKeeper client from 3.3.4 to 3.4.6.
- Added support for configuring Mesos role by passing `-mesos_role` to Aurora scheduler at start time.
  This enables resource reservation for Aurora when running in a shared Mesos cluster.
- Aurora task metadata is now mapped to Mesos task labels. Labels are prefixed with
  `org.apache.aurora.metadata.` to prevent clashes with other, external label sources.
- Added new scheduler flag `-default_docker_parameters` to allow a cluster operator to specify a
  universal set of parameters that should be used for every container that does not have parameters
  explicitly configured at the job level.
- Added support for jobs to specify arbitrary ZooKeeper paths for service registration.
  See [here](docs/reference/configuration.md#announcer-objects) for details.
- Log destination is configurable for the thermos runner. See the configuration reference for details
  on how to configure destination per-process. Command line options may also be passed through the
  scheduler in order to configure the global default behavior.
- Env variables can be passed through to task processes by passing `--preserve_env`
  to thermos.
- Changed scheduler logging to use logback.
  Operators wishing to customize logging may do so with standard
  [logback configuration](http://logback.qos.ch/manual/configuration.html)
- When using `--read-json`, aurora can now load multiple jobs from one json file,
  similar to the usual pystachio structure: `{"jobs": [job1, job2, ...]}`. The
  older single-job json format is also still supported.
- `aurora config list` command now supports `--read-json`
- Added scheduler command line argument `-shiro_after_auth_filter`. Optionally specify a class
  implementing javax.servlet.Filter that will be included in the Filter chain following the Shiro
  auth filters.
- The `addInstances` thrift RPC does now increase job instance count (scale out) based on the
  task template pointed by instance `key`.

### Deprecations and removals:

- Deprecated `AddInstancesConfig` argument in `addInstances` thrift RPC.
- Deprecated `TaskQuery` argument in `killTasks` thrift RPC to disallow killing tasks across
  multiple roles. The new safer approach is using `JobKey` with `instances` instead.
- Removed the deprecated field 'ConfigGroup.instanceIds' from the API.
- Removed the following deprecated `HealthCheckConfig` client-side configuration fields: `endpoint`,
  `expected_response`, `expected_response_code`.  These are now set exclusively in like-named fields
  of `HttpHealthChecker.`
- Removed the deprecated 'JobUpdateSettings.maxWaitToInstanceRunningMs' thrift api field (
  UpdateConfig.restart_threshold in client-side configuration). This aspect of job restarts is now
  controlled exclusively via the client with `aurora job restart --restart-threshold=[seconds]`.
- Deprecated executor flag `--announcer-enable`. Enabling the announcer previously required both flags
  `--announcer-enable` and `--announcer-ensemble`, but now only `--announcer-ensemble` must be set.
  `--announcer-enable` is a no-op flag now and will be removed in future version.
- Removed scheduler command line arguments:
  - `-enable_cors_support`.  Enabling CORS is now implicit by setting the argument
    `-enable_cors_for`.
  - `-deduplicate_snapshots` and `-deflate_snapshots`.  These features are good to always enable.
  - `-enable_job_updates` and `-enable_job_creation`
  - `-extra_modules`
  - `-logtostderr`, `-alsologtostderr`, `-vlog`, `-vmodule`, and `use_glog_formatter`. Removed
     in favor of the new logback configuration.


0.11.0
------

### New/updated:

- Upgraded Mesos to 0.24.1.
- Added a new scheduler flag 'framework_announce_principal' to support use of authorization and
  rate limiting in Mesos.
- Added support for shell-based health checkers in addition to HTTP health checkers. In concert with
  this change the `HealthCheckConfig` schema has been restructured to more cleanly allow configuring
  varied health checkers.
- Added support for taking in an executor configuration in JSON via a command line argument
  `--custom_executor_config` which will override all other the command line arguments and default
  values pertaining to the executor.
- Log rotation has been added to the thermos runner. See the configuration reference for details
  on how configure rotation per-process. Command line options may also be passed through the
  scheduler in order to configure the global default behavior.

### Deprecations and removals:

- The client-side updater has been removed, along with the CLI commands that used it:
  'aurora job update' and 'aurora job cancel-update'.  Users are encouraged to take
  advantage of scheduler-driven updates (see 'aurora update -h' for usage), which has been a
  stable feature for several releases.
- The following fields from `HealthCheckConfig` are now deprecated:
  `endpoint`, `expected_response`, `expected_response_code` in favor of setting them as part of an
  `HttpHealthChecker.`
- The field 'JobUpdateSettings.maxWaitToInstanceRunningMs' (UpdateConfig.restart_threshold in
  client-side configuration) is now deprecated.  This setting was brittle in practice, and is
  ignored by the 0.11.0 scheduler.


0.10.0
------

### New/updated:

- Upgraded Mesos to 0.23.0. NOTE: Aurora executor now requires openssl runtime dependencies that
  were not previously enforced. You will need libcurl available on every Mesos slave (or Docker
  container) to successfully launch Aurora executor.  See [here](docs/getting-started.md) for more
  details on Mesos runtime dependencies.
- Resource quota is no longer consumed by production jobs with a dedicated constraint (AURORA-1457).
- The Python build layout has changed:
  * The `apache.thermos` package has been removed.
  * The `apache.gen.aurora` package has been renamed to `apache.aurora.thrift`.
  * The `apache.gen.thermos` package has been renamed to `apache.thermos.thrift`.
  * A new `apache.thermos.runner` package has been introduced, providing the `thermos_runner`
    binary.
  * A new `apache.aurora.kerberos` package has been introduced, containing the Kerberos-supporting
    versions of `aurora` and `aurora_admin` (`kaurora` and `kaurora_admin`).
  * Most BUILD targets under `src/main` have been removed, see [here](http://s.apache.org/b8z) for
    details.

### Deprecations and removals:

- Removed the `--root` option from the observer.
- Thrift `ConfigGroup.instanceIds` field has been deprecated. Use ConfigGroup.instances instead.
- Deprecated `SessionValidator` and `CapabilityValidator` interfaces have been removed. All
  `SessionKey`-typed arguments are now nullable and ignored by the scheduler Thrift API.


0.9.0
-----

- Now requires JRE 8 or greater.
- GC executor is fully replaced by the task state reconciliation (AURORA-1047).
- The scheduler command line argument `-enable_legacy_constraints` has been
  removed, and the scheduler no longer automatically injects `host` and `rack`
  constraints for production services. (AURORA-1074)
- SLA metrics for non-production jobs have been disabled by default. They can
  be enabled via the scheduler command line. Metric names have changed from
  `...nonprod_ms` to `...ms_nonprod` (AURORA-1350).


0.8.0
-----

- A new command line argument was added to the observer: `--mesos-root`
  This must point to the same path as `--work_dir` on the mesos slave.
- Build targets for thermos and observer have changed, they are now:
  * `src/main/python/apache/aurora/tools:thermos`
  * `src/main/python/apache/aurora/tools:thermos_observer`
