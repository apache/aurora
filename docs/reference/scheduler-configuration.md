# Scheduler Configuration Reference

The Aurora scheduler can take a variety of configuration options through command-line arguments.
A list of the available options can be seen by running `aurora-scheduler -help`.

Please refer to the [Operator Configuration Guide](../operations/configuration.md) for details on how
to properly set the most important options.

```
$ aurora-scheduler -help
-------------------------------------------------------------------------
-h or -help to print this help message

Required flags:
-backup_dir [not null]
	Directory to store backups under. Will be created if it does not exist.
-cluster_name [not null]
	Name to identify the cluster being served.
-db_max_active_connection_count [must be > 0]
	Max number of connections to use with database via MyBatis
-db_max_idle_connection_count [must be > 0]
	Max number of idle connections to the database via MyBatis
-framework_authentication_file
	Properties file which contains framework credentials to authenticate with Mesosmaster. Must contain the properties 'aurora_authentication_principal' and 'aurora_authentication_secret'.
-ip
	The ip address to listen. If not set, the scheduler will listen on all interfaces.
-mesos_master_address [not null]
	Address for the mesos master, can be a socket address or zookeeper path.
-mesos_role
	The Mesos role this framework will register as. The default is to left this empty, and the framework will register without any role and only receive unreserved resources in offer.
-serverset_path [not null, must be non-empty]
	ZooKeeper ServerSet path to register at.
-shiro_after_auth_filter
	Fully qualified class name of the servlet filter to be applied after the shiro auth filters are applied.
-thermos_executor_path
	Path to the thermos executor entry point.
-tier_config [file must be readable]
	Configuration file defining supported task tiers, task traits and behaviors.
-webhook_config [file must exist, file must be readable]
	Path to webhook configuration file.
-zk_endpoints [must have at least 1 item]
	Endpoint specification for the ZooKeeper servers.

Optional flags:
-allow_container_volumes (default false)
	Allow passing in volumes in the job. Enabling this could pose a privilege escalation threat.
-allow_docker_parameters (default false)
	Allow to pass docker container parameters in the job.
-allow_gpu_resource (default false)
	Allow jobs to request Mesos GPU resource.
-allowed_container_types (default [MESOS])
	Container types that are allowed to be used by jobs.
-async_slot_stat_update_interval (default (1, mins))
	Interval on which to try to update open slot stats.
-async_task_stat_update_interval (default (1, hrs))
	Interval on which to try to update resource consumption stats.
-async_worker_threads (default 8)
	The number of worker threads to process async task operations with.
-backup_interval (default (1, hrs))
	Minimum interval on which to write a storage backup.
-cron_scheduler_num_threads (default 10)
	Number of threads to use for the cron scheduler thread pool.
-cron_scheduling_max_batch_size (default 10) [must be > 0]
	The maximum number of triggered cron jobs that can be processed in a batch.
-cron_start_initial_backoff (default (5, secs))
	Initial backoff delay while waiting for a previous cron run to be killed.
-cron_start_max_backoff (default (1, mins))
	Max backoff delay while waiting for a previous cron run to be killed.
-cron_timezone (default GMT)
	TimeZone to use for cron predictions.
-custom_executor_config [file must exist, file must be readable]
	Path to custom executor settings configuration file.
-db_lock_timeout (default (1, mins))
	H2 table lock timeout
-db_row_gc_interval (default (2, hrs))
	Interval on which to scan the database for unused row references.
-default_docker_parameters (default {})
	Default docker parameters for any job that does not explicitly declare parameters.
-dlog_max_entry_size (default (512, KB))
	Specifies the maximum entry size to append to the log. Larger entries will be split across entry Frames.
-dlog_shutdown_grace_period (default (2, secs))
	Specifies the maximum time to wait for scheduled checkpoint and snapshot actions to complete before forcibly shutting down.
-dlog_snapshot_interval (default (1, hrs))
	Specifies the frequency at which snapshots of local storage are taken and written to the log.
-enable_cors_for
	List of domains for which CORS support should be enabled.
-enable_h2_console (default false)
	Enable H2 DB management console.
-enable_mesos_fetcher (default false)
	Allow jobs to pass URIs to the Mesos Fetcher. Note that enabling this feature could pose a privilege escalation threat.
-enable_preemptor (default true)
	Enable the preemptor and preemption
-enable_revocable_cpus (default true)
	Treat CPUs as a revocable resource.
-enable_revocable_ram (default false)
	Treat RAM as a revocable resource.
-executor_user (default root)
	User to start the executor. Defaults to "root". Set this to an unprivileged user if the mesos master was started with "--no-root_submissions". If set to anything other than "root", the executor will ignore the "role" setting for jobs since it can't use setuid() anymore. This means that all your jobs will run under the specified user and the user has to exist on the Mesos agents.
-first_schedule_delay (default (1, ms))
	Initial amount of time to wait before first attempting to schedule a PENDING task.
-flapping_task_threshold (default (5, mins))
	A task that repeatedly runs for less than this time is considered to be flapping.
-framework_announce_principal (default false)
	When 'framework_authentication_file' flag is set, the FrameworkInfo registered with the mesos master will also contain the principal. This is necessary if you intend to use mesos authorization via mesos ACLs. The default will change in a future release. Changing this value is backwards incompatible. For details, see MESOS-703.
-framework_failover_timeout (default (21, days))
	Time after which a framework is considered deleted.  SHOULD BE VERY HIGH.
-framework_name (default Aurora)
	Name used to register the Aurora framework with Mesos.
-global_container_mounts (default [])
	A comma separated list of mount points (in host:container form) to mount into all (non-mesos) containers.
-history_max_per_job_threshold (default 100)
	Maximum number of terminated tasks to retain in a job history.
-history_min_retention_threshold (default (1, hrs))
	Minimum guaranteed time for task history retention before any pruning is attempted.
-history_prune_threshold (default (2, days))
	Time after which the scheduler will prune terminated task history.
-hostname
	The hostname to advertise in ZooKeeper instead of the locally-resolved hostname.
-http_authentication_mechanism (default NONE)
	HTTP Authentication mechanism to use.
-http_port (default 0)
	The port to start an HTTP server on.  Default value will choose a random port.
-initial_flapping_task_delay (default (30, secs))
	Initial amount of time to wait before attempting to schedule a flapping task.
-initial_schedule_penalty (default (1, secs))
	Initial amount of time to wait before attempting to schedule a task that has failed to schedule.
-initial_task_kill_retry_interval (default (5, secs))
	When killing a task, retry after this delay if mesos has not responded, backing off up to transient_task_state_timeout
-job_update_history_per_job_threshold (default 10)
	Maximum number of completed job updates to retain in a job update history.
-job_update_history_pruning_interval (default (15, mins))
	Job update history pruning interval.
-job_update_history_pruning_threshold (default (30, days))
	Time after which the scheduler will prune completed job update history.
-kerberos_debug (default false)
	Produce additional Kerberos debugging output.
-kerberos_server_keytab
	Path to the server keytab.
-kerberos_server_principal
	Kerberos server principal to use, usually of the form HTTP/aurora.example.com@EXAMPLE.COM
-max_flapping_task_delay (default (5, mins))
	Maximum delay between attempts to schedule a flapping task.
-max_leading_duration (default (1, days))
	After leading for this duration, the scheduler should commit suicide.
-max_registration_delay (default (1, mins))
	Max allowable delay to allow the driver to register before aborting
-max_reschedule_task_delay_on_startup (default (30, secs))
	Upper bound of random delay for pending task rescheduling on scheduler startup.
-max_saved_backups (default 48)
	Maximum number of backups to retain before deleting the oldest backups.
-max_schedule_attempts_per_sec (default 40.0)
	Maximum number of scheduling attempts to make per second.
-max_schedule_penalty (default (1, mins))
	Maximum delay between attempts to schedule a PENDING tasks.
-max_status_update_batch_size (default 1000) [must be > 0]
	The maximum number of status updates that can be processed in a batch.
-max_task_event_batch_size (default 300) [must be > 0]
	The maximum number of task state change events that can be processed in a batch.
-max_tasks_per_job (default 4000) [must be > 0]
	Maximum number of allowed tasks in a single job.
-max_tasks_per_schedule_attempt (default 5) [must be > 0]
	The maximum number of tasks to pick in a single scheduling attempt.
-max_update_instance_failures (default 20000) [must be > 0]
	Upper limit on the number of failures allowed during a job update. This helps cap potentially unbounded entries into storage.
-min_offer_hold_time (default (5, mins))
	Minimum amount of time to hold a resource offer before declining.
-native_log_election_retries (default 20)
	The maximum number of attempts to obtain a new log writer.
-native_log_election_timeout (default (15, secs))
	The timeout for a single attempt to obtain a new log writer.
-native_log_file_path
	Path to a file to store the native log data in.  If the parent directory doesnot exist it will be created.
-native_log_quorum_size (default 1)
	The size of the quorum required for all log mutations.
-native_log_read_timeout (default (5, secs))
	The timeout for doing log reads.
-native_log_write_timeout (default (3, secs))
	The timeout for doing log appends and truncations.
-native_log_zk_group_path
	A zookeeper node for use by the native log to track the master coordinator.
-offer_filter_duration (default (5, secs))
	Duration after which we expect Mesos to re-offer unused resources. A short duration improves scheduling performance in smaller clusters, but might lead to resource starvation for other frameworks if you run many frameworks in your cluster.
-offer_hold_jitter_window (default (1, mins))
	Maximum amount of random jitter to add to the offer hold time window.
-offer_reservation_duration (default (3, mins))
	Time to reserve a agent's offers while trying to satisfy a task preempting another.
-populate_discovery_info (default false)
	If true, Aurora populates DiscoveryInfo field of Mesos TaskInfo.
-preemption_delay (default (3, mins))
	Time interval after which a pending task becomes eligible to preempt other tasks
-preemption_slot_hold_time (default (5, mins))
	Time to hold a preemption slot found before it is discarded.
-preemption_slot_search_interval (default (1, mins))
	Time interval between pending task preemption slot searches.
-receive_revocable_resources (default false)
	Allows receiving revocable resource offers from Mesos.
-reconciliation_explicit_batch_interval (default (5, secs))
	Interval between explicit batch reconciliation requests.
-reconciliation_explicit_batch_size (default 1000) [must be > 0]
	Number of tasks in a single batch request sent to Mesos for explicit reconciliation.
-reconciliation_explicit_interval (default (60, mins))
	Interval on which scheduler will ask Mesos for status updates of all non-terminal tasks known to scheduler.
-reconciliation_implicit_interval (default (60, mins))
	Interval on which scheduler will ask Mesos for status updates of all non-terminal tasks known to Mesos.
-reconciliation_initial_delay (default (1, mins))
	Initial amount of time to delay task reconciliation after scheduler start up.
-reconciliation_schedule_spread (default (30, mins))
	Difference between explicit and implicit reconciliation intervals intended to create a non-overlapping task reconciliation schedule.
-require_docker_use_executor (default true)
	If false, Docker tasks may run without an executor (EXPERIMENTAL)
-scheduling_max_batch_size (default 3) [must be > 0]
	The maximum number of scheduling attempts that can be processed in a batch.
-shiro_ini_path
	Path to shiro.ini for authentication and authorization configuration.
-shiro_realm_modules (default [class org.apache.aurora.scheduler.http.api.security.IniShiroRealmModule])
	Guice modules for configuring Shiro Realms.
-sla_non_prod_metrics (default [])
	Metric categories collected for non production tasks.
-sla_prod_metrics (default [JOB_UPTIMES, PLATFORM_UPTIME, MEDIANS])
	Metric categories collected for production tasks.
-sla_stat_refresh_interval (default (1, mins))
	The SLA stat refresh interval.
-slow_query_log_threshold (default (25, ms))
	Log all queries that take at least this long to execute.
-slow_query_log_threshold (default (25, ms))
	Log all queries that take at least this long to execute.
-stat_retention_period (default (1, hrs))
	Time for a stat to be retained in memory before expiring.
-stat_sampling_interval (default (1, secs))
	Statistic value sampling interval.
-thermos_executor_cpu (default 0.25)
	The number of CPU cores to allocate for each instance of the executor.
-thermos_executor_flags
	Extra arguments to be passed to the thermos executor
-thermos_executor_ram (default (128, MB))
	The amount of RAM to allocate for each instance of the executor.
-thermos_executor_resources (default [])
	A comma separated list of additional resources to copy into the sandbox.Note: if thermos_executor_path is not the thermos_executor.pex file itself, this must include it.
-thermos_home_in_sandbox (default false)
	If true, changes HOME to the sandbox before running the executor. This primarily has the effect of causing the executor and runner to extract themselves into the sandbox.
-transient_task_state_timeout (default (5, mins))
	The amount of time after which to treat a task stuck in a transient state as LOST.
-use_beta_db_task_store (default false)
	Whether to use the experimental database-backed task store.
-viz_job_url_prefix (default )
	URL prefix for job container stats.
-zk_chroot_path
	chroot path to use for the ZooKeeper connections
-zk_digest_credentials
	user:password to use when authenticating with ZooKeeper.
-zk_in_proc (default false)
	Launches an embedded zookeeper server for local testing causing -zk_endpoints to be ignored if specified.
-zk_session_timeout (default (4, secs))
	The ZooKeeper session timeout.
-zk_use_curator (default true)
	DEPRECATED: Uses Apache Curator as the zookeeper client; otherwise a copy of Twitter commons/zookeeper (the legacy library) is used.
-------------------------------------------------------------------------
```
