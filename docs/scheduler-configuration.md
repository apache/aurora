# Scheduler Configuration

The Aurora scheduler can take a variety of configuration options through command-line arguments.
A list of the available options can be seen by running `aurora-scheduler -help`.

Please refer to [Deploying the Aurora Scheduler](deploying-aurora-scheduler.md) for details on how
to properly set the most important options.

```
$ aurora-scheduler -help
-------------------------------------------------------------------------
-h or -help to print this help message

Required flags:
-backup_dir [not null]
	Directory to store backups under. Will be created if it does not exist.
	(org.apache.aurora.scheduler.storage.backup.BackupModule.backup_dir)
-cluster_name [not null]
	Name to identify the cluster being served.
	(org.apache.aurora.scheduler.app.SchedulerMain.cluster_name)
-framework_authentication_file
	Properties file which contains framework credentials to authenticate with Mesosmaster. Must contain the properties 'aurora_authentication_principal' and 'aurora_authentication_secret'.
	(org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule.framework_authentication_file)
-mesos_master_address [not null]
	Address for the mesos master, can be a socket address or zookeeper path.
	(org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule.mesos_master_address)
-mesos_role
	The Mesos role this framework will register as. The default is to left this empty, and the framework will register without any role and only receive unreserved resources in offer.
	(org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule.mesos_role)
-serverset_path [not null, must be non-empty]
	ZooKeeper ServerSet path to register at.
	(org.apache.aurora.scheduler.app.SchedulerMain.serverset_path)
-shiro_after_auth_filter
	Fully qualified class name of the servlet filter to be applied after the shiro auth filters are applied.
	(org.apache.aurora.scheduler.http.api.security.HttpSecurityModule.shiro_after_auth_filter)
-thermos_executor_path
	Path to the thermos executor entry point.
	(org.apache.aurora.scheduler.configuration.executor.ExecutorModule.thermos_executor_path)
-tier_config [file must be readable]
	Configuration file defining supported task tiers, task traits and behaviors.
	(org.apache.aurora.scheduler.SchedulerModule.tier_config)
-zk_digest_credentials
	user:password to use when authenticating with ZooKeeper.
	(org.apache.aurora.scheduler.zookeeper.guice.client.flagged.FlaggedClientConfig.zk_digest_credentials)
-zk_endpoints [must have at least 1 item]
	Endpoint specification for the ZooKeeper servers.
	(org.apache.aurora.scheduler.zookeeper.guice.client.flagged.FlaggedClientConfig.zk_endpoints)

Optional flags:
-allow_docker_parameters=false
	Allow to pass docker container parameters in the job.
	(org.apache.aurora.scheduler.app.AppModule.allow_docker_parameters)
-allowed_container_types=[MESOS]
	Container types that are allowed to be used by jobs.
	(org.apache.aurora.scheduler.app.AppModule.allowed_container_types)
-async_slot_stat_update_interval=(1, mins)
	Interval on which to try to update open slot stats.
	(org.apache.aurora.scheduler.stats.AsyncStatsModule.async_slot_stat_update_interval)
-async_task_stat_update_interval=(1, hrs)
	Interval on which to try to update resource consumption stats.
	(org.apache.aurora.scheduler.stats.AsyncStatsModule.async_task_stat_update_interval)
-async_worker_threads=8
	The number of worker threads to process async task operations with.
	(org.apache.aurora.scheduler.async.AsyncModule.async_worker_threads)
-backup_interval=(1, hrs)
	Minimum interval on which to write a storage backup.
	(org.apache.aurora.scheduler.storage.backup.BackupModule.backup_interval)
-cron_scheduler_num_threads=100
	Number of threads to use for the cron scheduler thread pool.
	(org.apache.aurora.scheduler.cron.quartz.CronModule.cron_scheduler_num_threads)
-cron_start_initial_backoff=(1, secs)
	Initial backoff delay while waiting for a previous cron run to be killed.
	(org.apache.aurora.scheduler.cron.quartz.CronModule.cron_start_initial_backoff)
-cron_start_max_backoff=(1, mins)
	Max backoff delay while waiting for a previous cron run to be killed.
	(org.apache.aurora.scheduler.cron.quartz.CronModule.cron_start_max_backoff)
-cron_timezone=GMT
	TimeZone to use for cron predictions.
	(org.apache.aurora.scheduler.cron.quartz.CronModule.cron_timezone)
-custom_executor_config [file must exist, file must be readable]
	Path to custom executor settings configuration file.
	(org.apache.aurora.scheduler.configuration.executor.ExecutorModule.custom_executor_config)
-db_lock_timeout=(1, mins)
	H2 table lock timeout
	(org.apache.aurora.scheduler.storage.db.DbModule.db_lock_timeout)
-db_row_gc_interval=(2, hrs)
	Interval on which to scan the database for unused row references.
	(org.apache.aurora.scheduler.storage.db.DbModule.db_row_gc_interval)
-default_docker_parameters={}
	Default docker parameters for any job that does not explicitly declare parameters.
	(org.apache.aurora.scheduler.app.AppModule.default_docker_parameters)
-dlog_max_entry_size=(512, KB)
	Specifies the maximum entry size to append to the log. Larger entries will be split across entry Frames.
	(org.apache.aurora.scheduler.storage.log.LogStorageModule.dlog_max_entry_size)
-dlog_shutdown_grace_period=(2, secs)
	Specifies the maximum time to wait for scheduled checkpoint and snapshot actions to complete before forcibly shutting down.
	(org.apache.aurora.scheduler.storage.log.LogStorageModule.dlog_shutdown_grace_period)
-dlog_snapshot_interval=(1, hrs)
	Specifies the frequency at which snapshots of local storage are taken and written to the log.
	(org.apache.aurora.scheduler.storage.log.LogStorageModule.dlog_snapshot_interval)
-enable_cors_for
	List of domains for which CORS support should be enabled.
	(org.apache.aurora.scheduler.http.api.ApiModule.enable_cors_for)
-enable_h2_console=false
	Enable H2 DB management console.
	(org.apache.aurora.scheduler.http.H2ConsoleModule.enable_h2_console)
-enable_preemptor=true
	Enable the preemptor and preemption
	(org.apache.aurora.scheduler.preemptor.PreemptorModule.enable_preemptor)
-executor_user=root
	User to start the executor. Defaults to "root". Set this to an unprivileged user if the mesos master was started with "--no-root_submissions". If set to anything other than "root", the executor will ignore the "role" setting for jobs since it can't use setuid() anymore. This means that all your jobs will run under the specified user and the user has to exist on the mesos slaves.
	(org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule.executor_user)
-first_schedule_delay=(1, ms)
	Initial amount of time to wait before first attempting to schedule a PENDING task.
	(org.apache.aurora.scheduler.scheduling.SchedulingModule.first_schedule_delay)
-flapping_task_threshold=(5, mins)
	A task that repeatedly runs for less than this time is considered to be flapping.
	(org.apache.aurora.scheduler.scheduling.SchedulingModule.flapping_task_threshold)
-framework_announce_principal=false
	When 'framework_authentication_file' flag is set, the FrameworkInfo registered with the mesos master will also contain the principal. This is necessary if you intend to use mesos authorization via mesos ACLs. The default will change in a future release.
	(org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule.framework_announce_principal)
-framework_failover_timeout=(21, days)
	Time after which a framework is considered deleted.  SHOULD BE VERY HIGH.
	(org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule.framework_failover_timeout)
-global_container_mounts=[]
	A comma seperated list of mount points (in host:container form) to mount into all (non-mesos) containers.
	(org.apache.aurora.scheduler.configuration.executor.ExecutorModule.global_container_mounts)
-history_max_per_job_threshold=100
	Maximum number of terminated tasks to retain in a job history.
	(org.apache.aurora.scheduler.pruning.PruningModule.history_max_per_job_threshold)
-history_min_retention_threshold=(1, hrs)
	Minimum guaranteed time for task history retention before any pruning is attempted.
	(org.apache.aurora.scheduler.pruning.PruningModule.history_min_retention_threshold)
-history_prune_threshold=(2, days)
	Time after which the scheduler will prune terminated task history.
	(org.apache.aurora.scheduler.pruning.PruningModule.history_prune_threshold)
-hostname
	The hostname to advertise in ZooKeeper instead of the locally-resolved hostname.
	(org.apache.aurora.scheduler.http.JettyServerModule.hostname)
-http_authentication_mechanism=NONE
	HTTP Authentication mechanism to use.
	(org.apache.aurora.scheduler.http.api.security.HttpSecurityModule.http_authentication_mechanism)
-http_port=0
	The port to start an HTTP server on.  Default value will choose a random port.
	(org.apache.aurora.scheduler.http.JettyServerModule.http_port)
-initial_flapping_task_delay=(30, secs)
	Initial amount of time to wait before attempting to schedule a flapping task.
	(org.apache.aurora.scheduler.scheduling.SchedulingModule.initial_flapping_task_delay)
-initial_schedule_penalty=(1, secs)
	Initial amount of time to wait before attempting to schedule a task that has failed to schedule.
	(org.apache.aurora.scheduler.scheduling.SchedulingModule.initial_schedule_penalty)
-initial_task_kill_retry_interval=(5, secs)
	When killing a task, retry after this delay if mesos has not responded, backing off up to transient_task_state_timeout
	(org.apache.aurora.scheduler.reconciliation.ReconciliationModule.initial_task_kill_retry_interval)
-job_update_history_per_job_threshold=10
	Maximum number of completed job updates to retain in a job update history.
	(org.apache.aurora.scheduler.pruning.PruningModule.job_update_history_per_job_threshold)
-job_update_history_pruning_interval=(15, mins)
	Job update history pruning interval.
	(org.apache.aurora.scheduler.pruning.PruningModule.job_update_history_pruning_interval)
-job_update_history_pruning_threshold=(30, days)
	Time after which the scheduler will prune completed job update history.
	(org.apache.aurora.scheduler.pruning.PruningModule.job_update_history_pruning_threshold)
-kerberos_debug=false
	Produce additional Kerberos debugging output.
	(org.apache.aurora.scheduler.http.api.security.Kerberos5ShiroRealmModule.kerberos_debug)
-kerberos_server_keytab
	Path to the server keytab.
	(org.apache.aurora.scheduler.http.api.security.Kerberos5ShiroRealmModule.kerberos_server_keytab)
-kerberos_server_principal
	Kerberos server principal to use, usually of the form HTTP/aurora.example.com@EXAMPLE.COM
	(org.apache.aurora.scheduler.http.api.security.Kerberos5ShiroRealmModule.kerberos_server_principal)
-max_flapping_task_delay=(5, mins)
	Maximum delay between attempts to schedule a flapping task.
	(org.apache.aurora.scheduler.scheduling.SchedulingModule.max_flapping_task_delay)
-max_leading_duration=(1, days)
	After leading for this duration, the scheduler should commit suicide.
	(org.apache.aurora.scheduler.SchedulerModule.max_leading_duration)
-max_registration_delay=(1, mins)
	Max allowable delay to allow the driver to register before aborting
	(org.apache.aurora.scheduler.SchedulerModule.max_registration_delay)
-max_reschedule_task_delay_on_startup=(30, secs)
	Upper bound of random delay for pending task rescheduling on scheduler startup.
	(org.apache.aurora.scheduler.scheduling.SchedulingModule.max_reschedule_task_delay_on_startup)
-max_saved_backups=48
	Maximum number of backups to retain before deleting the oldest backups.
	(org.apache.aurora.scheduler.storage.backup.BackupModule.max_saved_backups)
-max_schedule_attempts_per_sec=40.0
	Maximum number of scheduling attempts to make per second.
	(org.apache.aurora.scheduler.scheduling.SchedulingModule.max_schedule_attempts_per_sec)
-max_schedule_penalty=(1, mins)
	Maximum delay between attempts to schedule a PENDING tasks.
	(org.apache.aurora.scheduler.scheduling.SchedulingModule.max_schedule_penalty)
-max_status_update_batch_size=1000 [must be > 0]
	The maximum number of status updates that can be processed in a batch.
	(org.apache.aurora.scheduler.SchedulerModule.max_status_update_batch_size)
-max_tasks_per_job=4000 [must be > 0]
	Maximum number of allowed tasks in a single job.
	(org.apache.aurora.scheduler.app.AppModule.max_tasks_per_job)
-max_update_instance_failures=20000 [must be > 0]
	Upper limit on the number of failures allowed during a job update. This helps cap potentially unbounded entries into storage.
	(org.apache.aurora.scheduler.app.AppModule.max_update_instance_failures)
-min_offer_hold_time=(5, mins)
	Minimum amount of time to hold a resource offer before declining.
	(org.apache.aurora.scheduler.offers.OffersModule.min_offer_hold_time)
-native_log_election_retries=20
	The maximum number of attempts to obtain a new log writer.
	(org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule.native_log_election_retries)
-native_log_election_timeout=(15, secs)
	The timeout for a single attempt to obtain a new log writer.
	(org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule.native_log_election_timeout)
-native_log_file_path
	Path to a file to store the native log data in.  If the parent directory doesnot exist it will be created.
	(org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule.native_log_file_path)
-native_log_quorum_size=1
	The size of the quorum required for all log mutations.
	(org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule.native_log_quorum_size)
-native_log_read_timeout=(5, secs)
	The timeout for doing log reads.
	(org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule.native_log_read_timeout)
-native_log_write_timeout=(3, secs)
	The timeout for doing log appends and truncations.
	(org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule.native_log_write_timeout)
-native_log_zk_group_path
	A zookeeper node for use by the native log to track the master coordinator.
	(org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule.native_log_zk_group_path)
-offer_hold_jitter_window=(1, mins)
	Maximum amount of random jitter to add to the offer hold time window.
	(org.apache.aurora.scheduler.offers.OffersModule.offer_hold_jitter_window)
-offer_reservation_duration=(3, mins)
	Time to reserve a slave's offers while trying to satisfy a task preempting another.
	(org.apache.aurora.scheduler.scheduling.SchedulingModule.offer_reservation_duration)
-preemption_delay=(3, mins)
	Time interval after which a pending task becomes eligible to preempt other tasks
	(org.apache.aurora.scheduler.preemptor.PreemptorModule.preemption_delay)
-preemption_slot_hold_time=(5, mins)
	Time to hold a preemption slot found before it is discarded.
	(org.apache.aurora.scheduler.preemptor.PreemptorModule.preemption_slot_hold_time)
-preemption_slot_search_interval=(1, mins)
	Time interval between pending task preemption slot searches.
	(org.apache.aurora.scheduler.preemptor.PreemptorModule.preemption_slot_search_interval)
-receive_revocable_resources=false
	Allows receiving revocable resource offers from Mesos.
	(org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule.receive_revocable_resources)
-reconciliation_explicit_interval=(60, mins)
	Interval on which scheduler will ask Mesos for status updates of all non-terminal tasks known to scheduler.
	(org.apache.aurora.scheduler.reconciliation.ReconciliationModule.reconciliation_explicit_interval)
-reconciliation_implicit_interval=(60, mins)
	Interval on which scheduler will ask Mesos for status updates of all non-terminal tasks known to Mesos.
	(org.apache.aurora.scheduler.reconciliation.ReconciliationModule.reconciliation_implicit_interval)
-reconciliation_initial_delay=(1, mins)
	Initial amount of time to delay task reconciliation after scheduler start up.
	(org.apache.aurora.scheduler.reconciliation.ReconciliationModule.reconciliation_initial_delay)
-reconciliation_schedule_spread=(30, mins)
	Difference between explicit and implicit reconciliation intervals intended to create a non-overlapping task reconciliation schedule.
	(org.apache.aurora.scheduler.reconciliation.ReconciliationModule.reconciliation_schedule_spread)
-shiro_ini_path
	Path to shiro.ini for authentication and authorization configuration.
	(org.apache.aurora.scheduler.http.api.security.IniShiroRealmModule.shiro_ini_path)
-shiro_realm_modules=[org.apache.aurora.scheduler.app.MoreModules$1@30c15d8b]
	Guice modules for configuring Shiro Realms.
	(org.apache.aurora.scheduler.http.api.security.HttpSecurityModule.shiro_realm_modules)
-sla_non_prod_metrics=[]
	Metric categories collected for non production tasks.
	(org.apache.aurora.scheduler.sla.SlaModule.sla_non_prod_metrics)
-sla_prod_metrics=[JOB_UPTIMES, PLATFORM_UPTIME, MEDIANS]
	Metric categories collected for production tasks.
	(org.apache.aurora.scheduler.sla.SlaModule.sla_prod_metrics)
-sla_stat_refresh_interval=(1, mins)
	The SLA stat refresh interval.
	(org.apache.aurora.scheduler.sla.SlaModule.sla_stat_refresh_interval)
-slow_query_log_threshold=(25, ms)
	Log all queries that take at least this long to execute.
	(org.apache.aurora.scheduler.storage.mem.InMemStoresModule.slow_query_log_threshold)
-slow_query_log_threshold=(25, ms)
	Log all queries that take at least this long to execute.
	(org.apache.aurora.scheduler.storage.db.DbModule.slow_query_log_threshold)
-stat_retention_period=(1, hrs)
	Time for a stat to be retained in memory before expiring.
	(org.apache.aurora.scheduler.stats.StatsModule.stat_retention_period)
-stat_sampling_interval=(1, secs)
	Statistic value sampling interval.
	(org.apache.aurora.scheduler.stats.StatsModule.stat_sampling_interval)
-thermos_executor_cpu=0.25
	The number of CPU cores to allocate for each instance of the executor.
	(org.apache.aurora.scheduler.configuration.executor.ExecutorModule.thermos_executor_cpu)
-thermos_executor_flags
	Extra arguments to be passed to the thermos executor
	(org.apache.aurora.scheduler.configuration.executor.ExecutorModule.thermos_executor_flags)
-thermos_executor_ram=(128, MB)
	The amount of RAM to allocate for each instance of the executor.
	(org.apache.aurora.scheduler.configuration.executor.ExecutorModule.thermos_executor_ram)
-thermos_executor_resources=[]
	A comma seperated list of additional resources to copy into the sandbox.Note: if thermos_executor_path is not the thermos_executor.pex file itself, this must include it.
	(org.apache.aurora.scheduler.configuration.executor.ExecutorModule.thermos_executor_resources)
-thermos_observer_root=/var/run/thermos
	Path to the thermos observer root (by default /var/run/thermos.)
	(org.apache.aurora.scheduler.configuration.executor.ExecutorModule.thermos_observer_root)
-transient_task_state_timeout=(5, mins)
	The amount of time after which to treat a task stuck in a transient state as LOST.
	(org.apache.aurora.scheduler.reconciliation.ReconciliationModule.transient_task_state_timeout)
-use_beta_db_task_store=false
	Whether to use the experimental database-backed task store.
	(org.apache.aurora.scheduler.storage.db.DbModule.use_beta_db_task_store)
-viz_job_url_prefix=
	URL prefix for job container stats.
	(org.apache.aurora.scheduler.app.SchedulerMain.viz_job_url_prefix)
-zk_chroot_path
	chroot path to use for the ZooKeeper connections
	(org.apache.aurora.scheduler.zookeeper.guice.client.flagged.FlaggedClientConfig.zk_chroot_path)
-zk_in_proc=false
	Launches an embedded zookeeper server for local testing causing -zk_endpoints to be ignored if specified.
	(org.apache.aurora.scheduler.zookeeper.guice.client.flagged.FlaggedClientConfig.zk_in_proc)
-zk_session_timeout=(4, secs)
	The ZooKeeper session timeout.
	(org.apache.aurora.scheduler.zookeeper.guice.client.flagged.FlaggedClientConfig.zk_session_timeout)
-------------------------------------------------------------------------
```
