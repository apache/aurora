# Scheduler Configuration Reference

The Aurora scheduler can take a variety of configuration options through command-line arguments.
A list of the available options can be seen by running `aurora-scheduler -help`.

Please refer to the [Operator Configuration Guide](../operations/configuration.md) for details on how
to properly set the most important options.

```
Usage: org.apache.aurora.scheduler.app.SchedulerMain [options]
  Options:
    -allow_container_volumes
      Allow passing in volumes in the job. Enabling this could pose a
      privilege escalation threat.
      Default: false
    -allow_docker_parameters
      Allow to pass docker container parameters in the job.
      Default: false
    -allow_gpu_resource
      Allow jobs to request Mesos GPU resource.
      Default: false
    -allowed_container_types
      Container types that are allowed to be used by jobs.
      Default: [MESOS]
    -allowed_job_environments
      Regular expression describing the environments that are allowed to be
      used by jobs.
      Default: ^(prod|devel|test|staging\d*)$
    -async_slot_stat_update_interval
      Interval on which to try to update open slot stats.
      Default: (1, mins)
    -async_task_stat_update_interval
      Interval on which to try to update resource consumption stats.
      Default: (1, hrs)
    -async_worker_threads
      The number of worker threads to process async task operations with.
      Default: 8
  * -backup_dir
      Directory to store backups under. Will be created if it does not exist.
    -backup_interval
      Minimum interval on which to write a storage backup.
      Default: (1, hrs)
  * -cluster_name
      Name to identify the cluster being served.
    -cron_scheduler_num_threads
      Number of threads to use for the cron scheduler thread pool.
      Default: 10
    -cron_scheduling_max_batch_size
      The maximum number of triggered cron jobs that can be processed in a
      batch.
      Default: 10
    -cron_start_initial_backoff
      Initial backoff delay while waiting for a previous cron run to be
      killed.
      Default: (5, secs)
    -cron_start_max_backoff
      Max backoff delay while waiting for a previous cron run to be killed.
      Default: (1, mins)
    -cron_timezone
      TimeZone to use for cron predictions.
      Default: GMT
    -custom_executor_config
      Path to custom executor settings configuration file.
    -default_docker_parameters
      Default docker parameters for any job that does not explicitly declare
      parameters.
      Default: []
    -dlog_max_entry_size
      Specifies the maximum entry size to append to the log. Larger entries
      will be split across entry Frames.
      Default: (512, KB)
    -dlog_snapshot_interval
      Specifies the frequency at which snapshots of local storage are taken
      and written to the log.
      Default: (1, hrs)
    -enable_cors_for
      List of domains for which CORS support should be enabled.
    -enable_mesos_fetcher
      Allow jobs to pass URIs to the Mesos Fetcher. Note that enabling this
      feature could pose a privilege escalation threat.
      Default: false
    -enable_preemptor
      Enable the preemptor and preemption
      Default: true
    -enable_revocable_cpus
      Treat CPUs as a revocable resource.
      Default: true
    -enable_revocable_ram
      Treat RAM as a revocable resource.
      Default: false
    -enable_update_affinity
      Enable best-effort affinity of task updates.
      Default: false
    -executor_user
      User to start the executor. Defaults to "root". Set this to an
      unprivileged user if the mesos master was started with
      "--no-root_submissions". If set to anything other than "root", the
      executor will ignore the "role" setting for jobs since it can't use
      setuid() anymore. This means that all your jobs will run under the
      specified user and the user has to exist on the Mesos agents.
      Default: root
    -first_schedule_delay
      Initial amount of time to wait before first attempting to schedule a
      PENDING task.
      Default: (1, ms)
    -flapping_task_threshold
      A task that repeatedly runs for less than this time is considered to be
      flapping.
      Default: (5, mins)
    -framework_announce_principal
      When 'framework_authentication_file' flag is set, the FrameworkInfo
      registered with the mesos master will also contain the principal. This
      is necessary if you intend to use mesos authorization via mesos ACLs.
      The default will change in a future release. Changing this value is
      backwards incompatible. For details, see MESOS-703.
      Default: false
    -framework_authentication_file
      Properties file which contains framework credentials to authenticate
      with Mesosmaster. Must contain the properties
      'aurora_authentication_principal' and 'aurora_authentication_secret'.
    -framework_failover_timeout
      Time after which a framework is considered deleted.  SHOULD BE VERY
      HIGH.
      Default: (21, days)
    -framework_name
      Name used to register the Aurora framework with Mesos.
      Default: Aurora
    -global_container_mounts
      A comma separated list of mount points (in host:container form) to mount
      into all (non-mesos) containers.
      Default: []
    -history_max_per_job_threshold
      Maximum number of terminated tasks to retain in a job history.
      Default: 100
    -history_min_retention_threshold
      Minimum guaranteed time for task history retention before any pruning is
      attempted.
      Default: (1, hrs)
    -history_prune_threshold
      Time after which the scheduler will prune terminated task history.
      Default: (2, days)
    -hold_offers_forever
      Hold resource offers indefinitely, disabling automatic offer decline
      settings.
      Default: false
    -host_maintenance_polling_interval
      Interval between polling for pending host maintenance requests.
      Default: (1, mins)
    -hostname
      The hostname to advertise in ZooKeeper instead of the locally-resolved
      hostname.
    -http_authentication_mechanism
      HTTP Authentication mechanism to use.
      Default: NONE
      Possible Values: [NONE, BASIC, NEGOTIATE]
    -http_port
      The port to start an HTTP server on.  Default value will choose a random
      port.
      Default: 0
    -initial_flapping_task_delay
      Initial amount of time to wait before attempting to schedule a flapping
      task.
      Default: (30, secs)
    -initial_schedule_penalty
      Initial amount of time to wait before attempting to schedule a task that
      has failed to schedule.
      Default: (1, secs)
    -initial_task_kill_retry_interval
      When killing a task, retry after this delay if mesos has not responded,
      backing off up to transient_task_state_timeout
      Default: (15, secs)
    -ip
      The ip address to listen. If not set, the scheduler will listen on all
      interfaces.
    -job_update_history_per_job_threshold
      Maximum number of completed job updates to retain in a job update
      history.
      Default: 10
    -job_update_history_pruning_interval
      Job update history pruning interval.
      Default: (15, mins)
    -job_update_history_pruning_threshold
      Time after which the scheduler will prune completed job update history.
      Default: (30, days)
    -kerberos_debug
      Produce additional Kerberos debugging output.
      Default: false
    -kerberos_server_keytab
      Path to the server keytab.
    -kerberos_server_principal
      Kerberos server principal to use, usually of the form
      HTTP/aurora.example.com@EXAMPLE.COM
    -max_flapping_task_delay
      Maximum delay between attempts to schedule a flapping task.
      Default: (5, mins)
    -max_leading_duration
      After leading for this duration, the scheduler should commit suicide.
      Default: (1, days)
    -max_parallel_coordinated_maintenance
      Maximum number of coordinators that can be contacted in parallel.
      Default: 10
    -max_registration_delay
      Max allowable delay to allow the driver to register before aborting
      Default: (1, mins)
    -max_reschedule_task_delay_on_startup
      Upper bound of random delay for pending task rescheduling on scheduler
      startup.
      Default: (30, secs)
    -max_saved_backups
      Maximum number of backups to retain before deleting the oldest backups.
      Default: 48
    -max_schedule_attempts_per_sec
      Maximum number of scheduling attempts to make per second.
      Default: 40.0
    -max_schedule_penalty
      Maximum delay between attempts to schedule a PENDING tasks.
      Default: (1, mins)
    -max_sla_duration_secs
      Maximum duration window for which SLA requirements are to be
      satisfied.This does not apply to jobs that have a CoordinatorSlaPolicy.
      Default: (2, hrs)
    -max_status_update_batch_size
      The maximum number of status updates that can be processed in a batch.
      Default: 1000
    -max_task_event_batch_size
      The maximum number of task state change events that can be processed in
      a batch.
      Default: 300
    -max_tasks_per_job
      Maximum number of allowed tasks in a single job.
      Default: 4000
    -max_tasks_per_schedule_attempt
      The maximum number of tasks to pick in a single scheduling attempt.
      Default: 5
    -max_update_instance_failures
      Upper limit on the number of failures allowed during a job update. This
      helps cap potentially unbounded entries into storage.
      Default: 20000
    -mesos_driver
      Which Mesos Driver to use
      Default: SCHEDULER_DRIVER
      Possible Values: [SCHEDULER_DRIVER, V0_DRIVER, V1_DRIVER]
  * -mesos_master_address
      Address for the mesos master, can be a socket address or zookeeper path.
    -mesos_role
      The Mesos role this framework will register as. The default is to left
      this empty, and the framework will register without any role and only
      receive unreserved resources in offer.
    -min_offer_hold_time
      Minimum amount of time to hold a resource offer before declining.
      Default: (5, mins)
    -min_required_instances_for_sla_check
      Minimum number of instances required for a job to be eligible for SLA
      check. This does not apply to jobs that have a CoordinatorSlaPolicy.
      Default: 20
    -native_log_election_retries
      The maximum number of attempts to obtain a new log writer.
      Default: 20
    -native_log_election_timeout
      The timeout for a single attempt to obtain a new log writer.
      Default: (15, secs)
    -native_log_file_path
      Path to a file to store the native log data in.  If the parent directory
      doesnot exist it will be created.
    -native_log_quorum_size
      The size of the quorum required for all log mutations.
      Default: 1
    -native_log_read_timeout
      The timeout for doing log reads.
      Default: (5, secs)
    -native_log_write_timeout
      The timeout for doing log appends and truncations.
      Default: (3, secs)
    -native_log_zk_group_path
      A zookeeper node for use by the native log to track the master
      coordinator.
    -offer_filter_duration
      Duration after which we expect Mesos to re-offer unused resources. A
      short duration improves scheduling performance in smaller clusters, but
      might lead to resource starvation for other frameworks if you run many
      frameworks in your cluster.
      Default: (5, secs)
    -offer_hold_jitter_window
      Maximum amount of random jitter to add to the offer hold time window.
      Default: (1, mins)
    -offer_order
      Iteration order for offers, to influence task scheduling. Multiple
      orderings will be compounded together. E.g. CPU,MEMORY,RANDOM would sort
      first by cpus offered, then memory and finally would randomize any equal
      offers.
      Default: [RANDOM]
    -offer_reservation_duration
      Time to reserve a agent's offers while trying to satisfy a task
      preempting another.
      Default: (3, mins)
    -offer_set_module
      Custom Guice module to provide a custom OfferSet.
      Default: class org.apache.aurora.scheduler.offers.OfferManagerModule$OfferSetModule
    -offer_static_ban_cache_max_size
      The number of offers to hold in the static ban cache. If no value is
      specified, the cache will grow indefinitely. However, entries will
      expire within 'min_offer_hold_time' + 'offer_hold_jitter_window' of
      being written.
      Default: 9223372036854775807
    -partition_aware
      Enable paritition-aware status updates.
      Default: false
    -populate_discovery_info
      If true, Aurora populates DiscoveryInfo field of Mesos TaskInfo.
      Default: false
    -preemption_delay
      Time interval after which a pending task becomes eligible to preempt
      other tasks
      Default: (3, mins)
    -preemption_reservation_max_batch_size
      The maximum number of reservations for a task group to be made in a
      batch.
      Default: 5
    -preemption_slot_finder_modules
      Guice modules for custom preemption slot searching for pending tasks.
      Default: [class org.apache.aurora.scheduler.preemptor.PendingTaskProcessorModule, class org.apache.aurora.scheduler.preemptor.PreemptionVictimFilterModule]
    -preemption_slot_hold_time
      Time to hold a preemption slot found before it is discarded.
      Default: (5, mins)
    -preemption_slot_search_initial_delay
      Initial amount of time to delay preemption slot searching after
      scheduler start up.
      Default: (3, mins)
    -preemption_slot_search_interval
      Time interval between pending task preemption slot searches.
      Default: (1, mins)
    -receive_revocable_resources
      Allows receiving revocable resource offers from Mesos.
      Default: false
    -reconciliation_explicit_batch_interval
      Interval between explicit batch reconciliation requests.
      Default: (5, secs)
    -reconciliation_explicit_batch_size
      Number of tasks in a single batch request sent to Mesos for explicit
      reconciliation.
      Default: 1000
    -reconciliation_explicit_interval
      Interval on which scheduler will ask Mesos for status updates of
      allnon-terminal tasks known to scheduler.
      Default: (60, mins)
    -reconciliation_implicit_interval
      Interval on which scheduler will ask Mesos for status updates of
      allnon-terminal tasks known to Mesos.
      Default: (60, mins)
    -reconciliation_initial_delay
      Initial amount of time to delay task reconciliation after scheduler
      start up.
      Default: (1, mins)
    -reconciliation_schedule_spread
      Difference between explicit and implicit reconciliation intervals
      intended to create a non-overlapping task reconciliation schedule.
      Default: (30, mins)
    -require_docker_use_executor
      If false, Docker tasks may run without an executor (EXPERIMENTAL)
      Default: true
    -scheduling_max_batch_size
      The maximum number of scheduling attempts that can be processed in a
      batch.
      Default: 3
    -serverset_endpoint_name
      Name of the scheduler endpoint published in ZooKeeper.
      Default: http
  * -serverset_path
      ZooKeeper ServerSet path to register at.
    -shiro_after_auth_filter
      Fully qualified class name of the servlet filter to be applied after the
      shiro auth filters are applied.
    -shiro_credentials_matcher
      The shiro credentials matcher to use (will be constructed by Guice).
      Default: class org.apache.shiro.authc.credential.SimpleCredentialsMatcher
    -shiro_ini_path
      Path to shiro.ini for authentication and authorization configuration.
    -shiro_realm_modules
      Guice modules for configuring Shiro Realms.
      Default: [class org.apache.aurora.scheduler.http.api.security.IniShiroRealmModule]
    -sla_aware_action_max_batch_size
      The maximum number of sla aware update actions that can be processed in
      a batch.
      Default: 300
    -sla_aware_kill_non_prod
      Enables SLA awareness for drain and and update for non-production tasks
      Default: false
    -sla_aware_kill_retry_max_delay
      Maximum amount of time to wait between attempting to perform an
      SLA-Aware kill on a task.
      Default: (5, mins)
    -sla_aware_kill_retry_min_delay
      Minimum amount of time to wait between attempting to perform an
      SLA-Aware kill on a task.
      Default: (1, mins)
    -sla_coordinator_timeout
      Timeout interval for communicating with Coordinator.
      Default: (1, mins)
    -sla_non_prod_metrics
      Metric categories collected for non production tasks.
      Default: []
    -sla_prod_metrics
      Metric categories collected for production tasks.
      Default: [JOB_UPTIMES, PLATFORM_UPTIME, MEDIANS]
    -sla_stat_refresh_interval
      The SLA stat refresh interval.
      Default: (1, mins)
    -stat_retention_period
      Time for a stat to be retained in memory before expiring.
      Default: (1, hrs)
    -stat_sampling_interval
      Statistic value sampling interval.
      Default: (1, secs)
    -task_assigner_modules
      Guice modules for customizing task assignment.
      Default: [class org.apache.aurora.scheduler.scheduling.TaskAssignerImplModule]
    -thermos_executor_cpu
      The number of CPU cores to allocate for each instance of the executor.
      Default: 0.25
    -thermos_executor_flags
      Extra arguments to be passed to the thermos executor
    -thermos_executor_path
      Path to the thermos executor entry point.
    -thermos_executor_ram
      The amount of RAM to allocate for each instance of the executor.
      Default: (128, MB)
    -thermos_executor_resources
      A comma separated list of additional resources to copy into the
      sandbox.Note: if thermos_executor_path is not the thermos_executor.pex
      file itself, this must include it.
      Default: []
    -thermos_home_in_sandbox
      If true, changes HOME to the sandbox before running the executor. This
      primarily has the effect of causing the executor and runner to extract
      themselves into the sandbox.
      Default: false
    -thrift_method_interceptor_modules
      Custom Guice module(s) to provide additional Thrift method interceptors.
      Default: []
    -tier_config
      Configuration file defining supported task tiers, task traits and
      behaviors.
    -transient_task_state_timeout
      The amount of time after which to treat a task stuck in a transient
      state as LOST.
      Default: (5, mins)
    -unavailability_threshold
      Threshold time, when running tasks should be drained from a host, before
      a host becomes unavailable. Should be greater than min_offer_hold_time +
      offer_hold_jitter_window.
      Default: (6, mins)
    -update_affinity_reservation_hold_time
      How long to wait for a reserved agent to reoffer freed up resources.
      Default: (3, mins)
    -viz_job_url_prefix
      URL prefix for job container stats.
      Default: <empty string>
    -webhook_config
      Path to webhook configuration file.
    -zk_chroot_path
      chroot path to use for the ZooKeeper connections
    -zk_connection_timeout
      The ZooKeeper connection timeout.
      Default: (10, secs)
    -zk_digest_credentials
      user:password to use when authenticating with ZooKeeper.
  * -zk_endpoints
      Endpoint specification for the ZooKeeper servers.
    -zk_in_proc
      Launches an embedded zookeeper server for local testing causing
      -zk_endpoints to be ignored if specified.
      Default: false
    -zk_session_timeout
      The ZooKeeper session timeout.
      Default: (15, secs)
```
