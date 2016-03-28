# Recovering from a Scheduler Backup

**Be sure to read the entire page before attempting to restore from a backup, as it may have
unintended consequences.**

# Summary

The restoration procedure replaces the existing (possibly corrupted) Mesos replicated log with an
earlier, backed up, version and requires all schedulers to be taken down temporarily while
restoring. Once completed, the scheduler state resets to what it was when the backup was created.
This means any jobs/tasks created or updated after the backup are unknown to the scheduler and will
be killed shortly after the cluster restarts. All other tasks continue operating as normal.

Usually, it is a bad idea to restore a backup that is not extremely recent (i.e. older than a few
hours). This is because the scheduler will expect the cluster to look exactly as the backup does,
so any tasks that have been rescheduled since the backup was taken will be killed.

Instructions below have been verified in [Vagrant environment](../getting-started/vagrant.md) and with minor
syntax/path changes should be applicable to any Aurora cluster.

# Preparation

Follow these steps to prepare the cluster for restoring from a backup:

* Stop all scheduler instances

* Consider blocking external traffic on a port defined in `-http_port` for all schedulers to
prevent users from interacting with the scheduler during the restoration process. This will help
troubleshooting by reducing the scheduler log noise and prevent users from making changes that will
be erased after the backup snapshot is restored.

* Configure `aurora_admin` access to run all commands listed in
  [Restore from backup](#restore-from-backup) section locally on the leading scheduler:
  * Make sure the [clusters.json](../reference/client-cluster-configuration.md) file configured to
    access scheduler directly. Set `scheduler_uri` setting and remove `zk`. Since leader can get
    re-elected during the restore steps, consider doing it on all scheduler replicas.
  * Depending on your particular security approach you will need to either turn off scheduler
    authorization by removing scheduler `-http_authentication_mechanism` flag or make sure the
    direct scheduler access is properly authorized. E.g.: in case of Kerberos you will need to make
    a `/etc/hosts` file change to match your local IP to the scheduler URL configured in keytabs:

        <local_ip> <scheduler_domain_in_keytabs>

* Next steps are required to put scheduler into a partially disabled state where it would still be
able to accept storage recovery requests but unable to schedule or change task states. This may be
accomplished by updating the following scheduler configuration options:
  * Set `-mesos_master_address` to a non-existent zk address. This will prevent scheduler from
    registering with Mesos. E.g.: `-mesos_master_address=zk://localhost:1111/mesos/master`
  * `-max_registration_delay` - set to sufficiently long interval to prevent registration timeout
    and as a result scheduler suicide. E.g: `-max_registration_delay=360mins`
  * Make sure `-reconciliation_initial_delay` option is set high enough (e.g.: `365days`) to
    prevent accidental task GC. This is important as scheduler will attempt to reconcile the cluster
    state and will kill all tasks when restarted with an empty Mesos replicated log.

* Restart all schedulers

# Cleanup and re-initialize Mesos replicated log

Get rid of the corrupted files and re-initialize Mesos replicated log:

* Stop schedulers
* Delete all files under `-native_log_file_path` on all schedulers
* Initialize Mesos replica's log file: `sudo mesos-log initialize --path=<-native_log_file_path>`
* Start schedulers

# Restore from backup

At this point the scheduler is ready to rehydrate from the backup:

* Identify the leading scheduler by:
  * examining the `scheduler_lifecycle_LEADER_AWAITING_REGISTRATION` metric at the scheduler
    `/vars` endpoint. Leader will have 1. All other replicas - 0.
  * examining scheduler logs
  * or examining Zookeeper registration under the path defined by `-zk_endpoints`
    and `-serverset_path`

* Locate the desired backup file, copy it to the leading scheduler's `-backup_dir` folder and stage
recovery by running the following command on a leader
`aurora_admin scheduler_stage_recovery --bypass-leader-redirect <cluster> scheduler-backup-<yyyy-MM-dd-HH-mm>`

* At this point, the recovery snapshot is staged and available for manual verification/modification
via `aurora_admin scheduler_print_recovery_tasks --bypass-leader-redirect` and
`scheduler_delete_recovery_tasks --bypass-leader-redirect` commands.
See `aurora_admin help <command>` for usage details.

* Commit recovery. This instructs the scheduler to overwrite the existing Mesos replicated log with
the provided backup snapshot and initiate a mandatory failover
`aurora_admin scheduler_commit_recovery --bypass-leader-redirect  <cluster>`

# Cleanup
Undo any modification done during [Preparation](#preparation) sequence.
