// Copyright 2013 Twitter, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
namespace java org.apache.aurora.gen
namespace py gen.apache.aurora

// Thrift interface definition for the aurora scheduler.

enum ResponseCode {
  INVALID_REQUEST = 0,
  OK              = 1,
  ERROR           = 2,
  WARNING         = 3,
  AUTH_FAILED     = 4,
  LOCK_ERROR      = 5  // Raised when a Lock-protected operation failed due to lock validation.
}

struct APIVersion {
  1: required i32 major
}

// Scheduler Thrift API Version. Increment this when breaking backwards compatibility.
const APIVersion CURRENT_API_VERSION = {'major': 3}

// Aurora executor framework name.
const string AURORA_EXECUTOR_NAME = 'AuroraExecutor'

struct Identity {
  1: string role
  2: string user
}

struct SessionKey {
  4: optional string mechanism  // The name of the authentication mechanism, which
                                // instructs the server how to interpret the data field.
  5: optional binary data       // A blob of data that the server may use for authentication.
}

// Quota entry associated with a role.
// In order for a user to launch a production job, they must have sufficient quota.
struct Quota {
  1: double numCpus  // Number of CPU cores allotted.
  2: i64 ramMb       // Megabytes of RAM allotted.
  3: i64 diskMb      // Megabytes of disk space allotted.
}

// A single host attribute.
struct Attribute {
  1: string name
  2: set<string> values
}

enum MaintenanceMode {
  NONE      = 1,
  SCHEDULED = 2,
  DRAINING  = 3,
  DRAINED   = 4
}

// The attributes assigned to a host.
struct HostAttributes {
  1: string          host
  2: set<Attribute>  attributes  // TODO(William Farner): Change this to map<String, Attribute>
                                 // and remove name field from Attribute.
  3: optional MaintenanceMode mode
  4: optional string slaveId
}

// A constraint that specifies an explicit set of values, at least one of which must be present
// on a host for a task to be scheduled there.
struct ValueConstraint {
  1: bool negated       // If true, treat this as a 'not' - to avoid specific values.
  2: set<string> values
}

// A constraint the specifies the maximum number of active tasks on a host with a matching
// attribute that may be scheduled simultaneously.
struct LimitConstraint {
  1: i32 limit
}

// Types of constraints that may be applied to a task.
union TaskConstraint {
  1: ValueConstraint value
  2: LimitConstraint limit
}

// A constraint that defines whether a task may be scheduled on a host.
struct Constraint {
  1: string name
  2: TaskConstraint constraint
}

struct Package {
  1: string role
  2: string name
  3: i32 version
}

// A unique identifier for a Job.
struct JobKey {
  1: string role        // Mesos role (Unix service account), for example "mesos"
  2: string environment // Environment, for example "devel"
  3: string name        // Name, for example "labrat"
}

// A unique lock key.
union LockKey {
  1: JobKey job
}

// A generic lock struct to facilitate context specific resource/operation serialization.
struct Lock {
  1: LockKey key                // ID of the lock - unique per storage
  2: string token               // UUID - facilitating soft lock authorization
  3: string user                // Lock owner
  4: i64 timestampMs            // Lock creation timestamp in milliseconds
  5: optional string message    // Optional message to record with the lock
}

// Defines the required lock validation level.
enum LockValidation {
  CHECKED   = 0   // The lock must be valid in order to be released.
  UNCHECKED = 1   // The lock will be released without validation (aka “force release”).
}

// A unique identifier for the active task within a job.
struct InstanceKey {
  1: JobKey jobKey   // Key identifying the job.
  2: i32 instanceId  // Unique instance ID for the active task in a job.
}

struct ExecutorConfig {
  1: string name    // Name identifying the Executor.
  2: string data    // Executor configuration data.
}

// Description of the tasks contained within a job.
struct TaskConfig {
                                             // TODO(William Farner): Store a JobKey instead.
 17: Identity owner                          // contains the role component of JobKey
 26: string environment                      // contains the environment component of JobKey
  3: string jobName                          // contains the name component of JobKey
  7: bool isService
  8: double numCpus
  9: i64 ramMb
 10: i64 diskMb
 11: i32 priority
 13: i32 maxTaskFailures
 18: optional bool production                // Whether this is a production task, which can preempt
                                             // non-production tasks.
 20: set<Constraint> constraints
 21: set<string> requestedPorts              // a list of named ports this task requests
 22: optional map<string, string> taskLinks  // Custom links to include when displaying this task
                                             // on the scheduler dashboard.  Keys are anchor text,
                                             // values are URLs.
                                             // Wildcards are supported for dynamic link
                                             // crafting based on host, ports, instance, etc.
 23: optional string contactEmail
 24: optional set<Package> packages          // Used only to display package information in the
                                             // scheduler UI.
 25: optional ExecutorConfig executorConfig  // Executor configuration
}

// Defines the policy for launching a new cron job when one is already running.
enum CronCollisionPolicy {
  KILL_EXISTING = 0,  // Kills the existing job with the colliding name, and runs the new cron job.
  CANCEL_NEW    = 1,  // Cancels execution of the new job, leaving the running job in tact.
  RUN_OVERLAP   = 2   // Runs both jobs, effectively adding more tasks to the existing job.
}

// Description of an aurora job.
// A list of task descriptions must be specified, which may be
// heterogeneous.  One task will be scheduled for each task description.
// The tuple (name, environment, owner.role) must be unique.
struct JobConfiguration {
  9: JobKey key                               // Key for this job. If not specified
                                              // name, owner.role, and a reasonable default
                                              // environment are used to construct it server-side.
                                              // TODO(William Farner): Deprecate Identity and
                                              // use JobKey instead (MESOS-4006).
  7: Identity owner                           // Owner of this job.
  4: string cronSchedule                      // If present, the job will be handled as a cron job
                                              // with this crontab-syntax schedule.
  5: CronCollisionPolicy cronCollisionPolicy  // Collision policy to use when handling overlapping
                                              // cron runs.  Default is KILL_EXISTING.
  6: TaskConfig taskConfig                    // Task configuration for this job.
  8: i32 instanceCount                        // The number of instances in the job.  Generated
                                              // instance IDs for tasks will be in the range
                                              // [0, instances).
}

// A request to add the following instances to an existing job. Used by addInstances.
struct AddInstancesConfig {
  1: JobKey key
  2: TaskConfig taskConfig
  3: set<i32> instanceIds
}

struct PopulateJobResult {
  1: set<TaskConfig> populated
}

struct GetQuotaResult {
  1: Quota quota
}

// Wraps return results for the acquireLock API.
struct AcquireLockResult {
  1: Lock lock			// Acquired Lock instance.
}

// States that a task may be in.
enum ScheduleStatus {
  // Initial state for a task.  A task will remain in this state until it has been persisted.
  INIT             = 11,
  // The task will be rescheduled, but is being throttled for restarting too frequently.
  THROTTLED        = 16,
  // Task is awaiting assignment to a slave.
  PENDING          = 0,
  // Task has been assigned to a slave.
  ASSIGNED         = 9,
  // Slave has acknowledged receipt of task and is bootstrapping the task.
  STARTING         = 1,
  // The task is running on the slave.
  RUNNING          = 2,
  // The task terminated with an exit code of zero.
  FINISHED         = 3,
  // The task is being preempted by another task.
  PREEMPTING       = 13,
  // The task is being restarted in response to a user request.
  RESTARTING       = 12,
  // The task terminated with a non-zero exit code.
  FAILED           = 4,
  // Execution of the task was terminated by the system.
  KILLED           = 5,
  // The task is being forcibly killed.
  KILLING          = 6,
  // A fault in the task environment has caused the system to believe the task no longer exists.
  // This can happen, for example, when a slave process disappears.
  LOST             = 7,
  // The task is unknown to one end of the system.  This is used to reconcile state when the
  // scheduler believes a task to exist in a location that stops reporting it, or vice versa.
  UNKNOWN          = 10
}

// States that a task may be in while still considered active.
const set<ScheduleStatus> ACTIVE_STATES = [ScheduleStatus.PENDING,
                                           ScheduleStatus.ASSIGNED,
                                           ScheduleStatus.STARTING,
                                           ScheduleStatus.RUNNING,
                                           ScheduleStatus.KILLING,
                                           ScheduleStatus.RESTARTING,
                                           ScheduleStatus.PREEMPTING]

// States that a task may be in while in an active sandbox.
const set<ScheduleStatus> LIVE_STATES = [ScheduleStatus.RUNNING,
                                         ScheduleStatus.KILLING,
                                         ScheduleStatus.RESTARTING,
                                         ScheduleStatus.PREEMPTING]

// States a completed task may be in.
const set<ScheduleStatus> TERMINAL_STATES = [ScheduleStatus.FAILED,
                                             ScheduleStatus.FINISHED,
                                             ScheduleStatus.KILLED,
                                             ScheduleStatus.LOST]

// Environment assigned to a job when unspecified
const string DEFAULT_ENVIRONMENT = "devel"

// Regular expressions for matching valid identifiers for job path components. All expressions
// below should accept and reject the same set of inputs.
const string GOOD_IDENTIFIER_PATTERN = "^[\\w\\-\\.]+$"
// JVM: Use with java.util.regex.Pattern#compile
const string GOOD_IDENTIFIER_PATTERN_JVM = GOOD_IDENTIFIER_PATTERN
// Python: Use with re.compile
const string GOOD_IDENTIFIER_PATTERN_PYTHON = GOOD_IDENTIFIER_PATTERN

// Event marking a state transition within a task's lifecycle.
struct TaskEvent {
  // Epoch timestamp in milliseconds.
  1: i64 timestamp
  // New status of the task.
  2: ScheduleStatus status
  // Audit message that explains why a transition occurred.
  3: optional string message
  // Hostname of the scheduler machine that performed the event.
  4: optional string scheduler
}

// A task assignment that is provided to a slave.
struct AssignedTask {
  1: string taskId                   // The mesos task ID for this task.  Guaranteed to be globally
                                     // unique.
  2: string slaveId                  // The mesos slave ID that this task has been assigned to.
                                     // This will not be populated for a PENDING task.
  3: string slaveHost                // The name of the machine that this task has been assigned to.
                                     // This will not be populated for a PENDING task.
  4: TaskConfig task                 // Information about how to run this task.
  5: map<string, i32> assignedPorts  // Ports reserved on the machine while this task is running.
  6: i32 instanceId                  // The instance ID assigned to this task.
                                     // Instance IDs must be unique and contiguous within a
                                     // job, and will be in the range [0, N-1] (inclusive)
                                     // for a job that has N instances.
}

// A task that has been scheduled.
struct ScheduledTask {
  1: AssignedTask assignedTask   // The task that was scheduled.
  2: ScheduleStatus status       // The current status of this task.
  3: i32 failureCount            // The number of failures that this task has accumulated over the
                                 // multi-generational history of this task.
  4: list<TaskEvent> taskEvents  // State change history for this task.
  5: string ancestorId           // The task ID of the previous generation of this task.  When a
                                 // task is automatically rescheduled, a copy of the task is created
                                 // and ancestor ID of the previous task's task ID.
}

struct ScheduleStatusResult {
  1: list<ScheduledTask> tasks
}

struct GetJobsResult {
  1: set<JobConfiguration> configs
}

// Contains a set of restrictions on matching tasks where all restrictions must be met (terms are
// AND'ed together).
struct TaskQuery {
  8: Identity owner               // TODO(wfarner): Deprecate Identity
  9: string environment
  2: string jobName
  4: set<string> taskIds
  5: set<ScheduleStatus> statuses
  6: string slaveHost
  7: set<i32> instanceIds
}

struct HostStatus {
  1: string host
  2: MaintenanceMode mode
}

struct JobSummary {
  1: string role
  2: i32 jobCount
  3: i32 cronJobCount
}

struct Hosts {
  1: set<string> hostNames
}

struct ListBackupsResult {
  1: set<string> backups
}

struct StartMaintenanceResult {
  1: set<HostStatus> statuses
}

struct DrainHostsResult {
  1: set<HostStatus> statuses
}

struct QueryRecoveryResult {
  1: set<ScheduledTask> tasks
}

struct MaintenanceStatusResult {
  1: set<HostStatus> statuses
}

struct EndMaintenanceResult {
  1: set<HostStatus> statuses
}

struct JobSummaryResult {
  1: list<JobSummary> summaries
}

// Specifies validation level for the populateJobConfig.
enum JobConfigValidation {
  NONE              = 0   // No additional job config validation would be performed (only parsing).
  RUN_FILTERS       = 1   // In addition to parsing config, will run through job filters.
}

union Result {
  1: PopulateJobResult populateJobResult
  3: ScheduleStatusResult scheduleStatusResult
  4: GetJobsResult getJobsResult
  5: GetQuotaResult getQuotaResult
  6: ListBackupsResult listBackupsResult
  7: StartMaintenanceResult startMaintenanceResult
  8: DrainHostsResult drainHostsResult
  9: QueryRecoveryResult queryRecoveryResult
  10: MaintenanceStatusResult maintenanceStatusResult
  11: EndMaintenanceResult endMaintenanceResult
  15: APIVersion getVersionResult
  16: AcquireLockResult acquireLockResult
  17: JobSummaryResult jobSummaryResult
}

struct Response {
  1: ResponseCode responseCode
  2: string message
  4: APIVersion version
  3: optional Result result
}

// A service that provides all the read only calls to the Aurora scheduler.
service ReadOnlyScheduler {
  // Returns a summary of the jobs grouped by role.
  Response getJobSummary()
}

// Due to assumptions in the client all authenticated RPCs must have a SessionKey as their
// last argument. Note that the order in this file is what matters, and message numbers should still
// never be reused.
service AuroraSchedulerManager extends ReadOnlyScheduler {
  // Creates a new job.  The request will be denied if a job with the provided
  // name already exists in the cluster.
  Response createJob(1: JobConfiguration description, 3: Lock lock, 2: SessionKey session)

  // Populates fields in a job configuration as though it were about to be run.
  // This can be used to diff a configuration running tasks.
  Response populateJobConfig(1: JobConfiguration description, 2: JobConfigValidation validation)

  // Starts a cron job immediately.  The request will be denied if the specified job does not
  // exist for the role account, or the job is not a cron job.
  Response startCronJob(4: JobKey job, 3: SessionKey session)

  // Restarts a batch of shards.
  Response restartShards(5: JobKey job, 3: set<i32> shardIds, 6: Lock lock 4: SessionKey session)

  // TODO(Suman Karumuri): Move this call into read only api
  // Fetches the status of tasks.
  Response getTasksStatus(1: TaskQuery query)

  // TODO(Suman Karumuri): Move this call into the read only api
  // Fetches the status of jobs.
  // ownerRole is optional, in which case all jobs are returned.
  Response getJobs(1: string ownerRole)

  // Initiates a kill on tasks.
  Response killTasks(1: TaskQuery query, 3: Lock lock, 2: SessionKey session)

  // TODO(Suman Karumuri): Move this call into the read only api
  // Fetches the quota allocated for a user.
  Response getQuota(1: string ownerRole)

  // TODO(Suman Karumuri): Move this call into the read only api
  // Returns the current version of the API implementation
  Response getVersion()

  // Adds new instances specified by the AddInstancesConfig.
  // A job represented by the JobKey must be protected by Lock.
  Response addInstances(
      1: AddInstancesConfig config,
      2: Lock lock,
      3: SessionKey session)

  // Creates and saves a new Lock instance guarding against multiple
  // mutating operations within the context defined by LockKey.
  Response acquireLock(1: LockKey lockKey, 2: SessionKey session)

  // Releases the lock acquired earlier in acquireLock call.
  Response releaseLock(1: Lock lock, 2: LockValidation validation, 3: SessionKey session)

  // Replaces the template (configuration) for the existing cron job.
  // The cron job template (configuration) must exist for the call to succeed.
  Response replaceCronTemplate(1: JobConfiguration config, 2: Lock lock, 3: SessionKey session)
}

struct InstanceConfigRewrite {
  1: InstanceKey instanceKey   // Key for the task to rewrite.
  2: TaskConfig oldTask        // The original configuration.
  3: TaskConfig rewrittenTask  // The rewritten configuration.
}

struct JobConfigRewrite {
  1: JobConfiguration oldJob        // The original job configuration.
  2: JobConfiguration rewrittenJob  // The rewritten job configuration.
}

union ConfigRewrite {
  1: JobConfigRewrite jobRewrite
  2: InstanceConfigRewrite instanceRewrite
}

struct RewriteConfigsRequest {
  1: list<ConfigRewrite> rewriteCommands
}

// It would be great to compose these services rather than extend, but that won't be possible until
// https://issues.apache.org/jira/browse/THRIFT-66 is resolved.
service AuroraAdmin extends AuroraSchedulerManager {
  // Assign quota to a user.  This will overwrite any pre-existing quota for the user.
  Response setQuota(1: string ownerRole, 2: Quota quota, 3: SessionKey session)

  // Forces a task into a specific state.  This does not guarantee the task will enter the given
  // state, as the task must still transition within the bounds of the state machine.  However,
  // it attempts to enter that state via the state machine.
  Response forceTaskState(
      1: string taskId,
      2: ScheduleStatus status,
      3: SessionKey session)

  // Immediately writes a storage snapshot to disk.
  Response performBackup(1: SessionKey session)

  // Lists backups that are available for recovery.
  Response listBackups(1: SessionKey session)

  // Loads a backup to an in-memory storage.  This must precede all other recovery operations.
  Response stageRecovery(1: string backupId, 2: SessionKey session)

  // Queries for tasks in a staged recovery.
  Response queryRecovery(1: TaskQuery query, 2: SessionKey session)

  // Deletes tasks from a staged recovery.
  Response deleteRecoveryTasks(1: TaskQuery query, 2: SessionKey session)

  // Commits a staged recovery, completely replacing the previous storage state.
  Response commitRecovery(1: SessionKey session)

  // Unloads (aborts) a staged recovery.
  Response unloadRecovery(1: SessionKey session)

  // Put the given hosts into maintenance mode.
  Response startMaintenance(1: Hosts hosts, 2: SessionKey session)

  // Ask scheduler to begin moving tasks scheduled on given hosts.
  Response drainHosts(1: Hosts hosts, 2: SessionKey session)

  // Retrieve the current maintenance states for a group of hosts.
  Response maintenanceStatus(1: Hosts hosts, 2: SessionKey session)

  // Set the given hosts back into serving mode.
  Response endMaintenance(1: Hosts hosts, 2: SessionKey session)

  // Start a storage snapshot and block until it completes.
  Response snapshot(1: SessionKey session)

  // Forcibly rewrites the stored definition of user configurations.  This is intended to be used
  // in a controlled setting, primarily to migrate pieces of configurations that are opaque to the
  // scheduler (e.g. executorConfig).
  // The scheduler may do some validation of the rewritten configurations, but it is important
  // that the caller take care to provide valid input and alter only necessary fields.
  Response rewriteConfigs(1: RewriteConfigsRequest request, 2: SessionKey session)
}
