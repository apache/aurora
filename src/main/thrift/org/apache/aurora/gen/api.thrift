/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace java org.apache.aurora.gen
namespace py gen.apache.aurora.api

// Thrift interface definition for the aurora scheduler.

/*
 * TODO(wfarner): It would be nice if we could put some HTML tags here, regex doesn't handle it though.
 * The result of an API operation.  A result may only be specified when this is OK.
 */
enum ResponseCode {
  INVALID_REQUEST = 0,
  OK              = 1,
  ERROR           = 2,
  WARNING         = 3,
  AUTH_FAILED     = 4,
  /** Raised when a Lock-protected operation failed due to lock validation. */
  LOCK_ERROR      = 5
}

const i32 THRIFT_API_VERSION = 3

struct APIVersion {
  1: required i32 major
}

// Scheduler Thrift API Version. Increment this when breaking backwards compatibility.
const APIVersion CURRENT_API_VERSION = {'major': THRIFT_API_VERSION}

// Aurora executor framework name.
const string AURORA_EXECUTOR_NAME = 'AuroraExecutor'

struct Identity {
  1: string role
  2: string user
}

struct SessionKey {
  /**
   * The name of the authentication mechanism, which instructs the server how to interpret the data
   * field.
   */
  4: optional string mechanism
  /** A blob of data that the server may use for authentication. */
  5: optional binary data
}

struct ResourceAggregate {
  /** Number of CPU cores allotted. */
  1: double numCpus
  /** Megabytes of RAM allotted. */
  2: i64 ramMb
  /** Megabytes of disk space allotted. */
  3: i64 diskMb
}

/** A single host attribute. */
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

/** The attributes assigned to a host. */
struct HostAttributes {
  1: string          host
  2: set<Attribute>  attributes
  3: optional MaintenanceMode mode
  4: optional string slaveId
}

/**
 * A constraint that specifies an explicit set of values, at least one of which must be present
 * on a host for a task to be scheduled there.
 */
struct ValueConstraint {
  /** If true, treat this as a 'not' - to avoid specific values. */
  1: bool negated
  2: set<string> values
}

/**
 * A constraint the specifies the maximum number of active tasks on a host with a matching
 * attribute that may be scheduled simultaneously.
 */
struct LimitConstraint {
  1: i32 limit
}

/** Types of constraints that may be applied to a task. */
union TaskConstraint {
  1: ValueConstraint value
  2: LimitConstraint limit
}

/** A constraint that defines whether a task may be scheduled on a host. */
struct Constraint {
  /** Mesos slave attribute that the constraint is matched against. */
  1: string name
  2: TaskConstraint constraint
}

struct Package {
  1: string role
  2: string name
  3: i32 version
}

/** Arbitrary key-value metadata to be included into TaskConfig. */
struct Metadata {
  1: string key
  2: string value
}

/** A unique identifier for a Job. */
struct JobKey {
  /** User role (Unix service account), for example "mesos" */
  1: string role
  /** Environment, for example "devel" */
  2: string environment
  /** Name, for example "labrat" */
  3: string name
}

/** A unique lock key. */
union LockKey {
  1: JobKey job
}

/** A generic lock struct to facilitate context specific resource/operation serialization. */
struct Lock {
  /** ID of the lock - unique per storage */
  1: LockKey key
  /** UUID - facilitating soft lock authorization */
  2: string token
  /** Lock creator */
  3: string user
  /** Lock creation timestamp in milliseconds */
  4: i64 timestampMs
  /** Optional message to record with the lock */
  5: optional string message
}

/** Defines the required lock validation level. */
enum LockValidation {
  /** The lock must be valid in order to be released. */
  CHECKED   = 0
  /** The lock will be released without validation (aka "force release"). */
  UNCHECKED = 1
}

/** A unique identifier for the active task within a job. */
struct InstanceKey {
  /** Key identifying the job. */
  1: JobKey jobKey
  /** Unique instance ID for the active task in a job. */
  2: i32 instanceId
}

struct ExecutorConfig {
  /** Name identifying the Executor. */
  1: string name
  /** Executor configuration data. */
  2: string data
}

/** Description of the tasks contained within a job. */
struct TaskConfig {
 // TODO(William Farner): Store a JobKey instead.
 /** contains the role component of JobKey */
 17: Identity owner
 /** contains the environment component of JobKey */
 26: string environment
 /** contains the name component of JobKey */
  3: string jobName
  7: bool isService
  8: double numCpus
  9: i64 ramMb
 10: i64 diskMb
 11: i32 priority
 13: i32 maxTaskFailures
 /** Whether this is a production task, which can preempt. */
 18: optional bool production

 20: set<Constraint> constraints
 /** a list of named ports this task requests */
 21: set<string> requestedPorts

 /**
  * Custom links to include when displaying this task on the scheduler dashboard. Keys are anchor
  * text, values are URLs. Wildcards are supported for dynamic link crafting based on host, ports,
  * instance, etc.
  */
 22: optional map<string, string> taskLinks
 23: optional string contactEmail
 /** Executor configuration */
 25: optional ExecutorConfig executorConfig
 /** Used to display additional details in the UI. */
 27: optional set<Metadata> metadata
}

/** Defines the policy for launching a new cron job when one is already running. */
enum CronCollisionPolicy {
  /** Kills the existing job with the colliding name, and runs the new cron job. */
  KILL_EXISTING = 0,
  /** Cancels execution of the new job, leaving the running job in tact. */
  CANCEL_NEW    = 1,
  /**
   * DEPRECATED. For existing jobs, treated the same as CANCEL_NEW.
   * createJob will reject jobs with this policy.
   */
  RUN_OVERLAP   = 2
}

/**
 * Description of an Aurora job. One task will be scheduled for each instance within the job.
 */
struct JobConfiguration {
  /**
   * Key for this job. If not specified name, owner.role, and a reasonable default environment are
   * used to construct it server-side.
   */
  9: JobKey key
  // TODO(William Farner): Deprecate Identity and
  // use JobKey instead (MESOS-4006).
  /** Owner of this job. */
  7: Identity owner
  /**
   * If present, the job will be handled as a cron job with this crontab-syntax schedule.
   */
  4: string cronSchedule
  /** Collision policy to use when handling overlapping cron runs.  Default is KILL_EXISTING. */
  5: CronCollisionPolicy cronCollisionPolicy
  /** Task configuration for this job. */
  6: TaskConfig taskConfig
  /**
   * The number of instances in the job. Generated instance IDs for tasks will be in the range
   * [0, instances).
   */
  8: i32 instanceCount
}

struct JobStats {
  /** Number of tasks in active state for this job. */
  1: i32 activeTaskCount
  /** Number of tasks in finished state for this job. */
  2: i32 finishedTaskCount
  /** Number of failed tasks for this job. */
  3: i32 failedTaskCount
  /** Number of tasks in pending state for this job. */
  4: i32 pendingTaskCount
}

struct JobSummary {
  1: JobConfiguration job
  2: JobStats stats
  /** Timestamp of next cron run in ms since epoch, for a cron job */
  3: optional i64 nextCronRunMs
}

/** A request to add the following instances to an existing job. Used by addInstances. */
struct AddInstancesConfig {
  1: JobKey key
  2: TaskConfig taskConfig
  3: set<i32> instanceIds
}

/** Closed range of integers. */
struct Range {
  1: i32 first
  2: i32 last
}

struct ConfigGroup {
  1: TaskConfig config
  2: set<i32> instanceIds   // TODO(maxim): change it to use list<Range> instead.
}

struct ConfigSummary {
  1: JobKey key
  2: set<ConfigGroup> groups
}

struct PopulateJobResult {
  1: set<TaskConfig> populated
}

struct GetQuotaResult {
  /** Total allocated resource quota. */
  1: ResourceAggregate quota
  /** Resources consumed by production jobs. */
  2: optional ResourceAggregate prodConsumption
  /** Resources consumed by non-production jobs. */
  3: optional ResourceAggregate nonProdConsumption
}

/** Wraps return results for the acquireLock API. */
struct AcquireLockResult {
  /** Acquired Lock instance. */
  1: Lock lock
}

/** States that a task may be in. */
enum ScheduleStatus {
  // TODO(maxim): This state does not add much value. Consider dropping it completely.
  /* Initial state for a task.  A task will remain in this state until it has been persisted. */
  INIT             = 11,
  /** The task will be rescheduled, but is being throttled for restarting too frequently. */
  THROTTLED        = 16,
  /** Task is awaiting assignment to a slave. */
  PENDING          = 0,
  /** Task has been assigned to a slave. */
  ASSIGNED         = 9,
  /** Slave has acknowledged receipt of task and is bootstrapping the task. */
  STARTING         = 1,
  /** The task is running on the slave. */
  RUNNING          = 2,
  /** The task terminated with an exit code of zero. */
  FINISHED         = 3,
  /** The task is being preempted by another task. */
  PREEMPTING       = 13,
  /** The task is being restarted in response to a user request. */
  RESTARTING       = 12,
  /** The task is being restarted in response to a host maintenance request. */
  DRAINING         = 17,
  /** The task terminated with a non-zero exit code. */
  FAILED           = 4,
  /** Execution of the task was terminated by the system. */
  KILLED           = 5,
  /** The task is being forcibly killed. */
  KILLING          = 6,
  /** A fault in the task environment has caused the system to believe the task no longer exists.
   * This can happen, for example, when a slave process disappears.
   */
  LOST             = 7,
  /** The task sandbox has been deleted by the executor. */
  SANDBOX_DELETED  = 10
}

// States that a task may be in while still considered active.
const set<ScheduleStatus> ACTIVE_STATES = [ScheduleStatus.ASSIGNED,
                                           ScheduleStatus.DRAINING,
                                           ScheduleStatus.KILLING,
                                           ScheduleStatus.PENDING,
                                           ScheduleStatus.PREEMPTING,
                                           ScheduleStatus.RESTARTING
                                           ScheduleStatus.RUNNING,
                                           ScheduleStatus.STARTING,
                                           ScheduleStatus.THROTTLED]

// States that a task may be in while associated with a slave machine and non-terminal.
const set<ScheduleStatus> SLAVE_ASSIGNED_STATES = [ScheduleStatus.ASSIGNED,
                                                   ScheduleStatus.DRAINING,
                                                   ScheduleStatus.KILLING,
                                                   ScheduleStatus.PREEMPTING,
                                                   ScheduleStatus.RESTARTING,
                                                   ScheduleStatus.RUNNING,
                                                   ScheduleStatus.STARTING]

// States that a task may be in while in an active sandbox.
const set<ScheduleStatus> LIVE_STATES = [ScheduleStatus.KILLING,
                                         ScheduleStatus.PREEMPTING,
                                         ScheduleStatus.RESTARTING,
                                         ScheduleStatus.DRAINING,
                                         ScheduleStatus.RUNNING]

// States a completed task may be in.
const set<ScheduleStatus> TERMINAL_STATES = [ScheduleStatus.FAILED,
                                             ScheduleStatus.FINISHED,
                                             ScheduleStatus.KILLED,
                                             ScheduleStatus.LOST,
                                             ScheduleStatus.SANDBOX_DELETED]

// Environment assigned to a job when unspecified
const string DEFAULT_ENVIRONMENT = "devel"

// Regular expressions for matching valid identifiers for job path components. All expressions
// below should accept and reject the same set of inputs.
const string GOOD_IDENTIFIER_PATTERN = "^[\\w\\-\\.]+$"
// JVM: Use with java.util.regex.Pattern#compile
const string GOOD_IDENTIFIER_PATTERN_JVM = GOOD_IDENTIFIER_PATTERN
// Python: Use with re.compile
const string GOOD_IDENTIFIER_PATTERN_PYTHON = GOOD_IDENTIFIER_PATTERN

/** Event marking a state transition within a task's lifecycle. */
struct TaskEvent {
  /** Epoch timestamp in milliseconds. */
  1: i64 timestamp
  /** New status of the task. */
  2: ScheduleStatus status
  /** Audit message that explains why a transition occurred. */
  3: optional string message
  /** Hostname of the scheduler machine that performed the event. */
  4: optional string scheduler
}

/** A task assignment that is provided to an executor. */
struct AssignedTask {
  /** The mesos task ID for this task.  Guaranteed to be globally unique */
  1: string taskId

  /**
   * The mesos slave ID that this task has been assigned to.
   * This will not be populated for a PENDING task.
   */
  2: string slaveId

  /**
   * The name of the machine that this task has been assigned to.
   * This will not be populated for a PENDING task.
   */
  3: string slaveHost

  /** Information about how to run this task. */
  4: TaskConfig task
  /** Ports reserved on the machine while this task is running. */
  5: map<string, i32> assignedPorts

  /**
   * The instance ID assigned to this task. Instance IDs must be unique and contiguous within a
   * job, and will be in the range [0, N-1] (inclusive) for a job that has N instances.
   */
  6: i32 instanceId
}

/** A task that has been scheduled. */
struct ScheduledTask {
  /** The task that was scheduled. */
  1: AssignedTask assignedTask
  /** The current status of this task. */
  2: ScheduleStatus status
  /**
   * The number of failures that this task has accumulated over the multi-generational history of
   * this task.
   */
  3: i32 failureCount
  /** State change history for this task. */
  4: list<TaskEvent> taskEvents
  /**
   * The task ID of the previous generation of this task.  When a task is automatically rescheduled,
   * a copy of the task is created and ancestor ID of the previous task's task ID.
   */
  5: string ancestorId
}

struct ScheduleStatusResult {
  1: list<ScheduledTask> tasks
}

struct GetJobsResult {
  1: set<JobConfiguration> configs
}

/**
 * Contains a set of restrictions on matching tasks where all restrictions must be met
 * (terms are AND'ed together).
 */
struct TaskQuery {
  8: Identity owner               // TODO(wfarner): Deprecate Identity
  9: string environment
  2: string jobName
  4: set<string> taskIds
  5: set<ScheduleStatus> statuses
  7: set<i32> instanceIds
  10: set<string> slaveHosts
  11: set<JobKey> jobKeys
  12: i32 offset
  13: i32 limit
}

struct HostStatus {
  1: string host
  2: MaintenanceMode mode
}

struct RoleSummary {
  1: string role
  2: i32 jobCount
  3: i32 cronJobCount
}

struct Hosts {
  1: set<string> hostNames
}

struct PendingReason {
  1: string taskId
  2: string reason
}

/** States that a job update may be in. */
enum UpdateStatus {
  /** Update is created but not yet started. */
  INIT = 0,

  /** Update is in progress. */
  ROLLING_FORWARD = 1,

  /** Update has failed and is being rolled back. */
  ROLLING_BACK = 2,

  /** Update has been paused while in progress. */
  ROLL_FORWARD_PAUSED = 3,

  /** Update has been paused during rollback. */
  ROLL_BACK_PAUSED = 4,

  /** Update has completed successfully. */
  ROLLED_FORWARD = 5,

  /** Update has failed and rolled back. */
  ROLLED_BACK = 6,

  /** Update was aborted. */
  ABORTED = 7,

  /** Unknown error during update. */
  ERROR = 8
}

/** Update actions that can be applied to job instances. */
enum UpdateAction {
  // TODO(maxim): Define when instance update part is completed.
}

/** Job update thresholds and limits. */
struct UpdateSettings {
  /** Max number of instances being updated at any given moment. */
  1: i32 updateGroupSize

  /** Max number of instance failures to tolerate before marking instance as FAILED. */
  2: i32 maxPerInstanceFailures

  /** Max number of FAILED instances to tolerate before terminating the forward roll. */
  3: i32 maxFailedInstances

  /** Max time to wait until an instance reaches RUNNING state. */
  4: i32 maxWaitToInstanceRunningMs

  /** Min time to watch to watch a RUNNING instance. */
  5: i32 minWaitInInstanceRunningMs

  /** If True, disables failed update rollback. */
  6: bool doNotRollbackOnFailure

  /** A set of instance IDs to act on. */
  7: set<i32> updateOnlyTheseInstances
}

/** Event marking a state transition in job update lifecycle. */
struct UpdateEvent {
  /** Update status. */
  1: UpdateStatus status

  /** Epoch timestamp in milliseconds. */
  2: i64 timestampMs
}

/** Event marking a state transition in job instance update lifecycle. */
struct InstanceUpdateEvent {
  /** Job instance ID. */
  1: i32 instanceId

  /** Epoch timestamp in milliseconds. */
  2: i64 timestampMs

  /** Update action taken on the instance. */
  3: UpdateAction action
}

/** Maps instance IDs to TaskConfigs it. */
struct InstanceTaskConfig {
  /** A TaskConfig associated with instances. */
  1: TaskConfig task

  /** Instances associated with the TaskConfig. */
  2: list<Range> instances
}

/** Job update state. */
struct Update {
  /** Update ID. */
  1: string updateId

  /** Job key. */
  2: JobKey jobKey

  /** User initiated an update. */
  3: string user

  /** Current status of the update. */
  4: UpdateStatus status

  /** Creation timestamp in milliseconds. */
  5: i64 createdTimestampMs

  /** Last modified timestamp in milliseconds. */
  6: i64 lastModifiedTimestampMs
}

/** Update configuration and setting details. */
struct UpdateConfiguration {
  /** Update ID. */
  1: string updateId

  /** Actual InstanceId -> TaskConfig mapping when the update was requested. */
  2: set<InstanceTaskConfig> oldTaskConfigs

  /** Desired InstanceId -> TaskConfig mapping when the update completes. */
  3: set<InstanceTaskConfig> newTaskConfigs

  /** Update specific settings. */
  4: UpdateSettings settings
}

/** Full job update info including all lifecycle events. */
struct UpdateDetails {
  /** Current job update state. */
  1: Update summary

  /** Update configuration and setting details. */
  2: UpdateConfiguration details

  /** History for this update. */
  3: list<UpdateEvent> updateEvents

  /** History for the individual instances updated. */
  4: list<InstanceUpdateEvent> instanceEvents
}

/** A request to update the following instances of the existing job. Used by startUpdate. */
struct UpdateRequest {
  /** Job key. */
  1: JobKey jobKey

  /** Desired TaskConfig for the job. */
  2: TaskConfig taskConfig

  /** Desired job instance count. */
  3: i32 instanceCount

  /** Update settings and limits. */
  4: UpdateSettings settings
}

/**
 * Contains a set of restrictions on matching job updates where all restrictions must be met
 * (terms are AND'ed together).
 */
struct UpdateQuery {
  /** Update ID. */
  1: string updateId

  /** Job role. */
  2: string role

  /** Job key. */
  3: JobKey jobKey

  /** Set of update statuses. */
  4: set<UpdateStatus> updateStatus

  /** Offset to serve data from. Used by pagination. */
  5: i32 offset

  /** Number or records to serve. Used by pagination. */
  6: i32 limit
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

struct RoleSummaryResult {
  1: set<RoleSummary> summaries
}

struct JobSummaryResult {
  1: set<JobSummary> summaries
}


struct GetLocksResult {
  1: set<Lock> locks
}

struct ConfigSummaryResult {
  1: ConfigSummary summary
}

struct GetPendingReasonResult {
  1: set<PendingReason> reasons
}

/** Result of the startUpdate call. */
struct StartUpdateResult {
  1: string updateId
}

/** Result of the getUpdates call. */
struct GetUpdatesResult {
  1: set<Update> updates
}

/** Result of the getUpdateDetails call. */
struct GetUpdateDetailsResult {
  1: UpdateDetails details
}

/** Information about the scheduler. */
struct ServerInfo {
  1: string clusterName
  2: i32 thriftAPIVersion
  /** A url prefix for job container stats. */
  3: string statsUrlPrefix
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
  17: RoleSummaryResult roleSummaryResult
  18: JobSummaryResult jobSummaryResult
  19: GetLocksResult getLocksResult
  20: ConfigSummaryResult configSummaryResult
  21: GetPendingReasonResult getPendingReasonResult
  22: StartUpdateResult startUpdateResult
  23: GetUpdatesResult getUpdatesResult
  24: GetUpdateDetailsResult getUpdateDetailsResult
}

struct ResponseDetail {
  1: string message
}

struct Response {
  1: ResponseCode responseCode
  // TODO(wfarner): Remove the message field in 0.7.0. (AURORA-466)
  2: optional string messageDEPRECATED
  // TODO(wfarner): Remove version field in 0.7.0. (AURORA-467)
  4: APIVersion DEPRECATEDversion
  5: ServerInfo serverInfo
  /** Payload from the invoked RPC. */
  3: optional Result result
  /**
   * Messages from the server relevant to the request, such as warnings or use of deprecated
   * features.
   */
  6: list<ResponseDetail> details
}

// A service that provides all the read only calls to the Aurora scheduler.
service ReadOnlyScheduler {
  /** Returns a summary of the jobs grouped by role. */
  Response getRoleSummary()

  /** Returns a summary of jobs, optionally only those owned by a specific role. */
  Response getJobSummary(1: string role)

  /** Fetches the status of tasks. */
  Response getTasksStatus(1: TaskQuery query)

  /**
   * Same as getTaskStatus but without the TaskConfig.ExecutorConfig data set.
   * This is an interim solution until we have a better way to query TaskConfigs (AURORA-541).
   */
  Response getTasksWithoutConfigs(1: TaskQuery query)

  /** Returns user-friendly reasons (if available) for tasks retained in PENDING state. */
  Response getPendingReason(1: TaskQuery query)

  /** Fetches the configuration summary of active tasks for the specified job. */
  Response getConfigSummary(1: JobKey job)

  /**
   * Fetches the status of jobs.
   * ownerRole is optional, in which case all jobs are returned.
   */
  Response getJobs(1: string ownerRole)

  /** Fetches the quota allocated for a user. */
  Response getQuota(1: string ownerRole)

  // TODO(Suman Karumuri): Delete this API once it is no longer used.
  /**
   * Returns the current version of the API implementation
   * NOTE: This method is deprecated.
   */
  Response getVersion()

  /**
   * Populates fields in a job configuration as though it were about to be run.
   * This can be used to diff a configuration running tasks.
   */
  Response populateJobConfig(1: JobConfiguration description)

  /** Returns all stored context specific resource/operation locks. */
  Response getLocks()

  /** Gets job updates. Not implemented yet. */
  Response getUpdates(1: UpdateQuery updateQuery)

  /** Gets job update details. Not implemented yet. */
  Response getUpdateDetails(1: string updateId)
}

// Due to assumptions in the client all authenticated RPCs must have a SessionKey as their
// last argument. Note that the order in this file is what matters, and message numbers should still
// never be reused.
service AuroraSchedulerManager extends ReadOnlyScheduler {
  /**
   * Creates a new job.  The request will be denied if a job with the provided name already exists
   * in the cluster.
   */
  Response createJob(1: JobConfiguration description, 3: Lock lock, 2: SessionKey session)

  /**
   * Enters a job into the cron schedule, without actually starting the job.
   * If the job is already present in the schedule, this will update the schedule entry with the new
   * configuration.
   */
  Response scheduleCronJob(1: JobConfiguration description, 3: Lock lock, 2: SessionKey session)

  /**
   * Removes a job from the cron schedule. The request will be denied if the job was not previously
   * scheduled with scheduleCronJob.
   */
  Response descheduleCronJob(4: JobKey job, 3: Lock lock, 2: SessionKey session)

  /**
   * Starts a cron job immediately.  The request will be denied if the specified job does not
   * exist for the role account, or the job is not a cron job.
   */
  Response startCronJob(4: JobKey job, 3: SessionKey session)

  /** Restarts a batch of shards. */
  Response restartShards(5: JobKey job, 3: set<i32> shardIds, 6: Lock lock 4: SessionKey session)

  /** Initiates a kill on tasks. */
  Response killTasks(1: TaskQuery query, 3: Lock lock, 2: SessionKey session)

  /**
   * Adds new instances specified by the AddInstancesConfig. A job represented by the JobKey must be
   * protected by Lock.
   */
  Response addInstances(
      1: AddInstancesConfig config,
      2: Lock lock,
      3: SessionKey session)

  /**
   * Creates and saves a new Lock instance guarding against multiple mutating operations within the
   * context defined by LockKey.
   */
  Response acquireLock(1: LockKey lockKey, 2: SessionKey session)

  /** Releases the lock acquired earlier in acquireLock call. */
  Response releaseLock(1: Lock lock, 2: LockValidation validation, 3: SessionKey session)

  /**
   * Replaces the template (configuration) for the existing cron job.
   * The cron job template (configuration) must exist for the call to succeed.
   */
  Response replaceCronTemplate(1: JobConfiguration config, 2: Lock lock, 3: SessionKey session)

  /** Starts update of the existing service job. Not implemented yet. */
  Response startUpdate(
      1: UpdateRequest request,
      2: Lock lock,
      3: SessionKey session)

  /** Pauses the update progress. Can be resumed by resumeUpdate call. Not implemented yet. */
  Response pauseUpdate(1: string updateId, 2: Lock lock, 3: SessionKey session)

  /** Resumes progress of a previously paused update. Not implemented yet. */
  Response resumeUpdate(1: string updateId, 2: Lock lock, 3: SessionKey session)

  /** Permanently aborts the update. Does not remove the update history. Not implemented yet. */
  Response abortUpdate(1: string updateId, 2: Lock lock, 3: SessionKey session)
}

struct InstanceConfigRewrite {
  /** Key for the task to rewrite. */
  1: InstanceKey instanceKey
  /** The original configuration. */
  2: TaskConfig oldTask
  /** The rewritten configuration. */
  3: TaskConfig rewrittenTask
}

struct JobConfigRewrite {
  /** The original job configuration. */
  1: JobConfiguration oldJob
  /** The rewritten job configuration. */
  2: JobConfiguration rewrittenJob
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
  /** Assign quota to a user.  This will overwrite any pre-existing quota for the user. */
  Response setQuota(1: string ownerRole, 2: ResourceAggregate quota, 3: SessionKey session)

  /**
   * Forces a task into a specific state.  This does not guarantee the task will enter the given
   * state, as the task must still transition within the bounds of the state machine.  However,
   * it attempts to enter that state via the state machine.
   */
  Response forceTaskState(
      1: string taskId,
      2: ScheduleStatus status,
      3: SessionKey session)

  /** Immediately writes a storage snapshot to disk. */
  Response performBackup(1: SessionKey session)

  /** Lists backups that are available for recovery. */
  Response listBackups(1: SessionKey session)

  /** Loads a backup to an in-memory storage.  This must precede all other recovery operations. */
  Response stageRecovery(1: string backupId, 2: SessionKey session)

  /** Queries for tasks in a staged recovery. */
  Response queryRecovery(1: TaskQuery query, 2: SessionKey session)

  /** Deletes tasks from a staged recovery. */
  Response deleteRecoveryTasks(1: TaskQuery query, 2: SessionKey session)

  /** Commits a staged recovery, completely replacing the previous storage state. */
  Response commitRecovery(1: SessionKey session)

  /** Unloads (aborts) a staged recovery. */
  Response unloadRecovery(1: SessionKey session)

  /** Put the given hosts into maintenance mode. */
  Response startMaintenance(1: Hosts hosts, 2: SessionKey session)

  /** Ask scheduler to begin moving tasks scheduled on given hosts. */
  Response drainHosts(1: Hosts hosts, 2: SessionKey session)

  /** Retrieve the current maintenance states for a group of hosts. */
  Response maintenanceStatus(1: Hosts hosts, 2: SessionKey session)

  /** Set the given hosts back into serving mode. */
  Response endMaintenance(1: Hosts hosts, 2: SessionKey session)

  /** Start a storage snapshot and block until it completes. */
  Response snapshot(1: SessionKey session)

  /**
   * Forcibly rewrites the stored definition of user configurations.  This is intended to be used
   * in a controlled setting, primarily to migrate pieces of configurations that are opaque to the
   * scheduler (e.g. executorConfig).
   * The scheduler may do some validation of the rewritten configurations, but it is important
   * that the caller take care to provide valid input and alter only necessary fields.
   */
  Response rewriteConfigs(1: RewriteConfigsRequest request, 2: SessionKey session)
}
