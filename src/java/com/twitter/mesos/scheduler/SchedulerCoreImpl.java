package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

import com.twitter.common.stats.Stats;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.ExecutorKey;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.scheduler.StateManager.StateChanger;
import com.twitter.mesos.scheduler.StateManager.StateMutation;
import com.twitter.mesos.scheduler.StateManager.UpdateException;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.quota.Quotas;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.Tasks.SCHEDULED_TO_SHARD_ID;
import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.CONSTRUCTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.INITIALIZED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STANDING_BY;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STARTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STOPPED;

/**
 * Implementation of the scheduler core.
 *
 * @author William Farner
 */
public class SchedulerCoreImpl implements SchedulerCore {

  private static final Logger LOG = Logger.getLogger(SchedulerCore.class.getName());

  private final CronJobManager cronScheduler;

  // Schedulers that are responsible for triggering execution of jobs.
  private final ImmutableList<JobManager> jobManagers;

  // State manager handles persistence of task modifications and state transitions.
  private final StateManager stateManager;

  // Filter to determine whether a task should be scheduled.
  private final SchedulingFilter schedulingFilter;

  // Monitor to determine when we need to send heartbeats to executors to determine their liveness.
  private final PulseMonitor<ExecutorKey> executorPulseMonitor;
  private final Function<TwitterTaskInfo, TwitterTaskInfo> executorResourceAugmenter;
  private final QuotaManager quotaManager;

  enum State {
    CONSTRUCTED,
    STANDING_BY,
    INITIALIZED,
    STARTED,
    STOPPED
  }

  private final StateMachine<State> stateMachine;

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler,
      ImmediateJobManager immediateScheduler,
      StateManager stateManager,
      SchedulingFilter schedulingFilter,
      PulseMonitor<ExecutorKey> executorPulseMonitor,
      Function<TwitterTaskInfo, TwitterTaskInfo> executorResourceAugmenter,
      QuotaManager quotaManager) {

    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    this.jobManagers = ImmutableList.of(cronScheduler, immediateScheduler);
    this.cronScheduler = cronScheduler;
    this.stateManager = checkNotNull(stateManager);

    this.schedulingFilter = checkNotNull(schedulingFilter);
    this.executorPulseMonitor = checkNotNull(executorPulseMonitor);
    this.executorResourceAugmenter = checkNotNull(executorResourceAugmenter);
    this.quotaManager = checkNotNull(quotaManager);

   // TODO(John Sirois): Add a method to StateMachine or write a wrapper that allows for a
   // read-locked do-in-state assertion around a block of work.  Transition would then need to grab
   // the write lock.  Another approach is to force these transitions with:
   // SchedulerCoreFactory -> SchedulerCoreRunner -> SchedulerCore which remove all state sensitive
   // methods out of schedulerCore save for stop.
   stateMachine = StateMachine.<State>builder("scheduler-core")
       .initialState(CONSTRUCTED)
       .addState(CONSTRUCTED, STANDING_BY)
       .addState(STANDING_BY, INITIALIZED)
       .addState(INITIALIZED, STARTED)
       .addState(STARTED, STOPPED)
       .build();
  }

  @Override
  public synchronized void prepare() {
    checkLifecycleState(CONSTRUCTED);
    stateManager.prepare();
    stateMachine.transition(STANDING_BY);
  }

  @Override
  public synchronized String initialize() {
    checkLifecycleState(STANDING_BY);
    String storedFrameworkId = stateManager.initialize();
    stateMachine.transition(INITIALIZED);
    return storedFrameworkId;
  }

  @Override
  public synchronized void start() {
    checkLifecycleState(INITIALIZED);
    stateManager.start();
    stateMachine.transition(STARTED);

    for (JobManager jobManager : jobManagers) {
      jobManager.start();
    }
  }

  @Override
  public synchronized void registered(String frameworkId) {
    checkStarted();
    stateManager.setFrameworkId(frameworkId);
  }

  @Override
  public synchronized Set<ScheduledTask> getTasks(final Query query) {
    checkStarted();

    return stateManager.fetchTasks(query);
  }

  @Override
  public Iterable<TwitterTaskInfo> apply(Query query) {
    return Iterables.transform(getTasks(query), Tasks.SCHEDULED_TO_INFO);
  }

  private boolean hasActiveJob(JobConfiguration job) {
    return Iterables.any(jobManagers, managerHasJob(job));
  }

  private boolean hasRunningTasks(JobConfiguration job) {
    return !getTasks(Query.activeQuery(job)).isEmpty();
  }

  public synchronized void tasksDeleted(Set<String> taskIds) {
    setTaskStatus(Query.byId(taskIds), ScheduleStatus.UNKNOWN, null);
  }

  @Override
  public synchronized void createJob(JobConfiguration job) throws ScheduleException,
      ConfigurationManager.TaskDescriptionException {
    checkStarted();

    final JobConfiguration populated = ConfigurationManager.validateAndPopulate(job);

    if (hasActiveJob(populated)) {
      throw new ScheduleException("Job already exists: " + jobKey(populated));
    }

    if (!quotaManager.hasRemaining(job.getOwner().getRole(), Quotas.fromJob(populated))) {
      throw new ScheduleException("Insufficient resource quota.");
    }

    boolean accepted = false;
    for (final JobManager manager : jobManagers) {
      if (manager.receiveJob(populated)) {
        LOG.info("Job accepted by manager: " + manager.getUniqueKey());
        accepted = true;
        break;
      }
    }

    if (!accepted) {
      LOG.severe("Job was not accepted by any of the configured schedulers, discarding.");
      LOG.severe("Discarded job: " + populated);
      throw new ScheduleException("Job not accepted, discarding.");
    }
  }

  @Override
  public synchronized void startCronJob(String role, String job) throws ScheduleException {
    checkNotBlank(role);
    checkNotBlank(job);
    checkStarted();

    String key = Tasks.jobKey(role, job);
    if (!cronScheduler.hasJob(key)) {
      throw new ScheduleException("Cron job does not exist for " + key);
    }

    cronScheduler.startJobNow(key);
  }

  @Override
  public synchronized void runJob(JobConfiguration job) {
    checkNotNull(job);
    checkNotNull(job.getTaskConfigs());
    checkStarted();
    checkState(!hasRunningTasks(job));

    launchTasks(job.getTaskConfigs());
  }

  /**
   * Launches tasks.
   *
   * @param tasks Tasks to launch.
   * @return The task IDs of the new tasks.
   */
  private Set<String> launchTasks(Set<TwitterTaskInfo> tasks) {
    if (tasks.isEmpty()) {
      return ImmutableSet.of();
    }

    LOG.info("Launching " + tasks.size() + " tasks.");
    return stateManager.insertTasks(tasks);
  }

  /**
   * Creates a predicate that will determine whether a job manager has a job matching a job key.
   *
   * @param job Job to match.
   * @return A new predicate matching the job owner and name given.
   */
  private static Predicate<JobManager> managerHasJob(final JobConfiguration job) {
    return new Predicate<JobManager>() {
      @Override public boolean apply(JobManager manager) {
        return manager.hasJob(jobKey(job));
      }
    };
  }

  private static TwitterTaskInfo makeBootstrapTask() {
    return new TwitterTaskInfo()
        .setOwner(new Identity("mesos", "mesos"))
        .setJobName("executor_bootstrap")
        .setNumCpus(0.25)
        .setRamMb(1)
        .setShardId(0)
        .setRequestedPorts(ImmutableSet.<String>of())
        .setStartCommand("echo \"Bootstrapping\"");
  }

  @Override
  @Nullable
  public synchronized TwitterTask offer(final Offer offer, ExecutorID defaultExecutorId)
      throws ScheduleException {
    checkStarted();
    checkNotNull(offer);

    vars.resourceOffers.incrementAndGet();

    final String hostname = offer.getHostname();
    ExecutorKey executorKey = new ExecutorKey(defaultExecutorId, offer.getHostname());

    stateManager.saveAttributesFromOffer(hostname, offer.getAttributesList());

    Query query;
    Predicate<TwitterTaskInfo> postFilter;
    if (!executorPulseMonitor.isAlive(executorKey)) {
      TwitterTaskInfo bootstrapTask = makeBootstrapTask();
      Predicate<TwitterTaskInfo> resourceFilter =
          schedulingFilter.staticFilter(Resources.from(offer), null);

      // Mesos core does not account for the resources the executor itself will use up when it has
      // not yet started - do that accounting here and fail fast if we can't both run the executor
      // and the bootstrap task on the given host.
      if (!resourceFilter.apply(executorResourceAugmenter.apply(bootstrapTask))) {
        LOG.severe(String.format(
            "Insufficient resources on host %s to start executor plus a bootstrap task", hostname));
        return null;
      }

      LOG.info("Pulse monitor considers executor dead, launching bootstrap task on: " + hostname);
      executorPulseMonitor.pulse(executorKey);
      query = Query.byId(Iterables.getOnlyElement(launchTasks(ImmutableSet.of(bootstrapTask))));
      postFilter = Predicates.alwaysTrue();
      vars.executorBootstraps.incrementAndGet();
    } else {
      query = Query.and(
          Query.byStatus(PENDING),
          Predicates.compose(schedulingFilter.staticFilter(Resources.from(offer), hostname),
              Tasks.SCHEDULED_TO_INFO));
      // Perform the (more expensive) check to find tasks matching the dynamic state of the machine.
      postFilter = schedulingFilter.dynamicFilter(hostname);
    }

    final ImmutableSortedSet<ScheduledTask> candidates = ImmutableSortedSet.copyOf(
        Tasks.SCHEDULING_ORDER.onResultOf(Tasks.SCHEDULED_TO_ASSIGNED),
        stateManager.fetchTasks(query));
    if (candidates.isEmpty()) {
      return null;
    }
    log(Level.FINEST, "Candidates for offer: %s", new Object() {
      @Override public String toString() {
        return Iterables.transform(candidates, Tasks.SCHEDULED_TO_ID).toString();
      }
    });

    ScheduledTask assignment = Iterables.get(Iterables.filter(
        candidates, Predicates.compose(postFilter, Tasks.SCHEDULED_TO_INFO)), 0, null);
    if (assignment == null) {
      return null;
    }

    Set<Integer> selectedPorts =
        Resources.getPorts(offer, assignment.getAssignedTask().getTask().getRequestedPortsSize());

    AssignedTask task =
        stateManager.assignTask(Tasks.id(assignment), hostname, offer.getSlaveId(), selectedPorts);

    // There were no PENDING candidates.
    if (task == null) {
      return null;
    }

    LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s.",
        hostname, task.getSlaveId(), jobKey(task)));

    ImmutableList.Builder<Resource> resourceBuilder =
      ImmutableList.<Resource>builder()
        .add(Resources.makeMesosResource(Resources.CPUS, task.getTask().getNumCpus()))
        .add(Resources.makeMesosResource(Resources.RAM_MB, task.getTask().getRamMb()));
    if (selectedPorts.size() > 0) {
        resourceBuilder.add(Resources.makeMesosRangeResource(Resources.PORTS, selectedPorts));
    }

    return makeTwitterTask(task, offer.getSlaveId().getValue(), resourceBuilder.build());
  }

  @VisibleForTesting
  static TwitterTask makeTwitterTask(AssignedTask task, String slaveId, List<Resource> resources) {
    return new TwitterTask(task.getTaskId(), slaveId,
        task.getTask().getJobName() + "-" + task.getTaskId(), resources, task);
  }

  @Override
  public synchronized void setTaskStatus(Query rawQuery, final ScheduleStatus status,
      @Nullable final String message) {
    checkStarted();
    checkNotNull(rawQuery);
    checkNotNull(status);

    stateManager.changeState(rawQuery, status, message);
  }

  @Override
  public synchronized void killTasks(final Query query, String user) throws ScheduleException {
    checkStarted();
    checkNotNull(query);
    LOG.info("Killing tasks matching " + query);

    boolean matchingScheduler = false;
    boolean updateFinished = false;

    if (query.specifiesJobOnly()) {
      // If this looks like a query for all tasks in a job, instruct the scheduler modules to
      // delete the job.
      for (JobManager manager : jobManagers) {
        matchingScheduler = manager.deleteJob(query.getJobKey()) || matchingScheduler;
      }

      String role = query.base().getOwner().getRole();
      String job = query.base().getJobName();
      if (!matchingScheduler) {
        try {
          updateFinished = stateManager.finishUpdate(
              role, job, Optional.<String>absent(), UpdateResult.TERMINATE, false);
        } catch (UpdateException e) {
          LOG.severe(String.format("Could not terminate job update for %s\n%s",
              query.getJobKey(), e.getMessage()));
        }
      }
    }

    int tasksAffected = stateManager.changeState(query, KILLING, "Killed by " + user);
    if (!matchingScheduler && !updateFinished && (tasksAffected == 0)) {
      throw new ScheduleException("No jobs to kill");
    }
  }

  @Override
  public synchronized void restartTasks(final Set<String> taskIds) throws RestartException {
    checkStarted();
    checkNotBlank(taskIds);

    LOG.info("Restart requested for tasks " + taskIds);

    // TODO(William Farner): Change this (and the thrift interface) to query by shard ID in the
    //    context of a job instead of task ID.

    stateManager.taskOperation(Query.byId(taskIds),
        new StateMutation<RestartException>() {
          @Override public void execute(Set<ScheduledTask> tasks, StateChanger changer)
              throws RestartException {

            if (tasks.size() != taskIds.size()) {
              Set<String> unknownTasks = Sets.difference(taskIds, ImmutableSet
                  .copyOf(transform(tasks, Tasks.SCHEDULED_TO_ID)));

              throw new RestartException("Restart requested for unknown tasks " + unknownTasks);
            } else if (Iterables.any(tasks, Predicates.not(Tasks.ACTIVE_FILTER))) {
              throw new RestartException("Restart requested for inactive tasks "
                  + Iterables.filter(tasks, Tasks.ACTIVE_FILTER));
            }

            Set<String> jobKeys = ImmutableSet.copyOf(transform(tasks, Tasks.SCHEDULED_TO_JOB_KEY));
            if (jobKeys.size() != 1) {
              throw new RestartException(
                  "Task restart request cannot span multiple jobs: " + jobKeys);
            }

            changer.changeState(
                ImmutableSet.copyOf(Iterables.transform(tasks, Tasks.SCHEDULED_TO_ID)),
                RESTARTING, "Restarting by client request");
          }
        });
  }

  @Override
  public synchronized String startUpdate(JobConfiguration job)
      throws ScheduleException, ConfigurationManager.TaskDescriptionException {
    checkStarted();

    JobConfiguration populated = ConfigurationManager.validateAndPopulate(job);

    Set<ScheduledTask> existingTasks =
        stateManager.fetchTasks(Query.activeQuery(Tasks.jobKey(job)));
    if (!existingTasks.isEmpty()) {
      Quota currentJobQuota =
          Quotas.fromTasks(Iterables.transform(existingTasks, Tasks.SCHEDULED_TO_INFO));
      Quota newJobQuota = Quotas.fromJob(populated);
      Quota additionalQuota = Quotas.subtract(newJobQuota, currentJobQuota);
      if (!quotaManager.hasRemaining(job.getOwner().getRole(), additionalQuota)) {
        throw new ScheduleException("Insufficient resource quota.");
      }
    }

    try {
      return stateManager.registerUpdate(job.getOwner().getRole(), job.getName(),
          populated.getTaskConfigs());
    } catch (StateManager.UpdateException e) {
      LOG.log(Level.INFO, "Failed to start update.", e);
      throw new ScheduleException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void updateShards(String role, String jobName, Set<Integer> shards,
      String updateToken) throws ScheduleException {
    checkStarted();

    String jobKey = Tasks.jobKey(role, jobName);
    Set<ScheduledTask> tasks =
       stateManager.fetchTasks(Query.liveShards(jobKey, shards));

    // Extract any shard IDs that are being added as a part of this stage in the update.
    Set<Integer> newShardIds = Sets.difference(shards,
        ImmutableSet.copyOf(Iterables.transform(tasks, SCHEDULED_TO_SHARD_ID)));

    if (!newShardIds.isEmpty()) {
      Set<TwitterTaskInfo> newTasks =
          stateManager.fetchUpdatedTaskConfigs(role, jobName, newShardIds);
      Set<Integer> unrecognizedShards = Sets.difference(newShardIds,
          ImmutableSet.copyOf(Iterables.transform(newTasks, Tasks.INFO_TO_SHARD_ID)));
      if (!unrecognizedShards.isEmpty()) {
        throw new ScheduleException("Cannot update unrecognized shards " + unrecognizedShards);
      }

      // Create new tasks, so they will be moved into the PENDING state.
      stateManager.insertTasks(newTasks);
    }

    // Initiate update on the existing shards.
    stateManager.changeState(Query.liveShards(jobKey, Sets.difference(shards, newShardIds)),
        ScheduleStatus.UPDATING);
  }

  @Override
  public synchronized void rollbackShards(String role, String jobName, Set<Integer> shards,
      String updateToken) throws ScheduleException {
    checkStarted();

    stateManager.changeState(Query.liveShards(Tasks.jobKey(role, jobName), shards),
        ScheduleStatus.ROLLBACK);
  }

  @Override
  public synchronized void finishUpdate(String role, String jobName, Optional<String> updateToken,
      UpdateResult result) throws ScheduleException {
    checkStarted();

    try {
      stateManager.finishUpdate(role, jobName, updateToken, result, true);
    } catch (StateManager.UpdateException e) {
      LOG.log(Level.INFO, "Failed to finish update.", e);
      throw new ScheduleException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void preemptTask(AssignedTask task, AssignedTask preemptingTask) {
    checkNotNull(task);
    checkNotNull(preemptingTask);
    // TODO(William Farner): Throw SchedulingException if either task doesn't exist, etc.

    stateManager.changeState(Query.byId(task.getTaskId()), ScheduleStatus.PREEMPTING,
        "Preempting in favor of " + Tasks.jobKey(preemptingTask));
  }

  @Override
  public synchronized void stop() {
    checkStarted();
    stateManager.stop();
    stateMachine.transition(STOPPED);
  }

  private void checkStarted() {
    checkLifecycleState(STARTED);
  }

  private void checkLifecycleState(State state) {
    Preconditions.checkState(stateMachine.getState() == state);
  }

  private static void log(Level level, String message, Object... args) {
    if (LOG.isLoggable(level)) {
      LOG.log(level, String.format(message, args));
    }
  }

  private final class Vars {
    final AtomicLong resourceOffers = Stats.exportLong("scheduler_resource_offers");
    final AtomicLong executorBootstraps = Stats.exportLong("executor_bootstraps");
  }
  private final Vars vars = new Vars();
}
