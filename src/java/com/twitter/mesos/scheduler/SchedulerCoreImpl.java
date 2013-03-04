package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.ShardUpdateResult;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.scheduler.StateManagerImpl.UpdateException;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.events.PubsubEvent.Interceptors.Event;
import com.twitter.mesos.scheduler.events.PubsubEvent.Interceptors.Notify;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.quota.Quotas;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.Tasks.ACTIVE_STATES;
import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.mesos.gen.ScheduleStatus.UPDATING;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.CONSTRUCTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.INITIALIZED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STANDING_BY;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STARTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STOPPED;

/**
 * Implementation of the scheduler core.
 */
public class SchedulerCoreImpl implements SchedulerCore {

  private static final Logger LOG = Logger.getLogger(SchedulerCoreImpl.class.getName());

  private static final Predicate<ScheduledTask> IS_UPDATING = new Predicate<ScheduledTask>() {
    @Override public boolean apply(ScheduledTask task) {
      return task.getStatus() == UPDATING || task.getStatus() == ROLLBACK;
    }
  };

  private final Storage storage;

  private final CronJobManager cronScheduler;

  // Schedulers that are responsible for triggering execution of jobs.
  private final ImmutableList<JobManager> jobManagers;

  // State manager handles persistence of task modifications and state transitions.
  private final StateManagerImpl stateManager;

  private final QuotaManager quotaManager;

  /**
   * Scheduler states.
   */
  enum State {
    CONSTRUCTED,
    STANDING_BY,
    INITIALIZED,
    STARTED,
    STOPPED
  }

  private final StateMachine<State> stateMachine;

  /**
   * Creates a new core scheduler.
   *
   * @param storage Backing store implementation.
   * @param cronScheduler Cron scheduler.
   * @param immediateScheduler Immediate scheduler.
   * @param stateManager Persistent state manager.
   * @param quotaManager Quota tracker.
   */
  @Inject
  public SchedulerCoreImpl(
      Storage storage,
      CronJobManager cronScheduler,
      ImmediateJobManager immediateScheduler,
      StateManagerImpl stateManager,
      QuotaManager quotaManager) {

    this.storage = checkNotNull(storage);

    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    this.jobManagers = ImmutableList.of(cronScheduler, immediateScheduler);
    this.cronScheduler = cronScheduler;
    this.stateManager = checkNotNull(stateManager);
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

  @Notify(after = Event.StorageStarted)
  @Override
  public synchronized void start() {
    checkLifecycleState(INITIALIZED);
    stateManager.start();
    stateMachine.transition(STARTED);

    for (JobManager jobManager : jobManagers) {
      jobManager.start();
    }
  }

  @Notify(after = Event.DriverRegistered)
  @Override
  public synchronized void registered(String frameworkId) {
    checkStarted();
    stateManager.setFrameworkId(frameworkId);
  }

  private boolean hasActiveJob(JobConfiguration job) {
    return Iterables.any(jobManagers, managerHasJob(job));
  }

  @Override
  public synchronized void tasksDeleted(Set<String> taskIds) {
    setTaskStatus(Query.byId(taskIds), ScheduleStatus.UNKNOWN, Optional.<String>absent());
  }

  @Override
  public synchronized void createJob(ParsedConfiguration parsedConfiguration)
      throws ScheduleException, ConfigurationManager.TaskDescriptionException {
    checkStarted();

    JobConfiguration job = parsedConfiguration.get();
    if (hasActiveJob(job)) {
      throw new ScheduleException("Job already exists: " + jobKey(job));
    }

    ensureHasAdditionalQuota(job.getOwner().getRole(), Quotas.fromJob(job));

    boolean accepted = false;
    for (final JobManager manager : jobManagers) {
      if (manager.receiveJob(job)) {
        LOG.info("Job accepted by manager: " + manager.getUniqueKey());
        accepted = true;
        break;
      }
    }

    if (!accepted) {
      LOG.severe("Job was not accepted by any of the configured schedulers, discarding.");
      LOG.severe("Discarded job: " + job);
      throw new ScheduleException("Job not accepted, discarding.");
    }
  }

  @Override
  public synchronized void startCronJob(String role, String job) throws ScheduleException {
    checkNotBlank(role);
    checkNotBlank(job);
    checkStarted();

    if (!cronScheduler.hasJob(role, job)) {
      throw new ScheduleException("Cron job does not exist for " + Tasks.jobKey(role, job));
    }

    cronScheduler.startJobNow(role, job);
  }

  @Override
  public synchronized void runJob(JobConfiguration job) {
    checkNotNull(job);
    checkNotNull(job.getTaskConfigs());
    checkStarted();

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
        return manager.hasJob(job.getOwner().getRole(), job.getName());
      }
    };
  }

  @Override
  public synchronized void setTaskStatus(TaskQuery query, final ScheduleStatus status,
      Optional<String> message) {
    checkStarted();
    checkNotNull(query);
    checkNotNull(status);

    stateManager.changeState(query, status, message);
  }

  private static boolean specifiesJobOnly(TaskQuery query) {
    return Query.isJobScoped(query)
        && (query.getStatusesSize() == 0)
        && (query.getTaskIdsSize() == 0);
  }

  @Override
  public synchronized void killTasks(TaskQuery query, String user) throws ScheduleException {
    checkStarted();
    checkNotNull(query);
    LOG.info("Killing tasks matching " + query);

    boolean matchingScheduler = false;
    boolean updateFinished = false;

    if (specifiesJobOnly(query)) {
      // If this looks like a query for all tasks in a job, instruct the scheduler modules to
      // delete the job.
      for (JobManager manager : jobManagers) {
        matchingScheduler =
            manager.deleteJob(query.getOwner().getRole(), query.getJobName()) || matchingScheduler;
      }

      String role = query.getOwner().getRole();
      String job = query.getJobName();
      if (!matchingScheduler) {
        try {
          updateFinished = stateManager.finishUpdate(
              new Identity(role, user),
              job,
              Optional.<String>absent(),
              UpdateResult.TERMINATE,
              false);
        } catch (UpdateException e) {
          LOG.severe(
              String.format("Could not terminate job update for %s\n%s", query, e.getMessage()));
        }
      }
    }

    // Unless statuses were specifically supplied, only attempt to kill active tasks.
    if (query.getStatusesSize() == 0) {
      query.setStatuses(ACTIVE_STATES);
    }

    int tasksAffected = stateManager.changeState(query, KILLING, Optional.of("Killed by " + user));
    if (!matchingScheduler && !updateFinished && (tasksAffected == 0)) {
      throw new ScheduleException("No jobs to kill");
    }
  }

  private void ensureHasAdditionalQuota(String role, Quota quota) throws ScheduleException {
    if (!quotaManager.hasRemaining(role, quota)) {
      throw new ScheduleException("Insufficient resource quota.");
    }
  }

  @Override
  public synchronized Optional<String> initiateJobUpdate(ParsedConfiguration parsedConfiguration)
      throws ScheduleException {
    checkStarted();

    final JobConfiguration job = parsedConfiguration.get();
    if (cronScheduler.hasJob(job.getOwner().getRole(), job.getName())) {
      cronScheduler.updateJob(job);
      return Optional.absent();
    }

    return storage.doInWriteTransaction(new MutateWork<Optional<String>, ScheduleException>() {
      @Override public Optional<String> apply(MutableStoreProvider storeProvider)
          throws ScheduleException {

        Set<ScheduledTask> existingTasks = storeProvider.getTaskStore().fetchTasks(
            Query.activeQuery(job.getOwner(), job.getName()));

        // Reject if any existing task for the job is in UPDATING/ROLLBACK
        if (Iterables.any(existingTasks, IS_UPDATING)) {
          throw new ScheduleException("Update/Rollback already in progress for "
              + Tasks.jobKey(job));
        }

        if (!existingTasks.isEmpty()) {
          Quota currentJobQuota =
              Quotas.fromTasks(Iterables.transform(existingTasks, Tasks.SCHEDULED_TO_INFO));
          Quota newJobQuota = Quotas.fromJob(job);
          Quota additionalQuota = Quotas.subtract(newJobQuota, currentJobQuota);
          ensureHasAdditionalQuota(job.getOwner().getRole(), additionalQuota);
        }

        try {
          return Optional.of(stateManager.registerUpdate(job.getOwner().getRole(), job.getName(),
              job.getTaskConfigs()));
        } catch (StateManagerImpl.UpdateException e) {
          LOG.log(Level.INFO, "Failed to start update.", e);
          throw new ScheduleException(e.getMessage(), e);
        }
      }
    });
  }

  @Override
  public synchronized Map<Integer, ShardUpdateResult> updateShards(
      Identity identity,
      String jobName,
      Set<Integer> shards,
      String updateToken) throws ScheduleException {

    try {
      return stateManager.modifyShards(identity, jobName, shards, updateToken, true);
    } catch (UpdateException e) {
      LOG.log(Level.INFO, "Failed to update shards for " + Tasks.jobKey(identity, jobName), e);
      throw new ScheduleException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized Map<Integer, ShardUpdateResult> rollbackShards(
      Identity identity,
      String jobName,
      Set<Integer> shards,
      String updateToken) throws ScheduleException {

    try {
      return stateManager.modifyShards(identity, jobName, shards, updateToken, false);
    } catch (UpdateException e) {
      LOG.log(Level.INFO, "Failed to roll back shards for " + Tasks.jobKey(identity, jobName), e);
      throw new ScheduleException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void finishUpdate(
      Identity identity,
      String jobName,
      Optional<String> updateToken,
      UpdateResult result) throws ScheduleException {
    checkStarted();

    try {
      stateManager.finishUpdate(identity, jobName, updateToken, result, true);
    } catch (StateManagerImpl.UpdateException e) {
      LOG.log(Level.INFO, "Failed to finish update for " + Tasks.jobKey(identity, jobName), e);
      throw new ScheduleException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void preemptTask(AssignedTask task, AssignedTask preemptingTask) {
    checkNotNull(task);
    checkNotNull(preemptingTask);
    // TODO(William Farner): Throw SchedulingException if either task doesn't exist, etc.

    stateManager.changeState(Query.byId(task.getTaskId()), ScheduleStatus.PREEMPTING,
        Optional.of("Preempting in favor of " + preemptingTask.getTaskId()));
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
}
