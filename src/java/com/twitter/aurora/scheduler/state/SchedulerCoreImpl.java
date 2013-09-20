/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler.state;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.ShardUpdateResult;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.UpdateResult;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.ScheduleException;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.aurora.scheduler.configuration.ParsedConfiguration;
import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.quota.Quotas;
import com.twitter.aurora.scheduler.state.StateManagerImpl.UpdateException;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.gen.ScheduleStatus.KILLING;
import static com.twitter.aurora.gen.ScheduleStatus.RESTARTING;
import static com.twitter.aurora.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.aurora.gen.ScheduleStatus.UPDATING;
import static com.twitter.aurora.scheduler.base.Tasks.ACTIVE_STATES;

/**
 * Implementation of the scheduler core.
 */
class SchedulerCoreImpl implements SchedulerCore {

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

  // TODO(Bill Farner): Avoid using StateManagerImpl.
  // State manager handles persistence of task modifications and state transitions.
  private final StateManagerImpl stateManager;

  private final QuotaManager quotaManager;
  private final Function<TaskConfig, String> taskIdGenerator;

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
      QuotaManager quotaManager,
      Function<TaskConfig, String> taskIdGenerator) {

    this.storage = checkNotNull(storage);

    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    this.jobManagers = ImmutableList.of(cronScheduler, immediateScheduler);
    this.cronScheduler = cronScheduler;
    this.stateManager = checkNotNull(stateManager);
    this.quotaManager = checkNotNull(quotaManager);
    this.taskIdGenerator = checkNotNull(taskIdGenerator);
  }

  private boolean hasActiveJob(JobConfiguration job) {
    return Iterables.any(jobManagers, managerHasJob(job));
  }

  @Override
  public synchronized void tasksDeleted(Set<String> taskIds) {
    setTaskStatus(Query.taskScoped(taskIds), ScheduleStatus.UNKNOWN, Optional.<String>absent());
  }

  // This number is derived from the maximum file name length limit on most UNIX systems, less
  // the number of characters we've observed being added by mesos for the executor ID, prefix, and
  // delimiters.
  @VisibleForTesting
  static final int MAX_TASK_ID_LENGTH = 255 - 90;

  private void checkTaskIdLength(TaskConfig taskConfig) throws ScheduleException {
    if (taskIdGenerator.apply(taskConfig).length() > MAX_TASK_ID_LENGTH) {
      throw new ScheduleException("Task ID is too long, please shorten your role or job name.");
    }
  }

  @Override
  public synchronized void createJob(ParsedConfiguration parsedConfiguration)
      throws ScheduleException {

    JobConfiguration job = parsedConfiguration.getJobConfig();
    if (hasActiveJob(job)) {
      throw new ScheduleException("Job already exists: " + JobKeys.toPath(job));
    }

    // TODO(William Farner); This is a short-term hack to stop the bleeding from MESOS-3788.
    checkTaskIdLength(Iterables.getFirst(parsedConfiguration.getTaskConfigs(), null));
    ensureHasAdditionalQuota(job.getOwner().getRole(), Quotas.fromJob(job));

    boolean accepted = false;
    for (final JobManager manager : jobManagers) {
      if (manager.receiveJob(parsedConfiguration)) {
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
  public synchronized void startCronJob(JobKey jobKey)
      throws ScheduleException, TaskDescriptionException {

    checkNotNull(jobKey);

    if (!cronScheduler.hasJob(jobKey)) {
      throw new ScheduleException("Cron job does not exist for " + JobKeys.toPath(jobKey));
    }

    cronScheduler.startJobNow(jobKey);
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
        return manager.hasJob(job.getKey());
      }
    };
  }

  @Override
  public synchronized void setTaskStatus(
      Query.Builder query,
      final ScheduleStatus status,
      Optional<String> message) {

    checkNotNull(query);
    checkNotNull(status);

    stateManager.changeState(query, status, message);
  }

  @Override
  public synchronized void killTasks(Query.Builder query, String user) throws ScheduleException {
    checkNotNull(query);
    LOG.info("Killing tasks matching " + query);

    boolean jobDeleted = false;
    boolean updateFinished = false;

    if (Query.isOnlyJobScoped(query)) {
      // If this looks like a query for all tasks in a job, instruct the scheduler modules to
      // delete the job.
      JobKey jobKey = JobKeys.from(query).get();
      for (JobManager manager : jobManagers) {
        if (manager.deleteJob(jobKey)) {
          jobDeleted = true;
        }
      }

      if (!jobDeleted) {
        try {
          updateFinished = stateManager.finishUpdate(
              jobKey,
              user,
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
    Query.Builder taskQuery = query.get().isSetStatuses() ? query.byStatus(ACTIVE_STATES) : query;

    int tasksAffected =
        stateManager.changeState(taskQuery, KILLING, Optional.of("Killed by " + user));
    if (!jobDeleted && !updateFinished && (tasksAffected == 0)) {
      throw new ScheduleException("No jobs to kill");
    }
  }

  @Override
  public void restartShards(
      JobKey jobKey,
      final Set<Integer> shards,
      final String requestingUser) throws ScheduleException {

    if (!JobKeys.isValid(jobKey)) {
      throw new ScheduleException("Invalid job key: " + jobKey);
    }

    if (shards.isEmpty()) {
      throw new ScheduleException("At least one shard must be specified.");
    }

    final Query.Builder query = Query.shardScoped(jobKey, shards).active();
    storage.write(new MutateWork.NoResult<ScheduleException>() {
      @Override protected void execute(MutableStoreProvider storeProvider)
          throws ScheduleException {

        Set<ScheduledTask> matchingTasks = storeProvider.getTaskStore().fetchTasks(query);
        if (matchingTasks.size() != shards.size()) {
          throw new ScheduleException("Not all requested shards are active.");
        }
        LOG.info("Restarting shards matching " + query);
        stateManager.changeState(
            Query.taskScoped(Tasks.ids(matchingTasks)),
            RESTARTING,
            Optional.of("Restarted by " + requestingUser));
      }
    });
  }

  private void ensureHasAdditionalQuota(String role, Quota quota) throws ScheduleException {
    if (!quotaManager.hasRemaining(role, quota)) {
      throw new ScheduleException("Insufficient resource quota.");
    }
  }

  @Override
  public synchronized Optional<String> initiateJobUpdate(
      final ParsedConfiguration parsedConfiguration) throws ScheduleException {

    final JobConfiguration job = parsedConfiguration.getJobConfig();
    final JobKey jobKey = job.getKey();
    if (cronScheduler.hasJob(jobKey)) {
      cronScheduler.updateJob(parsedConfiguration);
      return Optional.absent();
    }

    return storage.write(new MutateWork<Optional<String>, ScheduleException>() {
      @Override public Optional<String> apply(MutableStoreProvider storeProvider)
          throws ScheduleException {
        Query.Builder query = Query.jobScoped(jobKey).active();
        Set<ScheduledTask> existingTasks = storeProvider.getTaskStore().fetchTasks(query);

        // Reject if any existing task for the job is in UPDATING/ROLLBACK
        if (Iterables.any(existingTasks, IS_UPDATING)) {
          throw new ScheduleException("Update/Rollback already in progress for "
              + JobKeys.toPath(job));
        }

        if (!existingTasks.isEmpty()) {
          Quota currentJobQuota = Quotas.fromProductionTasks(
                  Iterables.transform(existingTasks, Tasks.SCHEDULED_TO_INFO));
          Quota newJobQuota = Quotas.fromJob(job);
          Quota additionalQuota = Quotas.subtract(newJobQuota, currentJobQuota);
          ensureHasAdditionalQuota(job.getOwner().getRole(), additionalQuota);
        }

        try {
          return Optional.of(
              stateManager.registerUpdate(jobKey, parsedConfiguration.getTaskConfigs()));
        } catch (UpdateException e) {
          LOG.log(Level.INFO, "Failed to start update.", e);
          throw new ScheduleException(e.getMessage(), e);
        }
      }
    });
  }

  @Override
  public synchronized Map<Integer, ShardUpdateResult> updateShards(
      JobKey jobKey,
      String invokingUser,
      Set<Integer> shards,
      String updateToken) throws ScheduleException {

    try {
      return stateManager.modifyShards(jobKey, invokingUser, shards, updateToken, true);
    } catch (UpdateException e) {
      LOG.log(Level.INFO, "Failed to update shards for " + JobKeys.toPath(jobKey), e);
      throw new ScheduleException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized Map<Integer, ShardUpdateResult> rollbackShards(
      JobKey jobKey,
      String invokingUser,
      Set<Integer> shards,
      String updateToken) throws ScheduleException {

    try {
      return stateManager.modifyShards(jobKey, invokingUser, shards, updateToken, false);
    } catch (UpdateException e) {
      LOG.log(Level.INFO, "Failed to roll back shards for " + JobKeys.toPath(jobKey), e);
      throw new ScheduleException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void finishUpdate(
      JobKey jobKey,
      String invokingUser,
      Optional<String> updateToken,
      UpdateResult result) throws ScheduleException {

    try {
      stateManager.finishUpdate(jobKey, invokingUser, updateToken, result, true);
    } catch (UpdateException e) {
      LOG.log(Level.INFO, "Failed to finish update for " + JobKeys.toPath(jobKey), e);
      throw new ScheduleException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void preemptTask(AssignedTask task, AssignedTask preemptingTask) {
    checkNotNull(task);
    checkNotNull(preemptingTask);
    // TODO(William Farner): Throw SchedulingException if either task doesn't exist, etc.

    stateManager.changeState(Query.taskScoped(task.getTaskId()), ScheduleStatus.PREEMPTING,
        Optional.of("Preempting in favor of " + preemptingTask.getTaskId()));
  }
}
