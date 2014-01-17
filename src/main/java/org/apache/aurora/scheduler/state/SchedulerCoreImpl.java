/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.state;


import java.util.Set;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Positive;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ScheduleException;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.RESTARTING;
import static org.apache.aurora.scheduler.base.Tasks.ACTIVE_STATES;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;

/**
 * Implementation of the scheduler core.
 */
class SchedulerCoreImpl implements SchedulerCore {
  @Positive
  @CmdLine(name = "max_tasks_per_job", help = "Maximum number of allowed tasks in a single job.")
  public static final Arg<Integer> MAX_TASKS_PER_JOB = Arg.create(4000);

  private static final Logger LOG = Logger.getLogger(SchedulerCoreImpl.class.getName());

  private final Storage storage;

  private final CronJobManager cronScheduler;

  // Schedulers that are responsible for triggering execution of jobs.
  private final ImmutableList<JobManager> jobManagers;

  // State manager handles persistence of task modifications and state transitions.
  private final StateManager stateManager;

  private final TaskIdGenerator taskIdGenerator;
  private final QuotaManager quotaManager;

  /**
   * Creates a new core scheduler.
   *
   * @param storage Backing store implementation.
   * @param cronScheduler Cron scheduler.
   * @param immediateScheduler Immediate scheduler.
   * @param stateManager Persistent state manager.
   * @param taskIdGenerator Task ID generator.
   * @param quotaManager Quota manager.
   */
  @Inject
  public SchedulerCoreImpl(
      Storage storage,
      CronJobManager cronScheduler,
      ImmediateJobManager immediateScheduler,
      StateManager stateManager,
      TaskIdGenerator taskIdGenerator,
      QuotaManager quotaManager) {

    this.storage = checkNotNull(storage);

    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    this.jobManagers = ImmutableList.of(cronScheduler, immediateScheduler);
    this.cronScheduler = cronScheduler;
    this.stateManager = checkNotNull(stateManager);
    this.taskIdGenerator = checkNotNull(taskIdGenerator);
    this.quotaManager = checkNotNull(quotaManager);
  }

  private boolean hasActiveJob(IJobConfiguration job) {
    return Iterables.any(jobManagers, managerHasJob(job));
  }

  @Override
  public synchronized void tasksDeleted(Set<String> taskIds) {
    for (String taskId : taskIds) {
      setTaskStatus(taskId, ScheduleStatus.UNKNOWN, Optional.<String>absent());
    }
  }

  @Override
  public synchronized void createJob(final SanitizedConfiguration sanitizedConfiguration)
      throws ScheduleException {

    storage.write(new MutateWork.NoResult<ScheduleException>() {
      @Override protected void execute(MutableStoreProvider storeProvider)
          throws ScheduleException {

        final IJobConfiguration job = sanitizedConfiguration.getJobConfig();
        if (hasActiveJob(job)) {
          throw new ScheduleException("Job already exists: " + JobKeys.toPath(job));
        }

        validateTaskLimits(job.getTaskConfig(), job.getInstanceCount());

        boolean accepted = false;
        // TODO(wfarner): Remove the JobManager abstraction, and directly invoke addInstances
        // here for non-cron jobs.
        for (final JobManager manager : jobManagers) {
          if (manager.receiveJob(sanitizedConfiguration)) {
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
    });
  }

  // This number is derived from the maximum file name length limit on most UNIX systems, less
  // the number of characters we've observed being added by mesos for the executor ID, prefix, and
  // delimiters.
  @VisibleForTesting
  static final int MAX_TASK_ID_LENGTH = 255 - 90;

  /**
   * Validates task specific requirements including name, count and quota checks.
   * Must be performed inside of a write storage transaction along with state mutation change
   * to avoid any data race conditions.
   *
   * @param task Task configuration.
   * @param instances Number of task instances
   * @throws ScheduleException If validation fails.
   */
  private void validateTaskLimits(ITaskConfig task, int instances)
      throws ScheduleException {

    // TODO(maximk): This is a short-term hack to stop the bleeding from
    //               https://issues.apache.org/jira/browse/MESOS-691
    if (taskIdGenerator.generate(task, instances).length() > MAX_TASK_ID_LENGTH) {
      throw new ScheduleException(
          "Task ID is too long, please shorten your role or job name.");
    }

    if (instances > MAX_TASKS_PER_JOB.get()) {
      throw new ScheduleException("Job exceeds task limit of " + MAX_TASKS_PER_JOB.get());
    }

    QuotaCheckResult quotaCheck = quotaManager.checkQuota(task, instances);
    if (quotaCheck.getResult() == INSUFFICIENT_QUOTA) {
      throw new ScheduleException("Insufficient resource quota: " + quotaCheck.getDetails());
    }
  }

  @Override
  public void validateJobResources(SanitizedConfiguration sanitizedConfiguration)
      throws ScheduleException {

    IJobConfiguration job = sanitizedConfiguration.getJobConfig();
    validateTaskLimits(job.getTaskConfig(), job.getInstanceCount());
  }

  @Override
  public void addInstances(
      final IJobKey jobKey,
      final ImmutableSet<Integer> instanceIds,
      final ITaskConfig config) throws ScheduleException {

    storage.write(new MutateWork.NoResult<ScheduleException>() {
      @Override protected void execute(MutableStoreProvider storeProvider)
          throws ScheduleException {

        validateTaskLimits(config, instanceIds.size());

        ImmutableSet<IScheduledTask> tasks =
            storeProvider.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active());

        Set<Integer> existingInstanceIds =
            FluentIterable.from(tasks).transform(Tasks.SCHEDULED_TO_INSTANCE_ID).toSet();
        if (!Sets.intersection(existingInstanceIds, instanceIds).isEmpty()) {
          throw new ScheduleException("Instance ID collision detected.");
        }

        stateManager.insertPendingTasks(Maps.asMap(instanceIds, Functions.constant(config)));
      }
    });
  }

  @Override
  public synchronized void startCronJob(IJobKey jobKey)
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
  private static Predicate<JobManager> managerHasJob(final IJobConfiguration job) {
    return new Predicate<JobManager>() {
      @Override public boolean apply(JobManager manager) {
        return manager.hasJob(job.getKey());
      }
    };
  }

  @Override
  public synchronized void setTaskStatus(
      String taskId,
      final ScheduleStatus status,
      Optional<String> message) {

    checkNotNull(taskId);
    checkNotNull(status);

    stateManager.changeState(taskId, Optional.<ScheduleStatus>absent(), status, message);
  }

  @Override
  public synchronized void killTasks(Query.Builder query, final String user)
      throws ScheduleException {

    checkNotNull(query);
    LOG.info("Killing tasks matching " + query);

    boolean jobDeleted = false;

    if (Query.isOnlyJobScoped(query)) {
      // If this looks like a query for all tasks in a job, instruct the scheduler modules to
      // delete the job.
      IJobKey jobKey = JobKeys.from(query).get();
      for (JobManager manager : jobManagers) {
        if (manager.deleteJob(jobKey)) {
          jobDeleted = true;
        }
      }
    }

    // Unless statuses were specifically supplied, only attempt to kill active tasks.
    final Query.Builder taskQuery = query.get().isSetStatuses()
        ? query.byStatus(ACTIVE_STATES)
        : query;

    int tasksAffected = storage.write(new MutateWork.Quiet<Integer>() {
      @Override public Integer apply(MutableStoreProvider storeProvider) {
        int total = 0;
        for (String taskId : Tasks.ids(storeProvider.getTaskStore().fetchTasks(taskQuery))) {
          boolean changed = stateManager.changeState(
              taskId,
              Optional.<ScheduleStatus>absent(),
              KILLING,
              Optional.of("Killed by " + user));

          if (changed) {
            total++;
          }
        }
        return total;
      }
    });

    if (!jobDeleted && (tasksAffected == 0)) {
      throw new ScheduleException("No jobs to kill");
    }
  }

  @Override
  public void restartShards(
      IJobKey jobKey,
      final Set<Integer> shards,
      final String requestingUser) throws ScheduleException {

    if (!JobKeys.isValid(jobKey)) {
      throw new ScheduleException("Invalid job key: " + jobKey);
    }

    if (shards.isEmpty()) {
      throw new ScheduleException("At least one shard must be specified.");
    }

    final Query.Builder query = Query.instanceScoped(jobKey, shards).active();
    storage.write(new MutateWork.NoResult<ScheduleException>() {
      @Override protected void execute(MutableStoreProvider storeProvider)
          throws ScheduleException {

        Set<IScheduledTask> matchingTasks = storeProvider.getTaskStore().fetchTasks(query);
        if (matchingTasks.size() != shards.size()) {
          throw new ScheduleException("Not all requested shards are active.");
        }
        LOG.info("Restarting shards matching " + query);
        for (String taskId : Tasks.ids(matchingTasks)) {
          stateManager.changeState(
              taskId,
              Optional.<ScheduleStatus>absent(),
              RESTARTING,
              Optional.of("Restarted by " + requestingUser));
        }
      }
    });
  }
}
