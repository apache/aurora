/**
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
package org.apache.aurora.scheduler.cron.quartz;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.BackoffHelper;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.scheduler.BatchWorker;
import org.apache.aurora.scheduler.BatchWorker.NoResult;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

import static org.apache.aurora.gen.ScheduleStatus.KILLING;

/**
 * Encapsulates the logic behind a single trigger of a single job key. Multiple executions may run
 * concurrently but only a single instance will be active at a time per job key.
 *
 * <p>
 * Executions may block for long periods of time when waiting for a kill to complete. The Quartz
 * scheduler should therefore be configured with a large number of threads.
 */
@DisallowConcurrentExecution
@PersistJobDataAfterExecution
class AuroraCronJob implements Job, EventSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(AuroraCronJob.class);

  private static final AtomicLong CRON_JOB_TRIGGERS = Stats.exportLong("cron_job_triggers");
  private static final AtomicLong CRON_JOB_MISFIRES = Stats.exportLong("cron_job_misfires");
  private static final AtomicLong CRON_JOB_PARSE_FAILURES =
      Stats.exportLong("cron_job_parse_failures");
  private static final AtomicLong CRON_JOB_COLLISIONS = Stats.exportLong("cron_job_collisions");
  private static final AtomicLong CRON_JOB_CONCURRENT_RUNS =
      Stats.exportLong("cron_job_concurrent_runs");

  @VisibleForTesting
  static final Optional<String> KILL_AUDIT_MESSAGE = Optional.of("Killed by cronScheduler");

  private final ConfigurationManager configurationManager;
  private final StateManager stateManager;
  private final BackoffHelper delayedStartBackoff;
  private final BatchWorker<NoResult> batchWorker;
  private final Set<IJobKey> killFollowups = Sets.newConcurrentHashSet();

  /**
   * Annotation for the max cron batch size.
   */
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface CronMaxBatchSize { }

  static class CronBatchWorker extends BatchWorker<NoResult> {
    @Inject
    CronBatchWorker(
        Storage storage,
        StatsProvider statsProvider,
        @CronMaxBatchSize int maxBatchSize) {

      super(storage, statsProvider, maxBatchSize);
    }

    @Override
    protected String serviceName() {
      return "CronBatchWorker";
    }
  }

  @Inject
  AuroraCronJob(
      ConfigurationManager configurationManager,
      Config config,
      StateManager stateManager,
      CronBatchWorker batchWorker) {

    this.configurationManager = requireNonNull(configurationManager);
    this.stateManager = requireNonNull(stateManager);
    this.batchWorker = requireNonNull(batchWorker);
    this.delayedStartBackoff = requireNonNull(config.getDelayedStartBackoff());
  }

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    // We assume quartz prevents concurrent runs of this job for a given job key. This allows us
    // to avoid races where we might kill another run's tasks.
    checkState(context.getJobDetail().isConcurrentExectionDisallowed());

    doExecute(context);
  }

  @VisibleForTesting
  void doExecute(JobExecutionContext context) throws JobExecutionException {
    final IJobKey key = Quartz.auroraJobKey(context.getJobDetail().getKey());
    final String path = JobKeys.canonicalString(key);

    // Prevent a concurrent run for this job in case a previous trigger took longer to run.
    // This approach relies on saving the "work in progress" token within the job context itself
    // (see below) and relying on killFollowups to signal "work completion".
    if (context.getJobDetail().getJobDataMap().containsKey(path)) {
      CRON_JOB_CONCURRENT_RUNS.incrementAndGet();
      if (killFollowups.contains(key)) {
        context.getJobDetail().getJobDataMap().remove(path);
        killFollowups.remove(key);
        LOG.info("Resetting job context for cron {}", path);
      } else {
        LOG.info("Ignoring trigger as another concurrent run is active for cron {}", path);
        return;
      }
    }

    CompletableFuture<NoResult> scheduleResult = batchWorker.<NoResult>execute(storeProvider -> {
      Optional<IJobConfiguration> config = storeProvider.getCronJobStore().fetchJob(key);
      if (!config.isPresent()) {
        LOG.warn("Cron was triggered for {} but no job with that key was found in storage.", path);
        CRON_JOB_MISFIRES.incrementAndGet();
        return BatchWorker.NO_RESULT;
      }

      SanitizedCronJob cronJob;
      try {
        cronJob = SanitizedCronJob.fromUnsanitized(configurationManager, config.get());
      } catch (ConfigurationManager.TaskDescriptionException | CronException e) {
        LOG.warn("Invalid cron job for {} in storage - failed to parse", key, e);
        CRON_JOB_PARSE_FAILURES.incrementAndGet();
        return BatchWorker.NO_RESULT;
      }

      CronCollisionPolicy collisionPolicy = cronJob.getCronCollisionPolicy();
      LOG.info("Cron triggered for {} at {} with policy {}", path, new Date(), collisionPolicy);
      CRON_JOB_TRIGGERS.incrementAndGet();

      final Query.Builder activeQuery = Query.jobScoped(key).active();
      Set<String> activeTasks = Tasks.ids(storeProvider.getTaskStore().fetchTasks(activeQuery));

      ITaskConfig task = cronJob.getSanitizedConfig().getJobConfig().getTaskConfig();
      Set<Integer> instanceIds = cronJob.getSanitizedConfig().getInstanceIds();
      if (activeTasks.isEmpty()) {
        stateManager.insertPendingTasks(storeProvider, task, instanceIds);
        return BatchWorker.NO_RESULT;
      }

      CRON_JOB_COLLISIONS.incrementAndGet();
      switch (collisionPolicy) {
        case KILL_EXISTING:
          for (String taskId : activeTasks) {
            stateManager.changeState(
                storeProvider,
                taskId,
                Optional.absent(),
                KILLING,
                KILL_AUDIT_MESSAGE);
          }

          LOG.info("Waiting for job to terminate before launching cron job " + path);
          // Use job detail map to signal a "work in progress" condition to subsequent triggers.
          context.getJobDetail().getJobDataMap().put(path, null);
          batchWorker.executeWithReplay(
              delayedStartBackoff.getBackoffStrategy(),
              store -> {
                Query.Builder query = Query.taskScoped(activeTasks).active();
                if (Iterables.isEmpty(storeProvider.getTaskStore().fetchTasks(query))) {
                  LOG.info("Initiating delayed launch of cron " + path);
                  stateManager.insertPendingTasks(store, task, instanceIds);
                  return new BatchWorker.Result<>(true, null);
                } else {
                  LOG.info("Not yet safe to run cron " + path);
                  return new BatchWorker.Result<>(false, null);
                }
              })
              .thenAccept(ignored -> {
                killFollowups.add(key);
                LOG.info("Finished delayed launch for cron " + path);
              });
          break;

        case RUN_OVERLAP:
          LOG.error("Ignoring trigger for job {} with deprecated collision"
              + "policy RUN_OVERLAP due to unterminated active tasks.", path);
          break;

        case CANCEL_NEW:
          break;

        default:
          LOG.error("Unrecognized cron collision policy: " + collisionPolicy);
      }
      return BatchWorker.NO_RESULT;
    });

    try {
      scheduleResult.get();
    } catch (ExecutionException | InterruptedException e) {
      LOG.warn("Interrupted while trying to launch cron " + path, e);
      Thread.currentThread().interrupt();
      throw new JobExecutionException(e);
    }
  }

  static class Config {
    private final BackoffHelper delayedStartBackoff;

    Config(BackoffHelper delayedStartBackoff) {
      this.delayedStartBackoff = requireNonNull(delayedStartBackoff);
    }

    public BackoffHelper getDelayedStartBackoff() {
      return delayedStartBackoff;
    }
  }
}
