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
package org.apache.aurora.scheduler.updater;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.scheduler.BatchWorker;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.sla.SlaManager;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ISlaPolicy;
import org.apache.aurora.scheduler.updater.UpdaterModule.UpdateActionBatchWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.base.Tasks.SLAVE_ASSIGNED_STATES;
import static org.apache.aurora.scheduler.base.Tasks.isKillable;

/**
 * This class helps push SLA-aware kills to completion.
 *
 * SLA-aware kills will be added to a queue and retried until SLA passes or the update changes
 * state. If the update becomes PAUSED or AWAITING_PULSE, then the retries will stop until the
 * update becomes active again.
 */
class SlaKillController {
  private static final Logger LOG = LoggerFactory.getLogger(SlaKillController.class);

  @VisibleForTesting
  static final String SLA_CHECKING_MESSAGE = "Checking SLA before continuing.";
  @VisibleForTesting
  static final String SLA_PASSED_MESSAGE = "SLA check passed, continuing.";
  @VisibleForTesting
  static final String SLA_KILL_ATTEMPT = "updates_sla_kill_attempt_";
  @VisibleForTesting
  static final String SLA_KILL_SUCCESS = "updates_sla_kill_success_";

  private final ScheduledExecutorService executor;
  private final UpdateActionBatchWorker batchWorker;
  private final SlaManager slaManager;
  private final BackoffStrategy backoffStrategy;
  private final Clock clock;

  private final LoadingCache<String, AtomicLong> killAttemptsByJob;
  private final LoadingCache<String, AtomicLong> killSuccessesByJob;

  @Inject
  SlaKillController(
      ScheduledExecutorService executor,
      UpdateActionBatchWorker batchWorker,
      SlaManager slaManager,
      Clock clock,
      BackoffStrategy backoffStrategy,
      StatsProvider statsProvider) {

    this.executor = requireNonNull(executor);
    this.batchWorker = requireNonNull(batchWorker);
    this.slaManager = requireNonNull(slaManager);
    this.backoffStrategy = requireNonNull(backoffStrategy);
    this.clock = requireNonNull(clock);

    killAttemptsByJob = CacheBuilder.newBuilder().build(
        new CacheLoader<String, AtomicLong>() {
          @Override
          public AtomicLong load(String key) {
            return statsProvider.makeCounter(key);
          }
        }
    );
    killSuccessesByJob = CacheBuilder.newBuilder().build(
        new CacheLoader<String, AtomicLong>() {
          @Override
          public AtomicLong load(String key) {
            return statsProvider.makeCounter(key);
          }
        }
    );
  }

  /**
   * Perform an SLA-aware kill on a given {@link IInstanceKey instance} and
   * {@link IScheduledTask task} combination. Adds {@link JobInstanceUpdateEvent}s to show progress
   * of the kill (e.g. whether it is waiting for the SLA check to pass or whether it has already
   * succeeded and is in the process of killing). SLA-aware kills are performed asynchronously
   * to calling this method.
   *
   * Kills will retry until:
   *
   *  - The kill is successful.
   *  - The update becomes inactive.
   *  - The task supplied is killed for some other reason.
   *
   * @param storeProvider A storage provider to access/modify update information.
   * @param instance The {@link IInstanceKey} of the task being killed.
   * @param task The task being killed.
   * @param key The update key.
   * @param slaPolicy The {@link ISlaPolicy} to use.
   * @param status The status of the update as of issuing the kill.
   * @param killCommand The consumer provided to execute on a successful SLA check. This consumer
   *    should perform the "kill" of the task (i.e. do the transition to KILLING).
   */
  void slaKill(
      MutableStoreProvider storeProvider,
      IInstanceKey instance,
      IScheduledTask task,
      IJobUpdateKey key,
      ISlaPolicy slaPolicy,
      JobUpdateStatus status,
      Consumer<MutableStoreProvider> killCommand) {

    if (!updateEventExists(
            storeProvider,
            instance,
            key,
            determineActionFromStatus(status),
            SLA_CHECKING_MESSAGE)) {
      // The check above is to ensure we do not add duplicate instance update events since slaKill
      // can be called multiple times for one instance (e.g. slaKill is called, the update is
      // paused, and then slaKill is called again after an unpause).
      addUpdatingInstanceEvent(
          storeProvider,
          instance,
          key,
          clock.nowMillis(),
          determineActionFromStatus(status),
          SLA_CHECKING_MESSAGE);
    }

    retryingSlaKill(
        instance,
        task.getAssignedTask().getTaskId(),
        key,
        slaPolicy,
        status,
        killCommand,
        0L);
  }

  private void retryingSlaKill(
      IInstanceKey instance,
      String taskId,
      IJobUpdateKey key,
      ISlaPolicy slaPolicy,
      JobUpdateStatus status,
      Consumer<MutableStoreProvider> killCommand,
      long retryInMs) {

    batchWorker.execute(storeProvider -> {
      if (updateInStatus(storeProvider, key, status)) {
        // Ensure that the update is still ongoing.
        storeProvider
            .getTaskStore()
            .fetchTask(taskId)
            .filter(task -> isKillable(task.getStatus()))
            .ifPresent(task -> {
              incrementJobStatCounter(killAttemptsByJob, SLA_KILL_ATTEMPT, instance.getJobKey());
              slaManager.checkSlaThenAct(
                  task,
                  slaPolicy,
                  slaStoreProvider -> performKill(
                      slaStoreProvider,
                      instance,
                      key,
                      status,
                      killCommand),
                  ImmutableMap.of(),
                  // If the task is not assigned, force the update since it does not affect the
                  // SLA. For example, if a task is THROTTLED or PENDING, we probably don't care
                  // if the update replaces it with a new instance.
                  !SLAVE_ASSIGNED_STATES.contains(task.getStatus()));

              // We retry all SLA kills since it is possible that the SLA check fails and thus we
              // will want to reevaluate later. If the kill succeeds or the update is cancelled by
              // the next time the kill is retried, it will be a NOOP.
              long backoff = backoffStrategy.calculateBackoffMs(retryInMs);
              executor.schedule(
                  () -> retryingSlaKill(
                      instance,
                      taskId,
                      key,
                      slaPolicy,
                      status,
                      killCommand,
                      backoff),
                  backoff,
                  TimeUnit.MILLISECONDS);
            });
      }

      return BatchWorker.NO_RESULT;
    });
  }

  /**
   * Passed into {@link SlaManager#checkSlaThenAct}. Actually performs the kill on the instance.
   *
   * Ensures that the update is still valid (the update's status has not changed since the kill
   * was initiated) before persisting new state.
   */
  private BatchWorker.NoResult performKill(
      MutableStoreProvider slaStoreProvider,
      IInstanceKey instance,
      IJobUpdateKey key,
      JobUpdateStatus status,
      Consumer<MutableStoreProvider> killCommand) {

    LOG.info("Performing SLA-aware kill of " + instance);
    if (updateInStatus(slaStoreProvider, key, status)) {
      // Check again that the status is the same as when the command was issued. We do this because
      // the SLA kill is executed asynchronously and the status may have changed (due to a pause or
      // waiting for a pulse).
      addUpdatingInstanceEvent(
          slaStoreProvider,
          instance,
          key,
          clock.nowMillis(),
          determineActionFromStatus(status),
          SLA_PASSED_MESSAGE);
      killCommand.accept(slaStoreProvider);
      incrementJobStatCounter(killSuccessesByJob, SLA_KILL_SUCCESS, instance.getJobKey());
    } else {
      // Between issuing the SLA kill and the kill being executed, the update has
      // changed state and is no longer ROLLING_FORWARD. Skip the kill until the
      // update becomes active again.
      LOG.info("Update " + key + " is not active, skipping SLA kill of " + instance);
    }

    return BatchWorker.NO_RESULT;
  }

  private static void incrementJobStatCounter(
      LoadingCache<String, AtomicLong> counter,
      String prefix,
      IJobKey jobKey) {

    counter.getUnchecked(prefix + JobKeys.canonicalString(jobKey)).incrementAndGet();
  }

  /**
   * Checks that an update contains an instance update event with a given instance ID,
   * {@link JobUpdateAction}, and message.
   */
  private static boolean updateEventExists(
      MutableStoreProvider storeProvider,
      IInstanceKey instance,
      IJobUpdateKey key,
      JobUpdateAction action,
      String message) {

    return storeProvider
        .getJobUpdateStore()
        .fetchJobUpdate(key)
        .get()
        .getInstanceEvents()
        .stream()
        .anyMatch(e -> e.getInstanceId() == instance.getInstanceId()
            && e.getAction() == action
            && e.isSetMessage()
            && e.getMessage().equals(message));
  }

  private static void addUpdatingInstanceEvent(
      MutableStoreProvider storeProvider,
      IInstanceKey instance,
      IJobUpdateKey key,
      long timestampMs,
      JobUpdateAction action,
      String message) {

    IJobInstanceUpdateEvent event = IJobInstanceUpdateEvent.build(
        new JobInstanceUpdateEvent()
            .setInstanceId(instance.getInstanceId())
            .setTimestampMs(timestampMs)
            .setAction(action)
            .setMessage(message));
    storeProvider.getJobUpdateStore().saveJobInstanceUpdateEvent(key, event);
  }

  private static boolean updateInStatus(
      MutableStoreProvider storeProvider,
      IJobUpdateKey key,
      JobUpdateStatus status) {

    return storeProvider
        .getJobUpdateStore()
        .fetchJobUpdate(key)
        .filter(jobUpdateDetails -> Updates.getJobUpdateStatus(jobUpdateDetails) == status)
        .isPresent();
  }

  /**
   * Determine the {@link JobUpdateAction} that corresponds with a give {@link JobUpdateStatus}.
   * For example, ROLLING_FORWARD corresponds with INSTANCE_UPDATING actions while ROLLING_BACK
   * corresponds with INSTANCE_ROLLING_BACK actions.
   */
  private static JobUpdateAction determineActionFromStatus(JobUpdateStatus status) {
    switch (status) {
      case ROLLING_BACK:
        return JobUpdateAction.INSTANCE_ROLLING_BACK;
      case ROLLING_FORWARD:
        return JobUpdateAction.INSTANCE_UPDATING;
      default:
        throw new RuntimeException("Unexpected status " + status + " encountered when"
            + " performing an SLA-aware update.");
    }
  }
}
