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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_PAUSED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_PAUSED;
import static org.apache.aurora.scheduler.storage.Storage.MutateWork;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.ACTIVE_QUERY;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_BACK;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_FORWARD;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.STOP_WATCHING;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.assertTransitionAllowed;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.EvaluationResult;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.OneWayStatus;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.OneWayStatus.SUCCEEDED;

/**
 * Implementation of an updater that orchestrates the process of gradually updating the
 * configuration of tasks in a job.
 * >p>
 * TODO(wfarner): Consider using AbstractIdleService here.
 */
class JobUpdateControllerImpl implements JobUpdateController {
  private static final Logger LOG = Logger.getLogger(JobUpdateControllerImpl.class.getName());

  private final UpdateFactory updateFactory;
  private final LockManager lockManager;
  private final Storage storage;
  private final ScheduledExecutorService executor;
  private final StateManager stateManager;
  private final Clock clock;

  // Currently-active updaters. An active updater is one that is rolling forward or back. Paused
  // and completed updates are represented only in storage, not here.
  private final Map<IJobKey, UpdateFactory.Update> updates =
      Collections.synchronizedMap(Maps.<IJobKey, UpdateFactory.Update>newHashMap());

  @Inject
  JobUpdateControllerImpl(
      UpdateFactory updateFactory,
      LockManager lockManager,
      Storage storage,
      ScheduledExecutorService executor,
      StateManager stateManager,
      Clock clock) {

    this.updateFactory = requireNonNull(updateFactory);
    this.lockManager = requireNonNull(lockManager);
    this.storage = requireNonNull(storage);
    this.executor = requireNonNull(executor);
    this.stateManager = requireNonNull(stateManager);
    this.clock = requireNonNull(clock);
  }

  @Override
  public void start(final IJobUpdate update, final String updatingUser)
      throws UpdateStateException, UpdateConfigurationException {

    requireNonNull(update);
    requireNonNull(updatingUser);

    // Validate the update configuration by making sure we can create an updater for it.
    updateFactory.newUpdate(update.getConfiguration(), true);

    storage.write(new MutateWork.NoResult<UpdateStateException>() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider)
          throws UpdateStateException {

        IJobUpdateSummary summary = update.getSummary();
        IJobKey job = summary.getJobKey();
        ILock lock;
        try {
          lock =
              lockManager.acquireLock(ILockKey.build(LockKey.job(job.newBuilder())), updatingUser);
        } catch (LockException e) {
          throw new UpdateStateException(e.getMessage(), e);
        }

        storeProvider.getJobUpdateStore().saveJobUpdate(update, lock.getToken());
        recordAndChangeJobUpdateStatus(
            storeProvider.getJobUpdateStore(),
            storeProvider.getTaskStore(),
            summary.getUpdateId(),
            job,
            ROLLING_FORWARD);
      }
    });
  }

  @Override
  public void pause(final IJobKey job) throws UpdateStateException {
    requireNonNull(job);
    unscopedChangeUpdateStatus(job, JobUpdateStateMachine.GET_PAUSE_STATE);
  }

  public void resume(IJobKey job) throws UpdateStateException {
    requireNonNull(job);
    unscopedChangeUpdateStatus(job, JobUpdateStateMachine.GET_RESUME_STATE);
  }

  @Override
  public void abort(IJobKey job) throws UpdateStateException {
    requireNonNull(job);
    unscopedChangeUpdateStatus(job, new Function<JobUpdateStatus, JobUpdateStatus>() {
      @Override
      public JobUpdateStatus apply(JobUpdateStatus input) {
        return ABORTED;
      }
    });
  }

  @Override
  public void systemResume() {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        for (IJobUpdateSummary summary
            : storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(ACTIVE_QUERY)) {

          LOG.info("Automatically resuming update " + JobKeys.canonicalString(summary.getJobKey()));
          changeJobUpdateStatus(
              storeProvider.getJobUpdateStore(),
              storeProvider.getTaskStore(),
              summary.getUpdateId(),
              summary.getJobKey(),
              summary.getState().getStatus(),
              false);
        }
      }
    });
  }

  @Override
  public void instanceChangedState(final IScheduledTask updatedTask) {
    instanceChanged(
        InstanceKeys.from(
            JobKeys.from(updatedTask.getAssignedTask().getTask()),
            updatedTask.getAssignedTask().getInstanceId()),
        Optional.of(updatedTask));
  }

  @Override
  public void instanceDeleted(IInstanceKey instance) {
    // This is primarily used to detect when an instance was stuck in PENDING and killed, which
    // results in deletion.
    instanceChanged(instance, Optional.<IScheduledTask>absent());
  }

  private void instanceChanged(final IInstanceKey instance, final Optional<IScheduledTask> state) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        IJobKey job = instance.getJobKey();
        UpdateFactory.Update update = updates.get(job);
        if (update != null) {
          if (update.getUpdater().containsInstance(instance.getInstanceId())) {
            LOG.info("Forwarding task change for " + InstanceKeys.toString(instance));
            evaluateUpdater(
                storeProvider.getJobUpdateStore(),
                storeProvider.getTaskStore(),
                update,
                getOnlyMatch(storeProvider.getJobUpdateStore(), queryByJob(job)),
                ImmutableMap.of(instance.getInstanceId(), state));
          } else {
            LOG.info("Instance " + instance + " is not part of active update for "
                + JobKeys.canonicalString(job));
          }
        }
      }
    });
  }

  private IJobUpdateSummary getOnlyMatch(JobUpdateStore store, IJobUpdateQuery query) {
    return Iterables.getOnlyElement(store.fetchJobUpdateSummaries(query));
  }

  @VisibleForTesting
  static IJobUpdateQuery queryByJob(IJobKey job) {
    return IJobUpdateQuery.build(new JobUpdateQuery()
        .setJobKey(job.newBuilder())
        .setUpdateStatuses(ImmutableSet.of(
            ROLLING_FORWARD,
            ROLLING_BACK,
            ROLL_FORWARD_PAUSED,
            ROLL_BACK_PAUSED)));
  }

  @VisibleForTesting
  static IJobUpdateQuery queryByUpdateId(String updateId) {
    return IJobUpdateQuery.build(new JobUpdateQuery()
        .setUpdateId(updateId));
  }

  /**
   * Changes the state of an update, without the 'scope' of an update ID.  This should only be used
   * when responding to outside inputs that are inherently un-scoped, such as a user action or task
   * state change.
   *
   * @param job Job whose update state should be changed.
   * @param stateChange State change computation, based on the current state of the update.
   * @throws UpdateStateException If no active update exists for the provided {@code job}, or
   *                              if the proposed state transition is not allowed.
   */
  private void unscopedChangeUpdateStatus(
      final IJobKey job,
      final Function<JobUpdateStatus, JobUpdateStatus> stateChange)
      throws UpdateStateException {

    storage.write(new MutateWork.NoResult<UpdateStateException>() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider)
          throws UpdateStateException {

        IJobUpdateSummary update = Iterables.getOnlyElement(
            storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(queryByJob(job)), null);
        if (update == null) {
          throw new UpdateStateException("There is no active update for " + job);
        }

        JobUpdateStatus status = update.getState().getStatus();
        JobUpdateStatus newStatus = requireNonNull(stateChange.apply(status));
        changeUpdateStatus(
            storeProvider.getJobUpdateStore(),
            storeProvider.getTaskStore(),
            update,
            newStatus);
      }
    });
  }

  private void changeUpdateStatus(
      JobUpdateStore.Mutable updateStore,
      TaskStore taskStore,
      IJobUpdateSummary updateSummary,
      JobUpdateStatus newStatus) {

    if (updateSummary.getState().getStatus() == newStatus) {
      return;
    }

    assertTransitionAllowed(updateSummary.getState().getStatus(), newStatus);
    recordAndChangeJobUpdateStatus(
        updateStore,
        taskStore,
        updateSummary.getUpdateId(),
        updateSummary.getJobKey(),
        newStatus);
  }

  private void recordAndChangeJobUpdateStatus(
      JobUpdateStore.Mutable updateStore,
      TaskStore taskStore,
      String updateId,
      IJobKey job,
      JobUpdateStatus status) {

    changeJobUpdateStatus(updateStore, taskStore, updateId, job, status, true);
  }

  private static final Set<JobUpdateStatus> UNLOCK_STATES = ImmutableSet.of(
      ROLLED_FORWARD,
      ROLLED_BACK,
      ABORTED,
      JobUpdateStatus.FAILED,
      ERROR
  );

  private void changeJobUpdateStatus(
      JobUpdateStore.Mutable updateStore,
      TaskStore taskStore,
      String updateId,
      IJobKey job,
      JobUpdateStatus newStatus,
      boolean recordChange) {

    JobUpdateStatus status;
    boolean record;

    Optional<String> updateLock = updateStore.getLockToken(updateId);
    if (updateLock.isPresent()) {
      status = newStatus;
      record = recordChange;
    } else {
      LOG.severe("Update " + updateId + " does not have a lock");
      status = ERROR;
      record = true;
    }

    LOG.info(String.format(
        "Update %s %s is now in state %s", JobKeys.canonicalString(job), updateId, status));
    if (record) {
      updateStore.saveJobUpdateEvent(
          IJobUpdateEvent.build(new JobUpdateEvent()
              .setStatus(status)
              .setTimestampMs(clock.nowMillis())),
          updateId);
    }

    if (UNLOCK_STATES.contains(status) && updateLock.isPresent()) {
      lockManager.releaseLock(ILock.build(new Lock()
          .setKey(LockKey.job(job.newBuilder()))
          .setToken(updateLock.get())));
    }

    MonitorAction action = JobUpdateStateMachine.getActionForStatus(status);
    if (action == STOP_WATCHING) {
      updates.remove(job);
    } else if (action == ROLL_FORWARD || action == ROLL_BACK) {
      if (action == ROLL_BACK) {
        updates.remove(job);
      } else {
        checkState(!updates.containsKey(job), "Updater already exists for " + job);
      }

      IJobUpdate jobUpdate = updateStore.fetchJobUpdate(updateId).get();
      UpdateFactory.Update update;
      try {
        update = updateFactory.newUpdate(jobUpdate.getConfiguration(), action == ROLL_FORWARD);
      } catch (UpdateConfigurationException | RuntimeException e) {
        changeJobUpdateStatus(updateStore, taskStore, updateId, job, ERROR, true);
        return;
      }
      updates.put(job, update);
      evaluateUpdater(
          updateStore,
          taskStore,
          update,
          jobUpdate.getSummary(),
          ImmutableMap.<Integer, Optional<IScheduledTask>>of());
    }
  }

  private static Optional<IScheduledTask> getActiveInstance(
      TaskStore taskStore,
      IJobKey job,
      int instanceId) {

    return Optional.fromNullable(Iterables.getOnlyElement(
        taskStore.fetchTasks(Query.instanceScoped(job, instanceId).active()), null));
  }

  private void evaluateUpdater(
      JobUpdateStore.Mutable updateStore,
      final TaskStore taskStore,
      final UpdateFactory.Update update,
      IJobUpdateSummary summary,
      Map<Integer, Optional<IScheduledTask>> changedInstance) {

    JobUpdateStatus updaterStatus = summary.getState().getStatus();
    final IJobKey job = summary.getJobKey();

    if (!updateStore.getLockToken(summary.getUpdateId()).isPresent()) {
      recordAndChangeJobUpdateStatus(
          updateStore,
          taskStore,
          summary.getUpdateId(),
          job,
          ERROR);
      return;
    }

    InstanceStateProvider<Integer, Optional<IScheduledTask>> stateProvider =
        new InstanceStateProvider<Integer, Optional<IScheduledTask>>() {
          @Override
          public Optional<IScheduledTask> getState(Integer instanceId) {
            return getActiveInstance(taskStore, job, instanceId);
          }
        };

    EvaluationResult<Integer> result = update.getUpdater().evaluate(changedInstance, stateProvider);
    LOG.info(JobKeys.canonicalString(job) + " evaluation result: " + result);
    OneWayStatus status = result.getStatus();
    if (status == SUCCEEDED || status == OneWayStatus.FAILED) {
      Preconditions.checkArgument(
          result.getInstanceActions().isEmpty(),
          "A terminal state should not specify actions.");

      if (status == SUCCEEDED) {
        changeUpdateStatus(updateStore, taskStore, summary, update.getSuccessStatus());
      } else {
        changeUpdateStatus(updateStore, taskStore, summary, update.getFailureStatus());
      }
    } else if (result.getInstanceActions().isEmpty()) {
      LOG.info("No actions to perform at this time for update of " + job);
    } else {
      for (Map.Entry<Integer, InstanceAction> entry : result.getInstanceActions().entrySet()) {
        IInstanceKey instance = InstanceKeys.from(job, entry.getKey());
        Optional<InstanceActionHandler> handler = entry.getValue().getHandler();
        if (handler.isPresent()) {
          Amount<Long, Time> reevaluateDelay = handler.get().getReevaluationDelay(
              instance,
              updateStore.fetchJobUpdateConfiguration(summary.getUpdateId()).get(),
              taskStore,
              stateManager,
              updaterStatus);
          executor.schedule(
              getDeferredEvaluator(instance, summary.getUpdateId()),
              reevaluateDelay.getValue(),
              reevaluateDelay.getUnit().getTimeUnit());
        }
      }
    }
  }

  private Runnable getDeferredEvaluator(final IInstanceKey instance, final String updateId) {
    return new Runnable() {
      @Override
      public void run() {
        storage.write(new MutateWork.NoResult.Quiet() {
          @Override
          protected void execute(Storage.MutableStoreProvider storeProvider) {
            IJobUpdateSummary summary =
                getOnlyMatch(storeProvider.getJobUpdateStore(), queryByUpdateId(updateId));
            JobUpdateStatus status = summary.getState().getStatus();
            // Suppress this evaluation if the updater is not currently active.
            if (JobUpdateStateMachine.isActive(status)) {
              UpdateFactory.Update update = updates.get(instance.getJobKey());
              evaluateUpdater(
                  storeProvider.getJobUpdateStore(),
                  storeProvider.getTaskStore(),
                  update,
                  summary,
                  ImmutableMap.of(
                      instance.getInstanceId(),
                      getActiveInstance(
                          storeProvider.getTaskStore(),
                          instance.getJobKey(),
                          instance.getInstanceId())));
            }
          }
        });
      }
    };
  }
}
