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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import com.twitter.common.collections.Pair;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdatePulseStatus;
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
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
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
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.aurora.scheduler.storage.Storage.MutateWork;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.ACTIVE_QUERY;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.GET_ACTIVE_RESUME_STATE;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.GET_BLOCKED_RESUME_STATE;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.GET_PAUSE_STATE;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.GET_UNBLOCKED_STATE;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_BACK;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_FORWARD;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.STOP_WATCHING;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.assertTransitionAllowed;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.getBlockedState;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.EvaluationResult;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.OneWayStatus;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.OneWayStatus.SUCCEEDED;
import static org.apache.aurora.scheduler.updater.SideEffect.InstanceUpdateStatus;

/**
 * Implementation of an updater that orchestrates the process of gradually updating the
 * configuration of tasks in a job.
 * >p>
 * TODO(wfarner): Consider using AbstractIdleService here.
 */
class JobUpdateControllerImpl implements JobUpdateController {
  private static final Logger LOG = Logger.getLogger(JobUpdateControllerImpl.class.getName());
  private static final String INTERNAL_USER = "Aurora Updater";
  private static final Optional<String> NO_USER = Optional.absent();

  private final UpdateFactory updateFactory;
  private final LockManager lockManager;
  private final Storage storage;
  private final ScheduledExecutorService executor;
  private final StateManager stateManager;
  private final Clock clock;
  private final PulseHandler pulseHandler;

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
    this.pulseHandler = new PulseHandler(clock);
  }

  @Override
  public void start(final IJobUpdate update, final String updatingUser)
      throws UpdateStateException {

    requireNonNull(update);
    requireNonNull(updatingUser);

    storage.write(new MutateWork.NoResult<UpdateStateException>() {
      @Override
      protected void execute(MutableStoreProvider storeProvider)
          throws UpdateStateException {

        IJobUpdateSummary summary = update.getSummary();
        IJobUpdateInstructions instructions = update.getInstructions();
        IJobKey job = summary.getJobKey();

        // Validate the update configuration by making sure we can create an updater for it.
        updateFactory.newUpdate(update.getInstructions(), true);

        if (instructions.getInitialState().isEmpty() && !instructions.isSetDesiredState()) {
          throw new IllegalArgumentException("Update instruction is a no-op.");
        }

        List<IJobUpdateSummary> activeJobUpdates =
            storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(queryActiveByJob(job));
        if (!activeJobUpdates.isEmpty()) {
          throw new UpdateStateException("An active update already exists for this job, "
              + "please terminate it before starting another. "
              + "Active updates are those in states " + ACTIVE_JOB_UPDATE_STATES);
        }

        LOG.info("Starting update for job " + job);
        ILock lock;
        try {
          lock =
              lockManager.acquireLock(ILockKey.build(LockKey.job(job.newBuilder())), updatingUser);
        } catch (LockException e) {
          throw new UpdateStateException(e.getMessage(), e);
        }

        storeProvider.getJobUpdateStore().saveJobUpdate(
            update,
            Optional.of(requireNonNull(lock.getToken())));

        JobUpdateStatus status = ROLLING_FORWARD;
        if (isCoordinatedUpdate(instructions)) {
          status = ROLL_FORWARD_AWAITING_PULSE;
          pulseHandler.initializePulseState(update, status);
        }

        recordAndChangeJobUpdateStatus(
            storeProvider,
            summary.getUpdateId(),
            job,
            status,
            Optional.of(updatingUser));
      }
    });
  }

  @Override
  public void pause(final IJobKey job, String user) throws UpdateStateException {
    requireNonNull(job);
    LOG.info("Attempting to pause update for " + job);
    unscopedChangeUpdateStatus(job, GET_PAUSE_STATE, Optional.of(user));
  }

  @Override
  public void resume(final IJobKey job, final String user) throws UpdateStateException {
    requireNonNull(job);
    LOG.info("Attempting to resume update for " + job);
    storage.write(new MutateWork.NoResult<UpdateStateException>() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) throws UpdateStateException {
        IJobUpdateDetails details = Iterables.getOnlyElement(
            storeProvider.getJobUpdateStore().fetchJobUpdateDetails(queryActiveByJob(job)), null);

        if (details == null) {
          throw new UpdateStateException("There is no active update for " + job);
        }

        IJobUpdate update = details.getUpdate();
        String updateId = update.getSummary().getUpdateId();
        Function<JobUpdateStatus, JobUpdateStatus> stateChange =
            isCoordinatedAndPulseExpired(updateId, update.getInstructions())
                ? GET_BLOCKED_RESUME_STATE
                : GET_ACTIVE_RESUME_STATE;

        JobUpdateStatus newStatus = stateChange.apply(update.getSummary().getState().getStatus());
        changeUpdateStatus(storeProvider, update.getSummary(), newStatus, Optional.of(user));
      }
    });
  }

  @Override
  public void abort(IJobKey job, String user) throws UpdateStateException {
    requireNonNull(job);
    unscopedChangeUpdateStatus(job, new Function<JobUpdateStatus, JobUpdateStatus>() {
      @Override
      public JobUpdateStatus apply(JobUpdateStatus input) {
        return ABORTED;
      }
    }, Optional.of(user));
  }

  @Override
  public void systemResume() {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        for (IJobUpdateDetails details
            : storeProvider.getJobUpdateStore().fetchJobUpdateDetails(ACTIVE_QUERY)) {

          IJobUpdateSummary summary = details.getUpdate().getSummary();
          IJobUpdateInstructions instructions = details.getUpdate().getInstructions();
          String updateId = summary.getUpdateId();
          IJobKey job = summary.getJobKey();
          JobUpdateStatus status = summary.getState().getStatus();

          LOG.info("Automatically resuming update " + JobKeys.canonicalString(job));

          if (isCoordinatedUpdate(instructions)) {
            pulseHandler.initializePulseState(details.getUpdate(), status);
          }

          changeJobUpdateStatus(storeProvider, updateId, job, status, NO_USER, false);
        }
      }
    });
  }

  @Override
  public JobUpdatePulseStatus pulse(final String updateId) throws UpdateStateException {
    final PulseState state = pulseHandler.pulseAndGet(updateId);
    if (state == null) {
      LOG.info("Not pulsing inactive job update: " + updateId);
      return JobUpdatePulseStatus.FINISHED;
    }

    LOG.info(String.format(
        "Job update %s has been pulsed. Timeout of %d msec is reset.",
        updateId,
        state.getPulseTimeoutMs()));

    if (JobUpdateStateMachine.isAwaitingPulse(state.getStatus())) {
      // Attempt to unblock a job update previously blocked on expired pulse.
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            unscopedChangeUpdateStatus(
                state.getJobKey(),
                GET_UNBLOCKED_STATE,
                Optional.of(INTERNAL_USER));
          } catch (UpdateStateException e) {
            LOG.severe("Error while processing job update pulse: " + e);
          }
        }
      });
    }

    return JobUpdatePulseStatus.OK;
  }

  @Override
  public void instanceChangedState(final IScheduledTask updatedTask) {
    instanceChanged(
        InstanceKeys.from(
            updatedTask.getAssignedTask().getTask().getJob(),
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
      protected void execute(MutableStoreProvider storeProvider) {
        IJobKey job = instance.getJobKey();
        UpdateFactory.Update update = updates.get(job);
        if (update != null) {
          if (update.getUpdater().containsInstance(instance.getInstanceId())) {
            LOG.info("Forwarding task change for " + InstanceKeys.toString(instance));
            evaluateUpdater(
                storeProvider,
                update,
                getOnlyMatch(storeProvider.getJobUpdateStore(), queryActiveByJob(job)),
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
  static IJobUpdateQuery queryActiveByJob(IJobKey job) {
    return IJobUpdateQuery.build(new JobUpdateQuery()
        .setJobKey(job.newBuilder())
        .setUpdateStatuses(ACTIVE_JOB_UPDATE_STATES));
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
   * @param user The user who is changing the state.
   * @throws UpdateStateException If no active update exists for the provided {@code job}, or
   *                              if the proposed state transition is not allowed.
   */
  private void unscopedChangeUpdateStatus(
      final IJobKey job,
      final Function<JobUpdateStatus, JobUpdateStatus> stateChange,
      final Optional<String> user)
      throws UpdateStateException {

    storage.write(new MutateWork.NoResult<UpdateStateException>() {
      @Override
      protected void execute(MutableStoreProvider storeProvider)
          throws UpdateStateException {

        IJobUpdateSummary update = Iterables.getOnlyElement(
            storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(queryActiveByJob(job)), null);
        if (update == null) {
          throw new UpdateStateException("There is no active update for " + job);
        }

        JobUpdateStatus status = update.getState().getStatus();
        JobUpdateStatus newStatus = requireNonNull(stateChange.apply(status));
        changeUpdateStatus(storeProvider, update, newStatus, user);
      }
    });
  }

  private void changeUpdateStatus(
      MutableStoreProvider storeProvider,
      IJobUpdateSummary updateSummary,
      JobUpdateStatus newStatus,
      Optional<String> user) {

    if (updateSummary.getState().getStatus() == newStatus) {
      return;
    }

    assertTransitionAllowed(updateSummary.getState().getStatus(), newStatus);
    recordAndChangeJobUpdateStatus(
        storeProvider,
        updateSummary.getUpdateId(),
        updateSummary.getJobKey(),
        newStatus,
        user);
  }

  private void recordAndChangeJobUpdateStatus(
      MutableStoreProvider storeProvider,
      String updateId,
      IJobKey job,
      JobUpdateStatus status,
      Optional<String> user) {

    changeJobUpdateStatus(storeProvider, updateId, job, status, user, true);
  }

  private static final Set<JobUpdateStatus> TERMINAL_STATES = ImmutableSet.of(
      ROLLED_FORWARD,
      ROLLED_BACK,
      ABORTED,
      JobUpdateStatus.FAILED,
      ERROR
  );

  private void changeJobUpdateStatus(
      MutableStoreProvider storeProvider,
      String updateId,
      IJobKey job,
      JobUpdateStatus newStatus,
      Optional<String> user,
      boolean recordChange) {

    JobUpdateStatus status;
    boolean record;

    JobUpdateStore.Mutable updateStore = storeProvider.getJobUpdateStore();
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
              .setUser(user.orNull())
              .setTimestampMs(clock.nowMillis())),
          updateId);
    }

    if (TERMINAL_STATES.contains(status)) {
      if (updateLock.isPresent()) {
        lockManager.releaseLock(ILock.build(new Lock()
            .setKey(LockKey.job(job.newBuilder()))
            .setToken(updateLock.get())));
      }

      pulseHandler.remove(updateId);
    } else {
      pulseHandler.updatePulseStatus(updateId, status);
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
        update = updateFactory.newUpdate(jobUpdate.getInstructions(), action == ROLL_FORWARD);
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "Uncaught exception: " + e, e);
        changeJobUpdateStatus(storeProvider, updateId, job, ERROR, user, true);
        return;
      }
      updates.put(job, update);
      evaluateUpdater(
          storeProvider,
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

  private static final Set<InstanceUpdateStatus> NOOP_INSTANCE_UPDATE =
      ImmutableSet.of(InstanceUpdateStatus.WORKING, InstanceUpdateStatus.SUCCEEDED);

  private static boolean isCoordinatedUpdate(IJobUpdateInstructions instructions) {
    return instructions.getSettings().getBlockIfNoPulsesAfterMs() > 0;
  }

  private boolean isCoordinatedAndPulseExpired(
      String updateId,
      IJobUpdateInstructions instructions) {

    if (isCoordinatedUpdate(instructions)) {
      PulseState pulseState = pulseHandler.get(updateId);
      boolean result = pulseState == null || pulseState.isBlocked(clock);
      LOG.info(String.format("Coordinated update %s pulse expired: %s", updateId, result));
      return result;
    } else {
      return false;
    }
  }

  private void evaluateUpdater(
      final MutableStoreProvider storeProvider,
      final UpdateFactory.Update update,
      IJobUpdateSummary summary,
      Map<Integer, Optional<IScheduledTask>> changedInstance) {

    JobUpdateStatus updaterStatus = summary.getState().getStatus();
    final IJobKey job = summary.getJobKey();

    final JobUpdateStore.Mutable updateStore = storeProvider.getJobUpdateStore();
    if (!updateStore.getLockToken(summary.getUpdateId()).isPresent()) {
      recordAndChangeJobUpdateStatus(storeProvider, summary.getUpdateId(), job, ERROR, NO_USER);
      return;
    }

    IJobUpdateInstructions instructions =
        updateStore.fetchJobUpdateInstructions(summary.getUpdateId()).get();

    if (isCoordinatedAndPulseExpired(summary.getUpdateId(), instructions)) {
      // Move coordinated update into awaiting pulse state.
      JobUpdateStatus blockedStatus = getBlockedState(summary.getState().getStatus());
      changeUpdateStatus(storeProvider, summary, blockedStatus, Optional.of(INTERNAL_USER));
      return;
    }

    InstanceStateProvider<Integer, Optional<IScheduledTask>> stateProvider =
        new InstanceStateProvider<Integer, Optional<IScheduledTask>>() {
          @Override
          public Optional<IScheduledTask> getState(Integer instanceId) {
            return getActiveInstance(storeProvider.getTaskStore(), job, instanceId);
          }
        };

    EvaluationResult<Integer> result = update.getUpdater().evaluate(changedInstance, stateProvider);

    LOG.info(JobKeys.canonicalString(job) + " evaluation result: " + result);

    for (Map.Entry<Integer, SideEffect> entry : result.getSideEffects().entrySet()) {
      Iterable<InstanceUpdateStatus> statusChanges;

      // Don't bother persisting a sequence of status changes that represents an instance that
      // was immediately recognized as being healthy and in the desired state.
      if (entry.getValue().getStatusChanges().equals(NOOP_INSTANCE_UPDATE)) {
        List<IJobInstanceUpdateEvent> savedEvents =
            updateStore.fetchInstanceEvents(summary.getUpdateId(), entry.getKey());
        if (savedEvents.isEmpty()) {
          LOG.info("Suppressing no-op update for instance " + entry.getKey());
          statusChanges = ImmutableSet.of();
        } else {
          // We choose to risk redundant events in corner cases here (after failing over while
          // updates are in-flight) to simplify the implementation.
          statusChanges = entry.getValue().getStatusChanges();
        }
      } else {
        statusChanges = entry.getValue().getStatusChanges();
      }

      for (InstanceUpdateStatus statusChange : statusChanges) {
        JobUpdateAction action = STATE_MAP.get(Pair.of(statusChange, updaterStatus));
        requireNonNull(action);

        IJobInstanceUpdateEvent event = IJobInstanceUpdateEvent.build(
            new JobInstanceUpdateEvent()
                .setInstanceId(entry.getKey())
                .setTimestampMs(clock.nowMillis())
                .setAction(action));
        updateStore.saveJobInstanceUpdateEvent(event, summary.getUpdateId());
      }
    }

    OneWayStatus status = result.getStatus();
    if (status == SUCCEEDED || status == OneWayStatus.FAILED) {
      if (SideEffect.hasActions(result.getSideEffects().values())) {
        throw new IllegalArgumentException(
            "A terminal state should not specify actions: " + result);
      }

      if (status == SUCCEEDED) {
        changeUpdateStatus(storeProvider, summary, update.getSuccessStatus(), NO_USER);
      } else {
        changeUpdateStatus(storeProvider, summary, update.getFailureStatus(), NO_USER);
      }
    } else {
      LOG.info("Executing side-effects for update of " + job + ": " + result.getSideEffects());
      for (Map.Entry<Integer, SideEffect> entry : result.getSideEffects().entrySet()) {
        IInstanceKey instance = InstanceKeys.from(job, entry.getKey());

        Optional<InstanceAction> action = entry.getValue().getAction();
        if (action.isPresent()) {
          Optional<InstanceActionHandler> handler = action.get().getHandler();
          if (handler.isPresent()) {
            Amount<Long, Time> reevaluateDelay = handler.get().getReevaluationDelay(
                instance,
                instructions,
                storeProvider,
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
  }

  /**
   * Associates an instance updater state change and the job's update status to an action.
   */
  private static final Map<Pair<InstanceUpdateStatus, JobUpdateStatus>, JobUpdateAction> STATE_MAP =
      ImmutableMap.<Pair<InstanceUpdateStatus, JobUpdateStatus>, JobUpdateAction>builder()
          .put(
              Pair.of(InstanceUpdateStatus.WORKING, ROLLING_FORWARD),
              JobUpdateAction.INSTANCE_UPDATING)
          .put(
              Pair.of(InstanceUpdateStatus.SUCCEEDED, ROLLING_FORWARD),
              JobUpdateAction.INSTANCE_UPDATED)
          .put(
              Pair.of(InstanceUpdateStatus.FAILED, ROLLING_FORWARD),
              JobUpdateAction.INSTANCE_UPDATE_FAILED)
          .put(
              Pair.of(InstanceUpdateStatus.WORKING, ROLLING_BACK),
              JobUpdateAction.INSTANCE_ROLLING_BACK)
          .put(
              Pair.of(InstanceUpdateStatus.SUCCEEDED, ROLLING_BACK),
              JobUpdateAction.INSTANCE_ROLLED_BACK)
          .put(
              Pair.of(InstanceUpdateStatus.FAILED, ROLLING_BACK),
              JobUpdateAction.INSTANCE_ROLLBACK_FAILED)
          .build();

  private Runnable getDeferredEvaluator(final IInstanceKey instance, final String updateId) {
    return new Runnable() {
      @Override
      public void run() {
        storage.write(new MutateWork.NoResult.Quiet() {
          @Override
          protected void execute(MutableStoreProvider storeProvider) {
            IJobUpdateSummary summary =
                getOnlyMatch(storeProvider.getJobUpdateStore(), queryByUpdateId(updateId));
            JobUpdateStatus status = summary.getState().getStatus();
            // Suppress this evaluation if the updater is not currently active.
            if (JobUpdateStateMachine.isActive(status)) {
              UpdateFactory.Update update = updates.get(instance.getJobKey());
              evaluateUpdater(
                  storeProvider,
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

  private static class PulseHandler {
    private final Clock clock;

    // TODO(maxim): expose this data via a debug endpoint AURORA-1103.
    // Currently active coordinated update pulse states. A pulse state is added when a coordinated
    // update is created and removed only when an update reaches terminal state. A PAUSED update
    // pulse state is still retained in the map and accepts pulses.
    private final Map<String, PulseState> pulseMap = Maps.newHashMap();

    PulseHandler(Clock clock) {
      this.clock = requireNonNull(clock);
    }

    synchronized void initializePulseState(IJobUpdate update, JobUpdateStatus status) {
      pulseMap.put(update.getSummary().getUpdateId(), new PulseState(
          update.getSummary().getJobKey(),
          status,
          update.getInstructions().getSettings().getBlockIfNoPulsesAfterMs(),
          0L));
    }

    synchronized PulseState pulseAndGet(String updateId) {
      PulseState state = pulseMap.get(updateId);
      if (state != null) {
        state = pulseMap.put(updateId, new PulseState(
            state.getJobKey(),
            state.getStatus(),
            state.getPulseTimeoutMs(),
            clock.nowMillis()));
      }
      return state;
    }

    synchronized void updatePulseStatus(String updateId, JobUpdateStatus status) {
      PulseState state = pulseMap.get(updateId);
      if (state != null) {
        pulseMap.put(updateId, new PulseState(
            state.getJobKey(),
            status,
            state.getPulseTimeoutMs(),
            state.getLastPulseMs()));
      }
    }

    synchronized void remove(String updateId) {
      pulseMap.remove(updateId);
    }

    synchronized PulseState get(String updateId) {
      return pulseMap.get(updateId);
    }
  }

  private static class PulseState {
    private final IJobKey jobKey;
    private final JobUpdateStatus status;
    private final long pulseTimeoutMs;
    private final long lastPulseMs;

    PulseState(IJobKey jobKey, JobUpdateStatus status, long pulseTimeoutMs, long lastPulseMs) {
      this.jobKey = requireNonNull(jobKey);
      this.status = requireNonNull(status);
      this.pulseTimeoutMs = pulseTimeoutMs;
      this.lastPulseMs = lastPulseMs;
    }

    IJobKey getJobKey() {
      return jobKey;
    }

    JobUpdateStatus getStatus() {
      return status;
    }

    long getPulseTimeoutMs() {
      return pulseTimeoutMs;
    }

    long getLastPulseMs() {
      return lastPulseMs;
    }

    boolean isBlocked(Clock clock) {
      return clock.nowMillis() - lastPulseMs >= pulseTimeoutMs;
    }
  }
}
