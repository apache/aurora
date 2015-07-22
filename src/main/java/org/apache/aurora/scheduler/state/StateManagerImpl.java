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
package org.apache.aurora.scheduler.state;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator;
import org.apache.aurora.scheduler.state.SideEffect.Action;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.SlaveID;

import static java.util.Objects.requireNonNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.apache.aurora.scheduler.state.StateChangeResult.INVALID_CAS_STATE;
import static org.apache.aurora.scheduler.state.StateChangeResult.SUCCESS;

/**
 * Manager of all persistence-related operations for the scheduler.  Acts as a controller for
 * persisted state machine transitions, and their side-effects.
 */
public class StateManagerImpl implements StateManager {
  private static final Logger LOG = Logger.getLogger(StateManagerImpl.class.getName());

  private final Clock clock;
  private final Driver driver;
  private final TaskIdGenerator taskIdGenerator;
  private final EventSink eventSink;
  private final RescheduleCalculator rescheduleCalculator;

  @Inject
  StateManagerImpl(
      final Clock clock,
      Driver driver,
      TaskIdGenerator taskIdGenerator,
      EventSink eventSink,
      RescheduleCalculator rescheduleCalculator) {

    this.clock = requireNonNull(clock);
    this.driver = requireNonNull(driver);
    this.taskIdGenerator = requireNonNull(taskIdGenerator);
    this.eventSink = requireNonNull(eventSink);
    this.rescheduleCalculator = requireNonNull(rescheduleCalculator);
  }

  private IScheduledTask createTask(int instanceId, ITaskConfig template) {
    AssignedTask assigned = new AssignedTask()
        .setTaskId(taskIdGenerator.generate(template, instanceId))
        .setInstanceId(instanceId)
        .setTask(template.newBuilder());
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(INIT)
        .setAssignedTask(assigned));
  }

  @Override
  public void insertPendingTasks(
      MutableStoreProvider storeProvider,
      final ITaskConfig task,
      Set<Integer> instanceIds) {

    requireNonNull(storeProvider);
    requireNonNull(task);
    checkNotBlank(instanceIds);

    // Done outside the write transaction to minimize the work done inside a transaction.
    Set<IScheduledTask> scheduledTasks = FluentIterable.from(instanceIds)
        .transform(new Function<Integer, IScheduledTask>() {
          @Override
          public IScheduledTask apply(Integer instanceId) {
            return createTask(instanceId, task);
          }
        }).toSet();

    Iterable<IScheduledTask> existingTasks = storeProvider.getTaskStore().fetchTasks(
        Query.jobScoped(task.getJob()).active());

    Set<Integer> existingInstanceIds =
        FluentIterable.from(existingTasks).transform(Tasks.SCHEDULED_TO_INSTANCE_ID).toSet();

    if (!Sets.intersection(existingInstanceIds, instanceIds).isEmpty()) {
      throw new IllegalArgumentException("Instance ID collision detected.");
    }

    storeProvider.getUnsafeTaskStore().saveTasks(scheduledTasks);

    for (IScheduledTask scheduledTask : scheduledTasks) {
      updateTaskAndExternalState(
          storeProvider.getUnsafeTaskStore(),
          Tasks.id(scheduledTask),
          Optional.of(scheduledTask),
          Optional.of(PENDING),
          Optional.absent());
    }
  }

  @Override
  public StateChangeResult changeState(
      MutableStoreProvider storeProvider,
      String taskId,
      Optional<ScheduleStatus> casState,
      final ScheduleStatus newState,
      final Optional<String> auditMessage) {

    return updateTaskAndExternalState(
        storeProvider.getUnsafeTaskStore(),
        casState,
        taskId,
        newState,
        auditMessage);
  }

  @Override
  public IAssignedTask assignTask(
      MutableStoreProvider storeProvider,
      String taskId,
      final String slaveHost,
      final SlaveID slaveId,
      final Map<String, Integer> assignedPorts) {

    checkNotBlank(taskId);
    checkNotBlank(slaveHost);
    requireNonNull(slaveId);
    requireNonNull(assignedPorts);

    Query.Builder query = Query.taskScoped(taskId);

    storeProvider.getUnsafeTaskStore().mutateTasks(query,
        new Function<IScheduledTask, IScheduledTask>() {
          @Override
          public IScheduledTask apply(IScheduledTask task) {
            ScheduledTask builder = task.newBuilder();
            builder.getAssignedTask()
                .setAssignedPorts(assignedPorts)
                .setSlaveHost(slaveHost)
                .setSlaveId(slaveId.getValue());
            return IScheduledTask.build(builder);
          }
        });

    StateChangeResult changeResult = updateTaskAndExternalState(
        storeProvider.getUnsafeTaskStore(),
        Optional.absent(),
        taskId,
        ASSIGNED,
        Optional.absent());

    Preconditions.checkState(
        changeResult == SUCCESS,
        "Attempt to assign task " + taskId + " to " + slaveHost + " failed");

    return Iterables.getOnlyElement(
        Iterables.transform(
            storeProvider.getTaskStore().fetchTasks(query),
            Tasks.SCHEDULED_TO_ASSIGNED));
  }

  @VisibleForTesting
  static final Supplier<String> LOCAL_HOST_SUPPLIER = Suppliers.memoize(
      new Supplier<String>() {
        @Override
        public String get() {
          try {
            return InetAddress.getLocalHost().getHostName();
          } catch (UnknownHostException e) {
            LOG.log(Level.SEVERE, "Failed to get self hostname.");
            throw Throwables.propagate(e);
          }
        }
      });

  private StateChangeResult updateTaskAndExternalState(
      TaskStore.Mutable taskStore,
      Optional<ScheduleStatus> casState,
      String taskId,
      ScheduleStatus targetState,
      Optional<String> transitionMessage) {

    Optional<IScheduledTask> task = Optional.fromNullable(Iterables.getOnlyElement(
        taskStore.fetchTasks(Query.taskScoped(taskId)),
        null));

    // CAS operation fails if the task does not exist, or the states don't match.
    if (casState.isPresent()
        && (!task.isPresent() || casState.get() != task.get().getStatus())) {

      return INVALID_CAS_STATE;
    }

    return updateTaskAndExternalState(
        taskStore,
        taskId,
        task,
        Optional.of(targetState),
        transitionMessage);
  }

  private static final Function<SideEffect, Action> GET_ACTION =
      new Function<SideEffect, Action>() {
        @Override
        public Action apply(SideEffect sideEffect) {
          return sideEffect.getAction();
        }
      };

  private static final List<Action> ACTIONS_IN_ORDER = ImmutableList.of(
      Action.INCREMENT_FAILURES,
      Action.SAVE_STATE,
      Action.RESCHEDULE,
      Action.KILL,
      Action.DELETE);

  static {
    // Sanity check to ensure no actions are missing.
    Preconditions.checkState(
        ImmutableSet.copyOf(ACTIONS_IN_ORDER).equals(ImmutableSet.copyOf(Action.values())),
        "Not all actions are included in ordering.");
  }

  // Actions are deliberately ordered to prevent things like deleting a task before rescheduling it
  // (thus losing the object to copy), or rescheduling a task before incrementing the failure count
  // (thus not carrying forward the failure increment).
  private static final Ordering<SideEffect> ACTION_ORDER =
      Ordering.explicit(ACTIONS_IN_ORDER).onResultOf(GET_ACTION);

  private StateChangeResult updateTaskAndExternalState(
      TaskStore.Mutable taskStore,
      String taskId,
      // Note: This argument is deliberately non-final, and should not be made final.
      // This is because using the captured value within the storage operation below is
      // highly-risky, since it doesn't necessarily represent the value in storage.
      // As a result, it would be easy to accidentally clobber mutations.
      Optional<IScheduledTask> task,
      final Optional<ScheduleStatus> targetState,
      final Optional<String> transitionMessage) {

    if (task.isPresent()) {
      Preconditions.checkArgument(taskId.equals(task.get().getAssignedTask().getTaskId()));
    }

    final List<PubsubEvent> events = Lists.newArrayList();

    final TaskStateMachine stateMachine = task.isPresent()
        ? new TaskStateMachine(task.get())
        : new TaskStateMachine(taskId);

    TransitionResult result = stateMachine.updateState(targetState);
    Query.Builder query = Query.taskScoped(taskId);

    for (SideEffect sideEffect : ACTION_ORDER.sortedCopy(result.getSideEffects())) {
      Optional<IScheduledTask> upToDateTask = Optional.fromNullable(
          Iterables.getOnlyElement(taskStore.fetchTasks(query), null));

      switch (sideEffect.getAction()) {
        case INCREMENT_FAILURES:
          taskStore.mutateTasks(query, new TaskMutation() {
            @Override
            public IScheduledTask apply(IScheduledTask task) {
              return IScheduledTask.build(
                  task.newBuilder().setFailureCount(task.getFailureCount() + 1));
            }
          });
          break;

        case SAVE_STATE:
          Preconditions.checkState(
              upToDateTask.isPresent(),
              "Operation expected task " + taskId + " to be present.");

          taskStore.mutateTasks(query, new TaskMutation() {
            @Override
            public IScheduledTask apply(IScheduledTask task) {
              ScheduledTask mutableTask = task.newBuilder();
              mutableTask.setStatus(targetState.get());
              mutableTask.addToTaskEvents(new TaskEvent()
                  .setTimestamp(clock.nowMillis())
                  .setStatus(targetState.get())
                  .setMessage(transitionMessage.orNull())
                  .setScheduler(LOCAL_HOST_SUPPLIER.get()));
              return IScheduledTask.build(mutableTask);
            }
          });
          events.add(
              PubsubEvent.TaskStateChange.transition(
                  Iterables.getOnlyElement(taskStore.fetchTasks(query)),
                  stateMachine.getPreviousState()));
          break;

        case RESCHEDULE:
          Preconditions.checkState(
              upToDateTask.isPresent(),
              "Operation expected task " + taskId + " to be present.");
          LOG.info("Task being rescheduled: " + taskId);

          ScheduleStatus newState;
          String auditMessage;
          long flapPenaltyMs = rescheduleCalculator.getFlappingPenaltyMs(upToDateTask.get());
          if (flapPenaltyMs > 0) {
            newState = THROTTLED;
            auditMessage =
                String.format("Rescheduled, penalized for %s ms for flapping", flapPenaltyMs);
          } else {
            newState = PENDING;
            auditMessage = "Rescheduled";
          }

          IScheduledTask newTask = IScheduledTask.build(createTask(
              upToDateTask.get().getAssignedTask().getInstanceId(),
              upToDateTask.get().getAssignedTask().getTask())
              .newBuilder()
              .setFailureCount(upToDateTask.get().getFailureCount())
              .setAncestorId(taskId));
          taskStore.saveTasks(ImmutableSet.of(newTask));
          updateTaskAndExternalState(
              taskStore,
              Tasks.id(newTask),
              Optional.of(newTask),
              Optional.of(newState),
              Optional.of(auditMessage));
          break;

        case KILL:
          driver.killTask(taskId);
          break;

        case DELETE:
          Preconditions.checkState(
              upToDateTask.isPresent(),
              "Operation expected task " + taskId + " to be present.");

          events.add(deleteTasks(taskStore, ImmutableSet.of(taskId)));
          break;

        default:
          throw new IllegalStateException("Unrecognized side-effect " + sideEffect.getAction());
      }
    }

    // Note (AURORA-138): Delaying events until after the write operation is somewhat futile, since
    // the state may actually not be written to durable store
    // (e.g. if this is a nested transaction). Ideally, Storage would add a facility to attach
    // side-effects that are performed after the outer-most transaction completes (meaning state
    // has been durably persisted).
    for (PubsubEvent event : events) {
      eventSink.post(event);
    }

    return result.getResult();
  }

  @Override
  public void deleteTasks(MutableStoreProvider storeProvider, final Set<String> taskIds) {
    Map<String, IScheduledTask> tasks = Maps.uniqueIndex(
        storeProvider.getTaskStore().fetchTasks(Query.taskScoped(taskIds)),
        Tasks.SCHEDULED_TO_ID);

    for (Map.Entry<String, IScheduledTask> entry : tasks.entrySet()) {
      updateTaskAndExternalState(
          storeProvider.getUnsafeTaskStore(),
          entry.getKey(),
          Optional.of(entry.getValue()),
          Optional.absent(),
          Optional.absent());
    }
  }

  private static PubsubEvent deleteTasks(TaskStore.Mutable taskStore, Set<String> taskIds) {
    Iterable<IScheduledTask> tasks = taskStore.fetchTasks(Query.taskScoped(taskIds));
    taskStore.deleteTasks(taskIds);
    return new PubsubEvent.TasksDeleted(ImmutableSet.copyOf(tasks));
  }
}
