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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Atomics;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.Driver;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.async.RescheduleCalculator;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.state.SideEffectStorage.SideEffectWork;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.SlaveID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.apache.aurora.gen.ScheduleStatus.UNKNOWN;
import static org.apache.aurora.scheduler.state.SideEffectStorage.OperationFinalizer;


/**
 * Manager of all persistence-related operations for the scheduler.  Acts as a controller for
 * persisted state machine transitions, and their side-effects.
 *
 * TODO(William Farner): Re-evaluate thread safety here, specifically risk of races that
 * modify managerState.
 */
public class StateManagerImpl implements StateManager {
  private static final Logger LOG = Logger.getLogger(StateManagerImpl.class.getName());

  @VisibleForTesting
  SideEffectStorage getStorage() {
    return storage;
  }

  // Work queue to receive state machine side effect work.
  // Items are sorted to place DELETE entries last.  This is to ensure that within an operation,
  // a delete is always processed after a state transition.
  private final Queue<WorkEntry> workQueue = new PriorityQueue<>(10,
      new Comparator<WorkEntry>() {
        @Override public int compare(WorkEntry a, WorkEntry b) {
          if ((a.command == WorkCommand.DELETE) != (b.command == WorkCommand.DELETE)) {
            return (a.command == WorkCommand.DELETE) ? 1 : -1;
          } else {
            return 0;
          }
        }
      });

  // Adapt the work queue into a sink.
  private final TaskStateMachine.WorkSink workSink = new TaskStateMachine.WorkSink() {
      @Override public void addWork(
          WorkCommand work,
          TaskStateMachine stateMachine,
          Function<IScheduledTask, IScheduledTask> mutation) {

        workQueue.add(new WorkEntry(work, stateMachine, mutation));
      }
    };

  private final Function<Map.Entry<Integer, ITaskConfig>, IScheduledTask> taskCreator =
      new Function<Map.Entry<Integer, ITaskConfig>, IScheduledTask>() {
        @Override public IScheduledTask apply(Map.Entry<Integer, ITaskConfig> entry) {
          ITaskConfig task = entry.getValue();
          AssignedTask assigned = new AssignedTask()
              .setTaskId(taskIdGenerator.generate(task, entry.getKey()))
              .setInstanceId(entry.getKey())
              .setTask(task.newBuilder());
          return IScheduledTask.build(new ScheduledTask()
              .setStatus(INIT)
              .setAssignedTask(assigned));
        }
      };

  private final SideEffectStorage storage;
  private final Clock clock;
  private final Driver driver;
  private final TaskIdGenerator taskIdGenerator;
  private final RescheduleCalculator rescheduleCalculator;

  /**
   * An item of work on the work queue.
   */
  private static class WorkEntry {
    private final WorkCommand command;
    private final TaskStateMachine stateMachine;
    private final Function<IScheduledTask, IScheduledTask> mutation;

    WorkEntry(
        WorkCommand command,
        TaskStateMachine stateMachine,
        Function<IScheduledTask, IScheduledTask> mutation) {

      this.command = command;
      this.stateMachine = stateMachine;
      this.mutation = mutation;
    }
  }

  @Inject
  StateManagerImpl(
      final Storage storage,
      final Clock clock,
      Driver driver,
      TaskIdGenerator taskIdGenerator,
      EventSink eventSink,
      RescheduleCalculator rescheduleCalculator) {

    checkNotNull(storage);
    this.clock = checkNotNull(clock);

    OperationFinalizer finalizer = new OperationFinalizer() {
      @Override public void finalize(SideEffectWork<?, ?> work, MutableStoreProvider store) {
        processWorkQueueInWriteOperation(work, store);
      }
    };

    this.storage = new SideEffectStorage(storage, finalizer, eventSink);
    this.driver = checkNotNull(driver);
    this.taskIdGenerator = checkNotNull(taskIdGenerator);
    this.rescheduleCalculator = checkNotNull(rescheduleCalculator);

    Stats.exportSize("work_queue_depth", workQueue);
  }

  @Override
  public void insertPendingTasks(final Map<Integer, ITaskConfig> tasks) {
    checkNotNull(tasks);

    // Done outside the write transaction to minimize the work done inside a transaction.
    final Set<IScheduledTask> scheduledTasks =
        ImmutableSet.copyOf(transform(tasks.entrySet(), taskCreator));

    storage.write(storage.new NoResultQuietSideEffectWork() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(scheduledTasks);

        for (IScheduledTask task : scheduledTasks) {
          createStateMachine(task).updateState(PENDING);
        }
      }
    });
  }

  @Override
  public int changeState(
      Query.Builder query,
      final ScheduleStatus newState,
      final Optional<String> auditMessage) {

    return changeState(query, new Function<TaskStateMachine, Boolean>() {
      @Override public Boolean apply(TaskStateMachine stateMachine) {
        return stateMachine.updateState(newState, auditMessage);
      }
    });
  }

  @Override
  public IAssignedTask assignTask(
      String taskId,
      String slaveHost,
      SlaveID slaveId,
      Set<Integer> assignedPorts) {

    checkNotBlank(taskId);
    checkNotBlank(slaveHost);
    checkNotNull(assignedPorts);

    TaskAssignMutation mutation = assignHost(slaveHost, slaveId, assignedPorts);
    changeState(Query.taskScoped(taskId), mutation);

    return mutation.getAssignedTask();
  }

  private int changeStateInWriteOperation(
      Set<String> taskIds,
      Function<TaskStateMachine, Boolean> stateChange) {

    int count = 0;
    for (TaskStateMachine stateMachine : getStateMachines(taskIds).values()) {
      if (stateChange.apply(stateMachine)) {
        ++count;
      }
    }
    return count;
  }

  private int changeState(
      final Query.Builder query,
      final Function<TaskStateMachine, Boolean> stateChange) {

    return storage.write(storage.new QuietSideEffectWork<Integer>() {
      @Override public Integer apply(MutableStoreProvider storeProvider) {
        Set<String> ids = FluentIterable.from(storeProvider.getTaskStore().fetchTasks(query))
            .transform(Tasks.SCHEDULED_TO_ID)
            .toSet();
        return changeStateInWriteOperation(ids, stateChange);
      }
    });
  }

  private interface TaskAssignMutation extends Function<TaskStateMachine, Boolean> {
    IAssignedTask getAssignedTask();
  }

  private static Map<String, Integer> getNameMappedPorts(
      Set<String> portNames,
      Set<Integer> allocatedPorts) {

    Preconditions.checkNotNull(portNames);

    // Expand ports.
    Map<String, Integer> ports = Maps.newHashMap();
    Set<Integer> portsRemaining = Sets.newHashSet(allocatedPorts);
    Iterator<Integer> portConsumer = Iterables.consumingIterable(portsRemaining).iterator();

    for (String portName : portNames) {
      Preconditions.checkArgument(portConsumer.hasNext(),
          "Allocated ports %s were not sufficient to expand task.", allocatedPorts);
      int portNumber = portConsumer.next();
      ports.put(portName, portNumber);
    }

    if (!portsRemaining.isEmpty()) {
      LOG.warning("Not all allocated ports were used to map ports!");
    }

    return ports;
  }

  private TaskAssignMutation assignHost(
      final String slaveHost,
      final SlaveID slaveId,
      final Set<Integer> assignedPorts) {

    final TaskMutation mutation = new TaskMutation() {
      @Override public IScheduledTask apply(IScheduledTask task) {
        ScheduledTask builder = task.newBuilder();
        AssignedTask assigned = builder.getAssignedTask();
        assigned.setAssignedPorts(
            getNameMappedPorts(assigned.getTask().getRequestedPorts(), assignedPorts));
        assigned.setSlaveHost(slaveHost)
            .setSlaveId(slaveId.getValue());
        return IScheduledTask.build(builder);
      }
    };

    return new TaskAssignMutation() {
      private AtomicReference<IAssignedTask> assignedTask = Atomics.newReference();
      @Override public IAssignedTask getAssignedTask() {
        return assignedTask.get();
      }

      @Override public Boolean apply(final TaskStateMachine stateMachine) {
        TaskMutation wrapper = new TaskMutation() {
          @Override public IScheduledTask apply(IScheduledTask task) {
            IScheduledTask mutated = mutation.apply(task);
            Preconditions.checkState(
                assignedTask.compareAndSet(null, mutated.getAssignedTask()),
                "More than one result was found for an identity query.");
            return mutated;
          }
        };
        return stateMachine.updateState(ScheduleStatus.ASSIGNED, wrapper);
      }
    };
  }

  private void processWorkQueueInWriteOperation(
      SideEffectWork<?, ?> sideEffectWork,
      MutableStoreProvider storeProvider) {

    for (final WorkEntry work : Iterables.consumingIterable(workQueue)) {
      final TaskStateMachine stateMachine = work.stateMachine;

      if (work.command == WorkCommand.KILL) {
        driver.killTask(stateMachine.getTaskId());
      } else {
        TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();
        String taskId = stateMachine.getTaskId();
        Query.Builder idQuery = Query.taskScoped(taskId);

        switch (work.command) {
          case RESCHEDULE:
            IScheduledTask ancestor = Iterables.getOnlyElement(taskStore.fetchTasks(idQuery));

            ScheduledTask builder = ancestor.newBuilder();
            builder.getAssignedTask().unsetSlaveId();
            builder.getAssignedTask().unsetSlaveHost();
            builder.getAssignedTask().unsetAssignedPorts();
            builder.unsetTaskEvents();
            builder.setAncestorId(taskId);
            String newTaskId = taskIdGenerator.generate(
                ITaskConfig.build(builder.getAssignedTask().getTask()),
                builder.getAssignedTask().getInstanceId());
            builder.getAssignedTask().setTaskId(newTaskId);

            LOG.info("Task being rescheduled: " + taskId);

            IScheduledTask task = IScheduledTask.build(builder);
            taskStore.saveTasks(ImmutableSet.of(task));

            ScheduleStatus newState;
            String auditMessage;
            long flapPenaltyMs = rescheduleCalculator.getFlappingPenaltyMs(ancestor);
            if (flapPenaltyMs > 0) {
              newState = THROTTLED;
              auditMessage =
                  String.format("Rescheduled, penalized for %s ms for flapping", flapPenaltyMs);
            } else {
              newState = PENDING;
              auditMessage = "Rescheduled";
            }

            createStateMachine(task).updateState(newState, Optional.of(auditMessage));
            break;

          case UPDATE_STATE:
            taskStore.mutateTasks(idQuery, new TaskMutation() {
              @Override public IScheduledTask apply(IScheduledTask task) {
                return work.mutation.apply(
                    IScheduledTask.build(task.newBuilder().setStatus(stateMachine.getState())));
              }
            });
            sideEffectWork.addTaskEvent(
                PubsubEvent.TaskStateChange.transition(
                    Iterables.getOnlyElement(taskStore.fetchTasks(idQuery)),
                    stateMachine.getPreviousState()));
            break;

          case DELETE:
            deleteTasks(ImmutableSet.of(taskId));
            break;

          case INCREMENT_FAILURES:
            taskStore.mutateTasks(idQuery, new TaskMutation() {
              @Override public IScheduledTask apply(IScheduledTask task) {
                return IScheduledTask.build(
                    task.newBuilder().setFailureCount(task.getFailureCount() + 1));
              }
            });
            break;

          default:
            LOG.severe("Unrecognized work command type " + work.command);
        }
      }
    }
  }

  @Override
  public void deleteTasks(final Set<String> taskIds) {
    storage.write(storage.new NoResultQuietSideEffectWork() {
      @Override protected void execute(final MutableStoreProvider storeProvider) {
        TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();
        Iterable<IScheduledTask> tasks = taskStore.fetchTasks(Query.taskScoped(taskIds));
        addTaskEvent(new PubsubEvent.TasksDeleted(ImmutableSet.copyOf(tasks)));
        taskStore.deleteTasks(taskIds);
      }
    });
  }

  private Map<String, TaskStateMachine> getStateMachines(final Set<String> taskIds) {
    return storage.consistentRead(new Work.Quiet<Map<String, TaskStateMachine>>() {
      @Override public Map<String, TaskStateMachine> apply(StoreProvider storeProvider) {
        Map<String, IScheduledTask> existingTasks = Maps.uniqueIndex(
            storeProvider.getTaskStore().fetchTasks(Query.taskScoped(taskIds)),
            new Function<IScheduledTask, String>() {
              @Override public String apply(IScheduledTask input) {
                return input.getAssignedTask().getTaskId();
              }
            });

        ImmutableMap.Builder<String, TaskStateMachine> builder = ImmutableMap.builder();
        for (String taskId : taskIds) {
          // Pass null get() values through.
          builder.put(taskId, getStateMachine(taskId, existingTasks.get(taskId)));
        }
        return builder.build();
      }
    });
  }

  private TaskStateMachine getStateMachine(String taskId, @Nullable IScheduledTask task) {
    if (task != null) {
      return createStateMachine(task, task.getStatus());
    }

    // The task is unknown, not present in storage.
    TaskStateMachine stateMachine = new TaskStateMachine(
        taskId,
        null,
        workSink,
        clock,
        INIT);
    stateMachine.updateState(UNKNOWN);
    return stateMachine;
  }

  private TaskStateMachine createStateMachine(IScheduledTask task) {
    return createStateMachine(task, INIT);
  }

  private TaskStateMachine createStateMachine(IScheduledTask task, ScheduleStatus initialState) {
    return new TaskStateMachine(
        Tasks.id(task),
        task,
        workSink,
        clock,
        initialState);
  }
}
