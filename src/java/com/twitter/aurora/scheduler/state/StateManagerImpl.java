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

import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Atomics;
import com.google.inject.Inject;

import org.apache.mesos.Protos.SlaveID;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.ShardUpdateResult;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskUpdateConfiguration;
import com.twitter.aurora.gen.UpdateResult;
import com.twitter.aurora.scheduler.Driver;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.events.PubsubEvent;
import com.twitter.aurora.scheduler.state.SideEffectStorage.SideEffectWork;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.StorageException;
import com.twitter.aurora.scheduler.storage.Storage.StoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.Work;
import com.twitter.aurora.scheduler.storage.TaskStore;
import com.twitter.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import com.twitter.aurora.scheduler.storage.UpdateStore;
import com.twitter.aurora.scheduler.storage.entities.IAssignedTask;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.common.base.Closure;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

import static com.twitter.aurora.gen.ScheduleStatus.INIT;
import static com.twitter.aurora.gen.ScheduleStatus.KILLING;
import static com.twitter.aurora.gen.ScheduleStatus.PENDING;
import static com.twitter.aurora.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.aurora.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.aurora.gen.ScheduleStatus.UPDATING;
import static com.twitter.aurora.scheduler.base.Shards.GET_NEW_CONFIG;
import static com.twitter.aurora.scheduler.base.Shards.GET_ORIGINAL_CONFIG;
import static com.twitter.aurora.scheduler.base.Tasks.SCHEDULED_TO_SHARD_ID;
import static com.twitter.aurora.scheduler.state.SideEffectStorage.OperationFinalizer;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * Manager of all persistence-related operations for the scheduler.  Acts as a controller for
 * persisted state machine transitions, and their side-effects.
 *
 * TODO(William Farner): Re-evaluate thread safety here, specifically risk of races that
 * modify managerState.
 */
public class StateManagerImpl implements StateManager {
  private static final Logger LOG = Logger.getLogger(StateManagerImpl.class.getName());

  private final SideEffectStorage storage;
  @VisibleForTesting
  SideEffectStorage getStorage() {
    return storage;
  }

  private final Function<ITaskConfig, String> taskIdGenerator;

  // TODO(William Farner): Eliminate this and update all callers to use Storage directly.
  interface ReadOnlyStorage {
    <T, E extends Exception> T consistentRead(Work<T, E> work) throws StorageException, E;
  }
  private final ReadOnlyStorage readOnlyStorage;

  // Work queue to receive state machine side effect work.
  // Items are sorted to place DELETE entries last.  This is to ensure that within an operation,
  // a delete is always processed after a state transition.
  private final Queue<WorkEntry> workQueue = new PriorityQueue<WorkEntry>(10,
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

  private final Function<ITaskConfig, IScheduledTask> taskCreator =
      new Function<ITaskConfig, IScheduledTask>() {
        @Override public IScheduledTask apply(ITaskConfig task) {
          AssignedTask assigned =
              new AssignedTask().setTaskId(taskIdGenerator.apply(task)).setTask(task.newBuilder());
          return IScheduledTask.build(new ScheduledTask()
              .setStatus(INIT)
              .setAssignedTask(assigned));
        }
      };

  private final Driver driver;
  private final Clock clock;

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
      Function<ITaskConfig, String> taskIdGenerator,
      Closure<PubsubEvent> taskEventSink) {

    checkNotNull(storage);
    this.clock = checkNotNull(clock);

    OperationFinalizer finalizer = new OperationFinalizer() {
      @Override public void finalize(SideEffectWork<?, ?> work, MutableStoreProvider store) {
        processWorkQueueInWriteOperation(work, store);
      }
    };

    this.storage = new SideEffectStorage(storage, finalizer, taskEventSink);
    readOnlyStorage = new ReadOnlyStorage() {
      @Override public <T, E extends Exception> T consistentRead(Work<T, E> work)
          throws StorageException, E {
        return storage.consistentRead(work);
      }
    };

    this.driver = checkNotNull(driver);
    this.taskIdGenerator = checkNotNull(taskIdGenerator);

    Stats.exportSize("work_queue_depth", workQueue);
  }

  @Override
  public void insertPendingTasks(final Set<ITaskConfig> tasks) {
    checkNotNull(tasks);

    // Done outside the write transaction to minimize the work done inside a transaction.
    final Set<IScheduledTask> scheduledTasks = ImmutableSet.copyOf(transform(tasks, taskCreator));

    storage.write(storage.new NoResultSideEffectWork() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(scheduledTasks);

        for (IScheduledTask task : scheduledTasks) {
          createStateMachine(task).updateState(PENDING);
        }
      }
    });
  }

  /**
   * Thrown when an update fails.
   */
  static class UpdateException extends Exception {
    public UpdateException(String msg) {
      super(msg);
    }
    public UpdateException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * Registers a new update.
   *
   * @param jobKey Job to register an update for.
   * @param updatedTasks Updated Task information to be registered.
   * @throws UpdateException If no active tasks are found for the job, or if an update for the job
   *     is already in progress.
   * @return A unique string identifying the update.
   */
  String registerUpdate(final IJobKey jobKey, final Set<ITaskConfig> updatedTasks)
      throws UpdateException {

    checkNotNull(jobKey);
    checkNotBlank(updatedTasks);

    return storage.write(storage.new SideEffectWork<String, UpdateException>() {
      @Override public String apply(MutableStoreProvider storeProvider) throws UpdateException {
        assertNotUpdatingOrRollingBack(jobKey, storeProvider.getTaskStore());

        Set<ITaskConfig> existingTasks = ImmutableSet.copyOf(Iterables.transform(
            storeProvider.getTaskStore().fetchTasks(Query.jobScoped(jobKey).active()),
            Tasks.SCHEDULED_TO_INFO));

        if (existingTasks.isEmpty()) {
          throw new UpdateException("No active tasks found for job " + JobKeys.toPath(jobKey));
        }

        UpdateStore.Mutable updateStore = storeProvider.getUpdateStore();
        if (updateStore.fetchJobUpdateConfig(jobKey).isPresent()) {
          throw new UpdateException("Update already in progress for " + jobKey);
        }

        Map<Integer, ITaskConfig> oldShards = Maps.uniqueIndex(existingTasks,
            Tasks.INFO_TO_SHARD_ID);
        Map<Integer, ITaskConfig> newShards = Maps.uniqueIndex(updatedTasks,
            Tasks.INFO_TO_SHARD_ID);

        ImmutableSet.Builder<TaskUpdateConfiguration> shardConfigBuilder = ImmutableSet.builder();
        for (int shard : Sets.union(oldShards.keySet(), newShards.keySet())) {
          shardConfigBuilder.add(
              new TaskUpdateConfiguration(
                  Optional.fromNullable(oldShards.get(shard))
                      .transform(ITaskConfig.TO_BUILDER)
                      .orNull(),
                  Optional.fromNullable(newShards.get(shard))
                      .transform(ITaskConfig.TO_BUILDER)
                      .orNull()));
        }

        String updateToken = UUID.randomUUID().toString();
        updateStore.saveJobUpdateConfig(
            new JobUpdateConfiguration()
                .setJobKey(jobKey.newBuilder())
                .setUpdateToken(updateToken)
                .setConfigs(shardConfigBuilder.build()));
        return updateToken;
      }
    });
  }

  private static final Set<ScheduleStatus> UPDATE_IN_PROGRESS = EnumSet.of(UPDATING, ROLLBACK);
  private static void assertNotUpdatingOrRollingBack(IJobKey jobKey, TaskStore taskStore)
      throws UpdateException {

    Query.Builder query = Query.jobScoped(jobKey).byStatus(UPDATE_IN_PROGRESS);
    if (!taskStore.fetchTasks(query).isEmpty()) {
      throw new UpdateException("Unable to proceed until UPDATING and ROLLBACK tasks complete.");
    }
  }

  /**
   * Completes an in-progress update.
   *
   * @param jobKey The job key.
   * @param invokingUser User invoking the command for auditing purposes.
   * @param updateToken Token associated with the update.  If present, the token must match the
   *     the stored token for the update.
   * @param result The result of the update.
   * @param throwIfMissing If {@code true}, this throws UpdateException when the update is missing.
   * @return {@code true} if the update was finished, false if nonexistent.
   * @throws UpdateException If an update is not in-progress for the job, or the non-null token
   *     does not match the stored token.
   */
  boolean finishUpdate(
      final IJobKey jobKey,
      final String invokingUser,
      final Optional<String> updateToken,
      final UpdateResult result,
      final boolean throwIfMissing) throws UpdateException {

    checkNotNull(jobKey);
    checkNotBlank(invokingUser);

    return storage.write(storage.new SideEffectWork<Boolean, UpdateException>() {
      @Override public Boolean apply(MutableStoreProvider storeProvider) throws UpdateException {
        assertNotUpdatingOrRollingBack(jobKey, storeProvider.getTaskStore());

        UpdateStore.Mutable updateStore = storeProvider.getUpdateStore();

        // Since we store all shards in a job with the same token, we can just check shard 0,
        // which is always guaranteed to exist for a job.
        Optional<JobUpdateConfiguration> jobConfig = updateStore.fetchJobUpdateConfig(jobKey);
        if (!jobConfig.isPresent()) {
          if (throwIfMissing) {
            throw new UpdateException("Update does not exist for " + JobKeys.toPath(jobKey));
          }
          return false;
        }

        if (updateToken.isPresent()
            && !updateToken.get().equals(jobConfig.get().getUpdateToken())) {
          throw new UpdateException("Invalid update token for " + JobKeys.toPath(jobKey));
        }

        if (EnumSet.of(UpdateResult.SUCCESS, UpdateResult.FAILED).contains(result)) {
          // Kill any shards that were removed during the update or rollback.
          Function<TaskUpdateConfiguration, ITaskConfig> removedSelector =
              (result == UpdateResult.SUCCESS) ? GET_NEW_CONFIG : GET_ORIGINAL_CONFIG;
          for (Integer shard : fetchRemovedShards(jobConfig.get(), removedSelector)) {
            changeState(
                Query.shardScoped(jobKey, shard).active(),
                KILLING,
                Optional.of("Removed during update by " + invokingUser));
          }
        }

        updateStore.removeShardUpdateConfigs(jobKey);
        return true;
      }
    });
  }

  private static final Function<TaskUpdateConfiguration, Integer> GET_SHARD_ID =
      new Function<TaskUpdateConfiguration, Integer>() {
        @Override public Integer apply(TaskUpdateConfiguration config) {
          TaskConfig task = (config.getOldConfig() != null)
              ? config.getOldConfig()
              : config.getNewConfig();
          return task.getShardId();
        }
      };

  private Set<Integer> fetchRemovedShards(
      JobUpdateConfiguration jobConfig,
      Function<TaskUpdateConfiguration, ITaskConfig> configSelector) {

    return FluentIterable.from(jobConfig.getConfigs())
        .filter(Predicates.compose(Predicates.isNull(), configSelector))
        .transform(GET_SHARD_ID)
        .toSet();
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

  @VisibleForTesting
  static void putResults(
      ImmutableMap.Builder<Integer, ShardUpdateResult> builder,
      ShardUpdateResult result,
      Iterable<Integer> shardIds) {

    for (int shardId : shardIds) {
      builder.put(shardId, result);
    }
  }

  private static final Predicate<IScheduledTask> NOT_UPDATING = new Predicate<IScheduledTask>() {
    @Override public boolean apply(IScheduledTask task) {
      return task.getStatus() != UPDATING && task.getStatus() != ROLLBACK;
    }
  };

  private Set<Integer> getChangedShards(Set<IScheduledTask> tasks, Set<ITaskConfig> compareTo) {
    Set<ITaskConfig> existingTasks = FluentIterable.from(tasks)
        .transform(Tasks.SCHEDULED_TO_INFO)
        .toSet();
    Set<ITaskConfig> changedTasks = Sets.difference(compareTo, existingTasks);
    return FluentIterable.from(changedTasks)
        .transform(Tasks.INFO_TO_SHARD_ID)
        .toSet();
  }

  Map<Integer, ShardUpdateResult> modifyShards(
      final IJobKey jobKey,
      final String invokingUser,
      final Set<Integer> shards,
      final String updateToken,
      boolean updating) throws UpdateException {

    final Function<TaskUpdateConfiguration, ITaskConfig> configSelector = updating
        ? GET_NEW_CONFIG
        : GET_ORIGINAL_CONFIG;
    final ScheduleStatus modifyingState;
    final String auditMessage;
    if (updating) {
      modifyingState = UPDATING;
      auditMessage = "Updated by " + invokingUser;
    } else {
      modifyingState = ROLLBACK;
      auditMessage = "Rolled back by " + invokingUser;
    }

    return storage.write(
        storage.new SideEffectWork<Map<Integer, ShardUpdateResult>, UpdateException>() {
          @Override public Map<Integer, ShardUpdateResult> apply(MutableStoreProvider store)
              throws UpdateException {

            ImmutableMap.Builder<Integer, ShardUpdateResult> result = ImmutableMap.builder();

            Optional<JobUpdateConfiguration> updateConfig =
                store.getUpdateStore().fetchJobUpdateConfig(jobKey);
            if (!updateConfig.isPresent()) {
              throw new UpdateException("No active update found for " + JobKeys.toPath(jobKey));
            }

            if (!updateConfig.get().getUpdateToken().equals(updateToken)) {
              throw new UpdateException("Invalid update token for " + jobKey);
            }

            Query.Builder query = Query.shardScoped(jobKey, shards).active();
            Set<IScheduledTask> tasks = store.getTaskStore().fetchTasks(query);

            // Extract any shard IDs that are being added as a part of this stage in the update.
            Set<Integer> newShardIds = Sets.difference(shards,
                ImmutableSet.copyOf(Iterables.transform(tasks, SCHEDULED_TO_SHARD_ID)));

            if (!newShardIds.isEmpty()) {
              Set<ITaskConfig> newTasks = fetchTaskUpdateConfigs(
                  updateConfig.get(),
                  newShardIds,
                  configSelector);
              Set<Integer> unrecognizedShards = Sets.difference(newShardIds,
                  ImmutableSet.copyOf(Iterables.transform(newTasks, Tasks.INFO_TO_SHARD_ID)));
              if (!unrecognizedShards.isEmpty()) {
                throw new UpdateException(
                    "Cannot update unrecognized shards " + unrecognizedShards);
              }

              // Create new tasks, so they will be moved into the PENDING state.
              insertPendingTasks(newTasks);
              putResults(result, ShardUpdateResult.ADDED, newShardIds);
            }

            Set<Integer> updateShardIds = Sets.difference(shards, newShardIds);
            if (!updateShardIds.isEmpty()) {
              Set<ITaskConfig> targetConfigs = fetchTaskUpdateConfigs(
                  updateConfig.get(),
                  updateShardIds,
                  configSelector);
              // Filter tasks in UPDATING/ROLLBACK to obtain the changed shards. This is done so
              // that these tasks are either rolled back or updated. If not, when a task in UPDATING
              // is later KILLED and a new shard is created with the updated configuration it
              // appears as if the rollback completed successfully.
              Set<IScheduledTask> notUpdating = FluentIterable.from(tasks)
                  .filter(NOT_UPDATING)
                  .toSet();
              Set<Integer> changedShards = getChangedShards(notUpdating, targetConfigs);
              if (!changedShards.isEmpty()) {
                // Initiate update on the existing shards.
                // TODO(William Farner): The additional query could be avoided here.
                //                       Consider allowing state changes on tasks by task ID.
                changeState(
                    Query.shardScoped(jobKey, changedShards).active(),
                    modifyingState,
                    Optional.of(auditMessage));
                putResults(result, ShardUpdateResult.RESTARTING, changedShards);
              }
              putResults(
                  result,
                  ShardUpdateResult.UNCHANGED,
                  Sets.difference(updateShardIds, changedShards));
            }

            return result.build();
          }
        });
  }

  private Optional<TaskUpdateConfiguration> fetchShardUpdateConfig(
      UpdateStore updateStore, IJobKey jobKey, int shard) {

    Optional<JobUpdateConfiguration> optional = updateStore.fetchJobUpdateConfig(jobKey);
    if (optional.isPresent()) {
      Set<TaskUpdateConfiguration> matches =
          fetchShardUpdateConfigs(optional.get(), ImmutableSet.of(shard));
      return Optional.fromNullable(Iterables.getOnlyElement(matches, null));
    } else {
      return Optional.absent();
    }
  }

  private static final Function<TaskUpdateConfiguration, Integer> GET_SHARD =
      new Function<TaskUpdateConfiguration, Integer>() {
        @Override public Integer apply(TaskUpdateConfiguration config) {
          return config.isSetOldConfig()
              ? config.getOldConfig().getShardId()
              : config.getNewConfig().getShardId();
        }
      };

  private Set<TaskUpdateConfiguration> fetchShardUpdateConfigs(
      JobUpdateConfiguration config,
      Set<Integer> shards) {

    return ImmutableSet.copyOf(Iterables.filter(config.getConfigs(),
        Predicates.compose(Predicates.in(shards), GET_SHARD)));
  }

  /**
   * Fetches the task configurations for shards in the context of an in-progress job update.
   *
   * @param shards Shards within a job to fetch.
   * @return The task information of the shard.
   */
  private Set<ITaskConfig> fetchTaskUpdateConfigs(
      JobUpdateConfiguration config,
      final Set<Integer> shards,
      final Function<TaskUpdateConfiguration, ITaskConfig> configSelector) {

    checkNotNull(config);
    checkNotBlank(shards);
    return FluentIterable.from(fetchShardUpdateConfigs(config, shards))
        .transform(configSelector)
        .filter(Predicates.notNull())
        .toSet();
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

  // Supplier that checks if there is an active update for a job.
  private Supplier<Boolean> taskUpdateChecker(final IJobKey jobKey) {
    return new Supplier<Boolean>() {
      @Override public Boolean get() {
        return readOnlyStorage.consistentRead(new Work.Quiet<Boolean>() {
          @Override public Boolean apply(StoreProvider storeProvider) {
            return storeProvider.getUpdateStore().fetchJobUpdateConfig(jobKey).isPresent();
          }
        });
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
            ScheduledTask builder =
                Iterables.getOnlyElement(taskStore.fetchTasks(idQuery)).newBuilder();
            builder.getAssignedTask().unsetSlaveId();
            builder.getAssignedTask().unsetSlaveHost();
            builder.getAssignedTask().unsetAssignedPorts();
            builder.unsetTaskEvents();
            builder.setAncestorId(taskId);
            String newTaskId =
                taskIdGenerator.apply(ITaskConfig.build(builder.getAssignedTask().getTask()));
            builder.getAssignedTask().setTaskId(newTaskId);

            LOG.info("Task being rescheduled: " + taskId);

            IScheduledTask task = IScheduledTask.build(builder);
            taskStore.saveTasks(ImmutableSet.of(task));

            createStateMachine(task).updateState(PENDING, Optional.of("Rescheduled"));
            ITaskConfig taskInfo = task.getAssignedTask().getTask();
            sideEffectWork.addTaskEvent(
                new PubsubEvent.TaskRescheduled(
                    taskInfo.getOwner().getRole(),
                    taskInfo.getJobName(),
                    taskInfo.getShardId()));
            break;

          case UPDATE:
          case ROLLBACK:
            maybeRescheduleForUpdate(storeProvider, taskId, work.command == WorkCommand.ROLLBACK);
            break;

          case UPDATE_STATE:
            taskStore.mutateTasks(idQuery, new TaskMutation() {
              @Override public IScheduledTask apply(IScheduledTask task) {
                return work.mutation.apply(
                    IScheduledTask.build(task.newBuilder().setStatus(stateMachine.getState())));
              }
            });
            sideEffectWork.addTaskEvent(
                new PubsubEvent.TaskStateChange(
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
    storage.write(storage.new NoResultSideEffectWork() {
      @Override protected void execute(final MutableStoreProvider storeProvider) {
        TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();
        Iterable<IScheduledTask> tasks = taskStore.fetchTasks(Query.taskScoped(taskIds));
        addTaskEvent(new PubsubEvent.TasksDeleted(ImmutableSet.copyOf(tasks)));
        taskStore.deleteTasks(taskIds);
      }
    });
  }

  private void maybeRescheduleForUpdate(
      MutableStoreProvider storeProvider,
      String taskId,
      boolean rollingBack) {

    TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();

    ITaskConfig oldConfig = Tasks.SCHEDULED_TO_INFO.apply(
        Iterables.getOnlyElement(taskStore.fetchTasks(Query.taskScoped(taskId))));

    Optional<TaskUpdateConfiguration> optional = fetchShardUpdateConfig(
        storeProvider.getUpdateStore(),
        Tasks.INFO_TO_JOB_KEY.apply(oldConfig),
        oldConfig.getShardId());

    // TODO(Sathya): Figure out a way to handle race condition when finish update is called
    //     before ROLLBACK

    if (!optional.isPresent()) {
      LOG.warning("No update configuration found for key "
          + JobKeys.toPath(Tasks.INFO_TO_JOB_KEY.apply(oldConfig))
          + " shard " + oldConfig.getShardId() + " : Assuming update has finished.");
      return;
    }

    TaskUpdateConfiguration updateConfig = optional.get();
    TaskConfig newConfig =
        rollingBack ? updateConfig.getOldConfig() : updateConfig.getNewConfig();
    if (newConfig == null) {
      // The updated configuration removed the shard, nothing to reschedule.
      return;
    }

    IScheduledTask newTask =
        IScheduledTask.build(taskCreator.apply(ITaskConfig.build(newConfig)).newBuilder()
            .setAncestorId(taskId));
    taskStore.saveTasks(ImmutableSet.of(newTask));
    createStateMachine(newTask)
        .updateState(
            PENDING,
            Optional.of("Rescheduled after " + (rollingBack ? "rollback." : "update.")));
  }

  private Map<String, TaskStateMachine> getStateMachines(final Set<String> taskIds) {
    return readOnlyStorage.consistentRead(new Work.Quiet<Map<String, TaskStateMachine>>() {
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
        // Since the task doesn't exist, its job cannot be updating.
        Suppliers.ofInstance(false),
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
        taskUpdateChecker(Tasks.SCHEDULED_TO_JOB_KEY.apply(task)),
        workSink,
        clock,
        initialState);
  }
}
