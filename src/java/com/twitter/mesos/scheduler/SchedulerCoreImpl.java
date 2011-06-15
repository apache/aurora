package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveOffer;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.TaskStore;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED_BY_CLIENT;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.CONSTRUCTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.INITIALIZED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STARTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STOPPED;

/**
 * Implementation of the scheduler core.
 *
 * @author William Farner
 */
public class SchedulerCoreImpl implements SchedulerCore {

  private static final Logger LOG = Logger.getLogger(SchedulerCore.class.getName());

  @VisibleForTesting
  @CmdLine(name = "missing_task_grace_period",
      help = "The amount of time after which to treat an ASSIGNED task as LOST.")
  static final Arg<Amount<Long, Time>> MISSING_TASK_GRACE_PERIOD =
      Arg.create(Amount.of(1L, Time.MINUTES));

  private final Map<String, VolatileTaskState> taskStateById =
      new MapMaker().makeComputingMap(new Function<String, VolatileTaskState>() {
        @Override public VolatileTaskState apply(String taskId) {
          return new VolatileTaskState(taskId);
        }
      });

  // Work queue to receive state machine side effect work.
  private final Queue<WorkEntry> workQueue = Lists.newLinkedList();

  // An item of work on the work queue.
  private class WorkEntry {
    private final WorkCommand command;
    private final TaskStateMachine stateMachine;
    private final Closure<ScheduledTask> mutation;

    WorkEntry(WorkCommand command, TaskStateMachine stateMachine,
        Closure<ScheduledTask> mutation) {
      this.command = command;
      this.stateMachine = stateMachine;
      this.mutation = mutation;
    }
  }

  // Adapt the work queue into a sink.
  private final TaskStateMachine.WorkSink workSink = new TaskStateMachine.WorkSink() {
      @Override public void addWork(WorkCommand work, TaskStateMachine stateMachine,
          Closure<ScheduledTask> mutation) {
        workQueue.add(new WorkEntry(work, stateMachine, mutation));
      }
    };

  private final Map<String, TaskStateMachine> taskStateMachines = new MapMaker().makeComputingMap(
      new Function<String, TaskStateMachine>() {
        @Override public TaskStateMachine apply(String taskId) {
          return new TaskStateMachine(taskId, Suppliers.<ScheduledTask>ofInstance(null), workSink,
              MISSING_TASK_GRACE_PERIOD.get()).updateState(UNKNOWN);
        }
      }
  );

  // Schedulers that are responsible for triggering execution of jobs.
  private final ImmutableList<JobManager> jobManagers;

  // Handles all scheduler persistence
  private final Storage storage;

  // Kills the task with the id passed into execute.
  private Closure<String> killTask;

  // Filter to determine whether a task should be scheduled.
  private final SchedulingFilter schedulingFilter;

  private final PulseMonitor<String> executorPulseMonitor;
  private final Clock clock;

  enum State {
    CONSTRUCTED,
    INITIALIZED,
    STARTED,
    STOPPED
  }

  private final StateMachine<State> stateMachine;

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler,
      ImmediateJobManager immediateScheduler,
      @StorageRole(Role.Primary) Storage storage,
      SchedulingFilter schedulingFilter,
      PulseMonitor<String> executorPulseMonitor) {
    this(cronScheduler, immediateScheduler, storage, schedulingFilter, executorPulseMonitor,
        Clock.SYSTEM_CLOCK);
  }

  public SchedulerCoreImpl(CronJobManager cronScheduler,
      ImmediateJobManager immediateScheduler,
      Storage storage,
      SchedulingFilter schedulingFilter,
      PulseMonitor<String> executorPulseMonitor,
      Clock clock) {

    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    this.jobManagers = ImmutableList.of(cronScheduler, immediateScheduler);

    this.storage = checkNotNull(storage);
    this.schedulingFilter = checkNotNull(schedulingFilter);
    this.executorPulseMonitor = checkNotNull(executorPulseMonitor);
    this.clock = checkNotNull(clock);

   // TODO(John Sirois): Add a method to StateMachine or write a wrapper that allows for a
   // read-locked do-in-state assertion around a block of work.  Transition would then need to grab
   // the write lock.  Another approach is to force these transitions with:
   // SchedulerCoreFactory -> SchedulerCoreRunner -> SchedulerCore which remove all state sensitive
   // methods out of schedulerCore save for stop.
   stateMachine = StateMachine.<State>builder("scheduler-core")
       .initialState(CONSTRUCTED)
       .addState(CONSTRUCTED, INITIALIZED)
       .addState(INITIALIZED, STARTED)
       .addState(STARTED, STOPPED)
       .build();
  }

  @Override
  public String initialize() {
    checkLifecycleState(CONSTRUCTED);
    storage.start(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

        taskStore.mutate(Query.GET_ALL, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask task) {
            ConfigurationManager.applyDefaultsIfUnset(task.getAssignedTask().getTask());

            String taskId = Tasks.id(task);
            taskStateMachines.put(taskId,
                new TaskStateMachine(taskId, taskSupplier(taskId),
                    workSink, MISSING_TASK_GRACE_PERIOD.get(), task.getStatus()));
          }
        });
      }
    });

    LOG.info("Initialization of DbStorage complete.");
    String frameworkId = getFrameworkId();
    stateMachine.transition(INITIALIZED);
    return frameworkId;
  }

  private String getFrameworkId() {
    return storage.doInTransaction(new Work<String, RuntimeException>() {
      @Override public String apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws RuntimeException {
        return schedulerStore.fetchFrameworkId();
      }
    });
  }

  @Override
  public void start(Closure<String> killTask) {
    checkLifecycleState(INITIALIZED);
    this.killTask = Preconditions.checkNotNull(killTask);
    stateMachine.transition(STARTED);

    for (JobManager jobManager : jobManagers) {
      jobManager.start();
    }
  }

  @Override
  public void registered(final String frameworkId) {
    checkStarted();
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        schedulerStore.saveFrameworkId(frameworkId);
      }
    });
  }

  @Override
  public synchronized Set<TaskState> getTasks(final Query query) {
    checkStarted();
    return storage.doInTransaction(new Work.Quiet<Set<TaskState>>() {
      @Override public Set<TaskState> apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        ImmutableSortedSet<ScheduledTask> tasks = taskStore.fetch(query);
        return ImmutableSet.copyOf(Iterables.transform(tasks,
            new Function<ScheduledTask, TaskState>() {
              @Override public TaskState apply(ScheduledTask task) {
                VolatileTaskState volatileTaskState = taskStateById.get(Tasks.id(task));
                return new TaskState(task, volatileTaskState);
              }
            }));
      }
    });
  }

  private boolean hasActiveJob(JobConfiguration job) {
    return Iterables.any(jobManagers, managerHasJob(job));
  }

  private boolean hasRunningTasks(JobConfiguration job) {
    return !getTasks(Query.activeQuery(job)).isEmpty();
  }

  /**
   * Creates a new task ID that is permanently unique (not guaranteed, but highly confident),
   * and by default sorts in chronological order.
   *
   * @param task Task that an ID is being generated for.
   * @return New task ID.
   */
  private String generateTaskId(TwitterTaskInfo task) {
    return new StringBuilder()
        .append(clock.nowMillis())               // Allows chronological sorting.
        .append("-")
        .append(jobKey(task))                    // Identification and collision prevention.
        .append("-")
        .append(task.getShardId())               // Collision prevention within job.
        .append("-")
        .append(UUID.randomUUID())               // Just-in-case collision prevention.
        .toString().replaceAll("[^\\w-]", "-");  // Constrain character set.
  }

  // TODO(William Farner): This is does not currently clear out tasks when a host is decommissioned.
  //    Figure out a solution that will work.  Might require mesos support for fetching the list
  //    of slaves.
  @Override
  public synchronized void updateRegisteredTasks(final RegisteredTaskUpdate update) {
    checkStarted();
    checkNotNull(update);
    checkNotBlank(update.getSlaveHost());
    checkNotNull(update.getTaskInfos());

    executorPulseMonitor.pulse(update.getSlaveHost());

    List<LiveTaskInfo> taskInfos = update.isSetTaskInfos() ? update.getTaskInfos()
        : ImmutableList.<LiveTaskInfo>of();

    // Wrap with a mutable map so we can modify later.
    final Map<String, LiveTaskInfo> taskInfoMap = Maps.newHashMap(
        Maps.uniqueIndex(taskInfos, Tasks.LIVE_TO_ID));

    // TODO(William Farner): Have the scheduler only retain configurations for live jobs,
    //    and acquire all other state from slaves.
    //    This will allow the scheduler to only persist active tasks.

    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          final TaskStore taskStore) {

        processWorkQueueAfter(new Command() {
          @Override public void execute() {
            // Look for any tasks that we don't know about, or this slave should not be modifying.
            final Set<String> tasksForHost = taskStore.fetchIds(
                new Query(new TaskQuery().setSlaveHost(update.getSlaveHost())));

            final Set<String> tasksForOtherHosts = taskStore.fetchIds(
                new Query(new TaskQuery().setTaskIds(taskInfoMap.keySet()),
                    new Predicate<ScheduledTask>() {
                      @Override public boolean apply(ScheduledTask task) {
                        return !update.getSlaveHost().equals(task.getAssignedTask().getSlaveHost());
                      }
                    })
            );
            if (!tasksForOtherHosts.isEmpty()) {
              LOG.log(Level.SEVERE, "Slave " + update.getSlaveHost()
                  + " sent an update for task(s) not assigned to it: " + update);
            }

            Set<String> tasksToActOn =
                Sets.difference(Sets.union(tasksForHost, taskInfoMap.keySet()), tasksForOtherHosts);
            for (String taskId : tasksToActOn) {
              TaskStateMachine stateMachine = taskStateMachines.get(taskId);

              LiveTaskInfo taskUpdate = taskInfoMap.get(taskId);
              ScheduleStatus updatedState = taskUpdate == null ? UNKNOWN : taskUpdate.getStatus();
              // Prevent log spam from no-op updates.
              if (updatedState != stateMachine.getState()) {
                stateMachine.updateState(updatedState);
              }

              // Update the resource information for the tasks that we currently have on record.
              if (taskUpdate != null && taskUpdate.getResources() != null) {
                VolatileTaskState volatileTaskState = taskStateById.get(taskId);
                volatileTaskState.resources =
                    new ResourceConsumption(taskUpdate.getResources());
              }
            }
          }
        });
      }
    });
  }

  private void processWorkQueueAfter(Command command) {
    command.execute();

    for (final WorkEntry work : Iterables.consumingIterable(workQueue)) {
      final TaskStateMachine stateMachine = work.stateMachine;

      if (work.command == WorkCommand.KILL) {
        killTask.execute(stateMachine.getTaskId());
      } else {
        storage.doInTransaction(new Work.NoResult.Quiet() {
          @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
              final TaskStore taskStore) {

            switch (work.command) {
              case RESCHEDULE:
                ScheduledTask task = Iterables.getOnlyElement(
                    taskStore.fetch(Query.byId(stateMachine.getTaskId()))).deepCopy();
                task.getAssignedTask().unsetSlaveId();
                task.getAssignedTask().unsetSlaveHost();
                task.unsetTaskEvents();
                task.setAncestorId(Tasks.id(task));
                String taskId = generateTaskId(task.getAssignedTask().getTask());
                task.getAssignedTask().setTaskId(taskId);

                LOG.info("Task being rescheduled: " + stateMachine.getTaskId());

                taskStore.add(ImmutableSet.of(task));

                taskStateMachines.put(taskId,
                    new TaskStateMachine(taskId, taskSupplier(taskId),
                        workSink, MISSING_TASK_GRACE_PERIOD.get())
                        .updateState(PENDING, "Rescheduled"));
                break;

              case UPDATE_STATE:
                taskStore.mutate(Query.byId(stateMachine.getTaskId()),
                    new Closure<ScheduledTask>() {
                      @Override public void execute(ScheduledTask task) {
                        task.setStatus(stateMachine.getState());
                        work.mutation.execute(task);
                      }
                    });
                break;

              case DELETE:
                taskStore.remove(ImmutableSet.of(stateMachine.getTaskId()));
                break;

              case INCREMENT_FAILURES:
                taskStore.mutate(Query.byId(stateMachine.getTaskId()),
                    new Closure<ScheduledTask>() {
                      @Override public void execute(ScheduledTask task) {
                        task.setFailureCount(task.getFailureCount() + 1);
                      }
                    });
                break;

              default:
                LOG.severe("Unrecognized work command type " + work.command);
            }
          }
        });
      }
    }
  }

  @Override
  public synchronized void createJob(JobConfiguration job) throws ScheduleException,
      ConfigurationManager.TaskDescriptionException {
    checkStarted();

    final JobConfiguration populated = ConfigurationManager.validateAndPopulate(job);

    if (hasActiveJob(populated)) {
      throw new ScheduleException("Job already exists: " + jobKey(populated));
    }

    boolean accepted = false;
    for (final JobManager manager : jobManagers) {
      if (manager.receiveJob(populated)) {
        LOG.info("Job accepted by manager: " + manager.getUniqueKey());
        accepted = true;
        break;
      }
    }

    if (!accepted) {
      LOG.severe("Job was not accepted by any of the configured schedulers, discarding.");
      LOG.severe("Discarded job: " + populated);
      throw new ScheduleException("Job not accepted, discarding.");
    }
  }

  @Override
  public synchronized void runJob(JobConfiguration job) {
    checkStarted();
    checkState(!hasRunningTasks(job));

    launchTasks(checkNotNull(job.getTaskConfigs()));
  }

  private final Function<TwitterTaskInfo, ScheduledTask> taskCreator =
      new Function<TwitterTaskInfo, ScheduledTask>() {
        @Override public ScheduledTask apply(TwitterTaskInfo task) {
          return new ScheduledTask()
              .setStatus(INIT)
              .setAssignedTask(
                  new AssignedTask()
                      .setTaskId(generateTaskId(task))
                      .setTask(task));
        }
      };

  private Supplier<ScheduledTask> taskSupplier(final String taskId) {
    return new Supplier<ScheduledTask>() {
      @Override public ScheduledTask get() {
        return storage.doInTransaction(new Work.Quiet<ScheduledTask>() {
          @Override public ScheduledTask apply(SchedulerStore schedulerStore,
              JobStore jobStore, TaskStore taskStore) {
            return Iterables.getOnlyElement(taskStore.fetch(Query.byId(taskId)));
          }
        });
      }
    };
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
    final Set<ScheduledTask> scheduledTasks = ImmutableSet.copyOf(transform(tasks, taskCreator));

    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        taskStore.add(scheduledTasks);
      }
    });

    final Set<String> taskIds = ImmutableSet.copyOf(
        Iterables.transform(scheduledTasks, Tasks.SCHEDULED_TO_ID));

    processWorkQueueAfter(new Command() {
      @Override public void execute() {
        for (String taskId : taskIds) {
          taskStateMachines.put(taskId,
              new TaskStateMachine(taskId, taskSupplier(taskId),
                  workSink, MISSING_TASK_GRACE_PERIOD.get()).updateState(PENDING));
        }
      }
    });

    return taskIds;
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
        return manager.hasJob(jobKey(job));
      }
    };
  }

  private static TwitterTaskInfo makeBootstrapTask() {
    return new TwitterTaskInfo()
        .setOwner(new Identity("mesos", "mesos"))
        .setJobName("executor_bootstrap")
        .setNumCpus(1)
        .setRamMb(512)
        .setShardId(0)
        .setStartCommand("echo \"Bootstrapping\"");
  }

  @Override
  public synchronized TwitterTask offer(final SlaveOffer slaveOffer) throws ScheduleException {
    checkStarted();
    checkNotNull(slaveOffer);

    vars.resourceOffers.incrementAndGet();

    final TwitterTaskInfo offer;
    try {
      offer = ConfigurationManager.makeConcrete(slaveOffer);
    } catch (ConfigurationManager.TaskDescriptionException e) {
      LOG.log(Level.SEVERE, "Invalid slave offer", e);
      return null;
    }

    final AtomicReference<AssignedTask> assignedTask = new AtomicReference<AssignedTask>();

    processWorkQueueAfter(new Command() {
      @Override public void execute() {
        storage.doInTransaction(new Work.NoResult.Quiet() {
          @Override public void execute(SchedulerStore schedulerStore, JobStore jobStore,
              TaskStore taskStore) {

            String taskId;
            if (!executorPulseMonitor.isAlive(slaveOffer.getHostname())) {
              LOG.info("Pulse monitor considers executor dead, launching bootstrap task on: "
                  + slaveOffer.getHostname());
              executorPulseMonitor.pulse(slaveOffer.getHostname());
              taskId = Iterables.getOnlyElement(launchTasks(ImmutableSet.of(makeBootstrapTask())));
              vars.executorBootstraps.incrementAndGet();
            } else {
              SortedSet<ScheduledTask> candidates = taskStore.fetch(
                  Query.and(Query.byStatus(PENDING),
                      schedulingFilter.makeFilter(offer, slaveOffer.getHostname())));

              if (candidates.isEmpty()) {
                return;
              }

              LOG.info("Found " + candidates.size() + " candidates for offer.");

              // Choose the first (top) candidate.
              taskId = Tasks.id(Iterables.get(candidates, 0));
            }

            TaskStateMachine stateMachine = taskStateMachines.get(taskId);
            stateMachine.updateState(ASSIGNED,
                new Closure<ScheduledTask>() {
                  @Override public void execute(ScheduledTask task) {
                    AssignedTask assigned = task.getAssignedTask();
                    assigned.setSlaveHost(slaveOffer.getHostname());
                    assigned.setSlaveId(slaveOffer.getSlaveId().getValue());
                    assignedTask.set(assigned);
                  }
                });
          }
        });
      }
    });

    AssignedTask task = assignedTask.get();

    // There were no PENDING candidates
    if (task == null) {
      return null;
    }

    LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s.",
        slaveOffer.getHostname(), task.getSlaveId(), jobKey(task)));

    List<Resource> resources = ImmutableList.of(
        Resources.makeResource(Resources.CPUS, task.getTask().getNumCpus()),
        Resources.makeResource(Resources.RAM_MB, task.getTask().getRamMb())
    );

    return makeTwitterTask(task, slaveOffer.getSlaveId().getValue(), resources);
  }

  @VisibleForTesting
  static TwitterTask makeTwitterTask(AssignedTask task, String slaveId, List<Resource> resources) {
    return new TwitterTask(task.getTaskId(), slaveId,
        task.getTask().getJobName() + "-" + task.getTaskId(), resources, task);
  }

  @Override
  public synchronized void setTaskStatus(Query rawQuery, final ScheduleStatus status,
      @Nullable final String message) {
    checkStarted();
    checkNotNull(rawQuery);
    checkNotNull(status);

    // Only allow state transition from non-terminal state.
    final Query query = Query.and(rawQuery, Predicates.not(Tasks.TERMINATED_FILTER));

    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          final TaskStore taskStore) {

        processWorkQueueAfter(new Command() {
          @Override public void execute() {
            for (String taskId : taskStore.fetchIds(query)) {
              TaskStateMachine stateMachine = taskStateMachines.get(taskId);
              // Don't bother trying to change the state if matching tasks are already in the
              // new state.
              if (stateMachine.getState() != status) {
                stateMachine.updateState(status, message);
              }
            }
          }
        });
      }
    });
  }

  @Override
  public synchronized void killTasks(final Query query) throws ScheduleException {
    checkStarted();
    checkNotNull(query);

    LOG.info("Killing tasks matching " + query);

    storage.doInTransaction(new Work.NoResult<ScheduleException>() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          final TaskStore taskStore) throws ScheduleException {

        // If this looks like a query for all tasks in a job, instruct the scheduler modules to
        // delete the job.
        boolean matchingScheduler = false;

        if (query.specifiesJobOnly()) {
          String jobKey = query.getJobKey();

          for (JobManager manager : jobManagers) {
            if (manager.deleteJob(jobKey)) {
              matchingScheduler = true;
            }
          }
        }

        // Don't change the state of terminated tasks.
        final Set<String> taskIds = taskStore.fetchIds(Query.and(query, Tasks.ACTIVE_FILTER));
        if (!matchingScheduler && taskIds.isEmpty()) {
          throw new ScheduleException("No tasks matching query found.");
        }

        processWorkQueueAfter(new Command() {
          @Override public void execute() {
            for (String taskId : taskIds) {
              taskStateMachines.get(taskId).updateState(KILLED_BY_CLIENT,
                  "Manually killed by client");
            }
          }
        });
      }
    });
  }

  @Override
  public synchronized void restartTasks(final Set<String> taskIds) throws RestartException {
    checkStarted();
    checkNotBlank(taskIds);

    LOG.info("Restart requested for tasks " + taskIds);

    // TODO(William Farner): Change this (and the thrift interface) to query by shard ID in the
    //    context of a job instead of task ID.

    storage.doInTransaction(new Work.NoResult<RestartException>() {
      @Override public void execute(SchedulerStore schedulerStore, JobStore jobStore,
          final TaskStore taskStore) throws RestartException {

        Query byId = Query.byId(taskIds);
        final Set<ScheduledTask> tasks = taskStore.fetch(byId);
        if (tasks.size() != taskIds.size()) {
          Set<String> unknownTasks = Sets.difference(taskIds, ImmutableSet
              .copyOf(transform(tasks, Tasks.SCHEDULED_TO_ID)));

          throw new RestartException("Restart requested for unknown tasks " + unknownTasks);
        } else if (Iterables.any(tasks, Predicates.not(Tasks.ACTIVE_FILTER))) {
          throw new RestartException("Restart requested for inactive tasks "
              + Iterables.filter(tasks, Tasks.ACTIVE_FILTER));
        }

        Set<String> jobKeys = ImmutableSet.copyOf(transform(tasks, Tasks.SCHEDULED_TO_JOB_KEY));
        if (jobKeys.size() != 1) {
          throw new RestartException("Task restart request cannot span multiple jobs: " + jobKeys);
        }

        processWorkQueueAfter(new Command() {
          @Override public void execute() {
            for (String taskId : Iterables.transform(tasks, Tasks.SCHEDULED_TO_ID)) {
              taskStateMachines.get(taskId).updateState(RESTARTING, "Restarting by client request");
            }
          }
        });
      }
    });
  }

  @Override
  public void stop() {
    checkStarted();
    storage.stop();
    stateMachine.transition(STOPPED);
  }

  private void checkStarted() {
    checkLifecycleState(STARTED);
  }

  private void checkLifecycleState(State state) {
    Preconditions.checkState(stateMachine.getState() == state);
  }

  private final class Vars {
    final AtomicLong resourceOffers = Stats.exportLong("scheduler_resource_offers");
    final AtomicLong executorBootstraps = Stats.exportLong("executor_bootstraps");

    Vars() {
      for (final ScheduleStatus status : ScheduleStatus.values()) {
        Stats.export(new StatImpl<Integer>("task_store_" + status) {
          @Override public Integer read() {
            return storage.doInTransaction(new Work.Quiet<Integer>() {
              @Override public Integer apply(SchedulerStore schedulerStore, JobStore jobStore,
                  TaskStore taskStore) {

                // TODO(John Sirois): optimize this for DbStorage, currently burdened by un-needed
                // deserialization of ScheduledTask data - just need a select count
                return taskStore.fetch(Query.byStatus(status)).size();
              }
            });
          }
        });
      }
    }
  }
  private final Vars vars = new Vars();
}
