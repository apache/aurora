package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanExecute;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.Exists;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.executor.ProcessScanner.ProcessInfo;
import com.twitter.mesos.executor.Task.TaskRunException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.comm.DeletedTasks;
import com.twitter.mesos.gen.comm.SchedulerMessage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;

/**
 * ExecutorCore
 *
 * TODO(William Farner): When a LiveTask is terminated, should we replace its entry with a DeadTask?
 *
 * @author William Farner
 */
public class ExecutorCore implements TaskManager, Supplier<Map<String, ScheduleStatus>> {
  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());

  @NotNull
  @Exists
  @CanRead
  @CanExecute
  @CmdLine(name = "process_scraper_script_path",
           help = "the path of the script for killing orphan task")
  private static final Arg<File> PROCESS_SCRAPER_SCRIPT = Arg.create(null);

  /**
   * {@literal @Named} binding key for the task executor service.
   */
  public static final String TASK_EXECUTOR =
      "com.twitter.mesos.executor.ExecutorCore.TASK_EXECUTOR";

  /**
   * {@literal @Named} binding key for the schedule interval for the process scanner.
   */
  public static final String PROCESS_SCANNER_TASK_SCHEDULE_INTERVAL =
      "com.twitter.mesos.executor.ExecutorCore.PROCESS_SCANNER_TASK_SCHEDULE_INTERVAL";

  /**
   * {@literal @Named} binding key for the task port range.
   */
  public static final String TASK_PORT_RANGE =
      "com.twitter.mesos.executor.ExecutorCore.TASK_PORT_RANGE";

  private static final long SHUTDOWN_TIMEOUT_MS =
      Amount.of(20L, Time.SECONDS).as(Time.MILLISECONDS);

  private final Map<String, Task> tasks = Maps.newConcurrentMap();

  private final File executorRootDir;

  private final ScheduledExecutorService scheduledExecutorService =
      new ScheduledThreadPoolExecutor(1,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ExecutorSync-%d").build());

  private final AtomicReference<String> slaveId = new AtomicReference<String>();

  private final Function<AssignedTask, Task> taskFactory;
  private final ExecutorService taskExecutor;
  private final Driver driver;
  private final ExceptionalClosure<KillCommand, KillException> processKiller;
  private final ProcessScanner processScanner;
  private final Amount<Integer, Time> processScannerTaskScheduleInterval;
  private final Range<Integer> taskPortRange;
  private final FileDeleter fileDeleter;

  private static final AtomicLong tasksReceived = Stats.exportLong("executor_tasks_received");
  private static final AtomicLong taskFailures = Stats.exportLong("executor_task_launch_failures");
  private static final AtomicLong unrecognizedProcessesEncountered =
      Stats.exportLong("executor_unrecognized_processes_encountered");
  private static final AtomicLong orphansEncountered =
      Stats.exportLong("executor_orphan_processes_encountered");
  private static final AtomicLong tasksKilled = Stats.exportLong("executor_tasks_killed");
  private static final AtomicLong numUnallocatedPortsProcessesEncountered =
      Stats.exportLong("executor_unallocated_port_processes_encountered");
  private static final AtomicLong badProcessesKilled =
      Stats.exportLong("executor_bad_processes_killed");
  private static final AtomicLong numBadDeleteAttempts =
      Stats.exportLong("executor_bad_delete_attempts");
  private static final AtomicLong unknownRetainedTasks =
      Stats.exportLong("executor_unknown_retained_tasks");

  @Inject
  public ExecutorCore(@ExecutorRootDir File executorRootDir,
      Function<AssignedTask, Task> taskFactory,
      @Named(TASK_EXECUTOR) ExecutorService taskExecutor,
      Driver driver,
      ExceptionalClosure<KillCommand, KillException> processKiller,
      @Named(PROCESS_SCANNER_TASK_SCHEDULE_INTERVAL) Amount<Integer, Time> interval,
      @Named(TASK_PORT_RANGE) Range<Integer> taskPortRange,
      FileDeleter fileDeleter) {
    this.executorRootDir = checkNotNull(executorRootDir);
    this.taskFactory = checkNotNull(taskFactory);
    this.taskExecutor = checkNotNull(taskExecutor);
    this.driver = checkNotNull(driver);
    this.processKiller = checkNotNull(processKiller);
    this.processScanner = new ProcessScanner(PROCESS_SCRAPER_SCRIPT.get());
    this.processScannerTaskScheduleInterval = checkNotNull(interval);
    this.taskPortRange = checkNotNull(taskPortRange);
    this.fileDeleter = checkNotNull(fileDeleter);
    Stats.exportSize("executor_tasks_stored", tasks);
  }

  /**
   * Adds dead tasks that the executor may report record of.
   *
   * @param deadTasks Dead tasks to store.
   */
  void addDeadTasks(Iterable<Task> deadTasks) {
    checkNotNull(deadTasks);

    for (Task task : deadTasks) {
      tasks.put(task.getId(), task);
    }
  }

  /**
   * Initiates periodic tasks that the executor performs (state sync, resource monitoring, etc).
   *
   * @param shutdownRegistry to register orderly shutdown of the periodic task scheduler.
   */
  void startPeriodicTasks(ShutdownRegistry shutdownRegistry) {
    new ResourceManager(this, executorRootDir, shutdownRegistry, fileDeleter).start();
    startProcessScanner();
    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        LOG.info("Shutting down sync executor.");
        scheduledExecutorService.shutdownNow();
      }
    });
  }

  void setSlaveId(String slaveId) {
    this.slaveId.set(checkNotNull(slaveId));
    LOG.info("Assigned slave ID: " + slaveId);
  }

  /**
   * Executes a task on the system.
   *
   * @param assignedTask The assigned task to run.
   */
  public void executeTask(AssignedTask assignedTask) {
    checkNotNull(assignedTask);

    final String taskId = assignedTask.getTaskId();

    LOG.info(String.format("Received task for execution: %s - %s",
        Tasks.jobKey(assignedTask), taskId));
    tasksReceived.incrementAndGet();

    final Task task = taskFactory.apply(assignedTask);
    tasks.put(taskId, task);

    driver.sendStatusUpdate(taskId, STARTING, Optional.<String>absent());
    try {
      task.stage();
      driver.sendStatusUpdate(taskId, RUNNING, Optional.<String>absent());
      task.run();
    } catch (TaskRunException e) {
      LOG.log(Level.SEVERE, "Failed to stage or run task " + taskId, e);
      taskFailures.incrementAndGet();
      task.terminate(FAILED);
      driver.sendStatusUpdate(taskId, FAILED, Optional.of(e.getMessage()));
      return;
    }

    taskExecutor.execute(new Runnable() {
      @Override public void run() {
        LOG.info("Waiting for task " + taskId + " to complete.");
        ScheduleStatus state = task.blockUntilTerminated();
        LOG.info("Task " + taskId + " completed in state " + state);
        driver.sendStatusUpdate(taskId, state, Optional.<String>absent());
      }
    });
  }

  public Task stopLiveTask(String taskId) {
    Task task = tasks.get(taskId);

    if (task != null && Tasks.isActive(task.getScheduleStatus())) {
      LOG.info("Killing task: " + task);
      tasksKilled.incrementAndGet();
      task.terminate(ScheduleStatus.KILLED);
    } else if (task == null) {
      LOG.severe("No such task found: " + taskId);
      driver.sendStatusUpdate(taskId, ScheduleStatus.LOST, Optional.of("Task not found on slave."));
    } else {
      LOG.info("Kill request for task in state " + task.getScheduleStatus() + " ignored.");
      driver.sendStatusUpdate(taskId, task.getScheduleStatus(),
          Optional.of("Repeating lost update."));
    }
    return task;
  }

  public Task getTask(String taskId) {
    return tasks.get(taskId);
  }

  public Iterable<Task> getTasks() {
    return Iterables.unmodifiableIterable(tasks.values());
  }

  private static final Function<Task, ScheduleStatus> GET_STATUS =
      new Function<Task, ScheduleStatus>() {
        @Override public ScheduleStatus apply(Task task) {
          return task.getScheduleStatus();
        }
      };

  private static final Predicate<Task> IS_ACTIVE =
      Predicates.compose(Predicates.in(Tasks.ACTIVE_STATES), GET_STATUS);

  @Override
  public Iterable<Task> getLiveTasks() {
    return Iterables.unmodifiableIterable(Iterables.filter(tasks.values(), IS_ACTIVE));
  }

  @Override
  public boolean hasTask(String taskId) {
    return tasks.containsKey(taskId);
  }

  @Override
  public boolean isRunning(String taskId) {
    return hasTask(taskId) && tasks.get(taskId).isRunning();
  }

  private Predicate<String> taskIdActiveFilter() {
    return Predicates.compose(IS_ACTIVE, Functions.forMap(tasks));
  }

  @Override
  public void deleteCompletedTasks(Set<String> taskIds) {
    Set<String> deletedTasks;
    synchronized (tasks) {
      Set<String> runningTasks =
          ImmutableSet.copyOf(Iterables.filter(taskIds, taskIdActiveFilter()));
      if (!runningTasks.isEmpty()) {
        LOG.severe("Attempted to delete running tasks " + runningTasks);
        numBadDeleteAttempts.addAndGet(runningTasks.size());
      }

      deletedTasks = ImmutableSet.copyOf(Sets.difference(taskIds, runningTasks));
      tasks.keySet().removeAll(deletedTasks);
    }

    if (!deletedTasks.isEmpty()) {
      driver.apply(new Message(SchedulerMessage.deletedTasks(new DeletedTasks(deletedTasks))));
    }
  }

  @Override
  public void adjustRetainedTasks(Map<String, ScheduleStatus> retainedTasks) {
    LOG.info("Adjusting task retention to match " + retainedTasks);

    synchronized (tasks) {
      Set<String> unknownTasks = Sets.difference(retainedTasks.keySet(), tasks.keySet());
      if (!unknownTasks.isEmpty()) {
        LOG.severe("Asked to retain unknown tasks " + unknownTasks);
        // TODO(wfarner): We probably want to react to this, though some care must be given in case
        // there is a race between receiving this message and a launchTasks message.  Exposing a
        // counter for now.
        unknownRetainedTasks.addAndGet(unknownTasks.size());
      }

      // Tasks that are in the task map but not in the retained map have been deleted remotely.
      Map<String, Task> deleted =
          Maps.filterKeys(tasks, Predicates.not(Predicates.in(retainedTasks.keySet())));
      cleanupTasks(deleted);
      tasks.keySet().removeAll(deleted.keySet());

      // Reconcile state with tasks that are known both locally and remotely, but may have different
      // states.
      reconcileStates(Maps.filterKeys(retainedTasks, Predicates.in(tasks.keySet())));
    }
  }

  private void cleanupTasks(Map<String, Task> tasks) {
    // Tasks that are still active.
    Map<String, Task> runningTasks = Maps.filterValues(tasks, IS_ACTIVE);
    if (!runningTasks.isEmpty()) {
      LOG.warning("Retained tasks excluded locally-active tasks " + runningTasks.keySet());
      for (Task task : runningTasks.values()) {
        task.terminate(ScheduleStatus.KILLED);
      }
    }

    for (Task task : tasks.values()) {
      File sandbox = task.getSandboxDir();
      try {
        fileDeleter.execute(sandbox);
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Failed to delete sandbox " + sandbox, e);
      }
    }
  }

  private final AtomicLong localActiveMismatch = Stats.exportLong("executor_local_active_mismatch");
  private final AtomicLong localInactiveMismatch =
      Stats.exportLong("executor_local_inactive_mismatch");

  @VisibleForTesting static final String REPLAY_STATUS_MSG = "Replaying lost status update";

  private void reconcileStates(Map<String, ScheduleStatus> expectedStatuses) {
    for (Map.Entry<String, ScheduleStatus> expected : expectedStatuses.entrySet()) {
      String taskId = expected.getKey();
      Task local = tasks.get(taskId);
      boolean activeLocally = IS_ACTIVE.apply(local);
      boolean activeRemotely = Tasks.ACTIVE_STATES.contains(expected.getValue());
      if (activeLocally && !activeRemotely) {
        LOG.warning("Terminating locally-active task with mismatched state: " + taskId);
        localActiveMismatch.incrementAndGet();
        local.terminate(ScheduleStatus.KILLED);
      } else if (activeRemotely && !activeLocally) {
        LOG.warning("Task considered active remotely but not locally: " + taskId);
        localInactiveMismatch.incrementAndGet();
        driver.sendStatusUpdate(taskId, local.getScheduleStatus(), Optional.of(REPLAY_STATUS_MSG));
      }
    }
  }

  @Override
  public Map<String, ScheduleStatus> get() {
    ImmutableMap.Builder<String, ScheduleStatus> statuses = ImmutableMap.builder();
    for (Map.Entry<String, Task> entry : tasks.entrySet()) {
      statuses.put(entry.getKey(), entry.getValue().getScheduleStatus());
    }
    return statuses.build();
  }

  /**
   * Cleanly shuts down the executor.
   *
   * @return The active tasks that were killed as the executor shut down.
   */
  public Iterable<Task> shutdownCore() {
    LOG.info("Shutting down executor core.");

    List<Future<Task>> results = stopLiveTasks();
    List<Task> killed = Lists.newArrayList();
    for (Future<Task> result : results) {
      try {
        Task task = result.get();
        if (!result.isCancelled() && task != null
            && task.getScheduleStatus() == ScheduleStatus.KILLED) {
          killed.add(task);
        }
      } catch (InterruptedException e) {
        // This can't happen since invokeAll returns when all tasks are completed or cancelled.
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.warning("Exception killing task: " + e.getCause());
      }
    }

    return killed;
  }

  private List<Future<Task>> stopLiveTasks() {
    try {
      return taskExecutor.invokeAll(Lists.newArrayList(Iterables.transform(getLiveTasks(),
          new Function<Task, Callable<Task>>() {
            @Override public Callable<Task> apply(final Task task) {
              return new Callable<Task>() {
                @Override public Task call() {
                  return stopLiveTask(task.getId());
                }
              };
            }
          })),
          SHUTDOWN_TIMEOUT_MS,
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Lists.newArrayList();
    }
  }

  @VisibleForTesting
  @Timed("executor_check_processes")
  void checkProcesses(Set<ProcessInfo> runningProcesses) {
    Predicate<ProcessInfo> shouldKill = new Predicate<ProcessInfo>() {
      @Override public boolean apply(ProcessInfo proc) {
        Task task = tasks.get(proc.getTaskId());
        if (task == null) {
          LOG.info("Unrecognized process " + proc);
          unrecognizedProcessesEncountered.incrementAndGet();
          return true;
        }

        if (!task.isRunning()) {
          LOG.info("Found running process for non-running task: " + proc);
          orphansEncountered.incrementAndGet();
          return true;
        }

        AssignedTask assignedTask = task.getAssignedTask();
        Set<Integer> assignedPorts = assignedTask.isSetAssignedPorts()
            ? ImmutableSet.copyOf(assignedTask.getAssignedPorts().values())
            : ImmutableSet.<Integer>of();

        Iterable<Integer> unallocatedProtectedPorts=
            Iterables.filter(Sets.difference(proc.getListenPorts(), assignedPorts), taskPortRange);

        if (!Iterables.isEmpty(unallocatedProtectedPorts)) {
          LOG.warning("Process listening on unallocated ports " + unallocatedProtectedPorts
              + ": " + proc);
          numUnallocatedPortsProcessesEncountered.incrementAndGet();
        }

        return false;
      }
    };

    Iterable<ProcessInfo> badProcesses =
        ImmutableList.copyOf(Iterables.filter(runningProcesses, shouldKill));

    for (ProcessInfo proc : badProcesses) {
      LOG.info("Killing process " + proc);

      try {
        processKiller.execute(new KillCommand(proc.getPid()));
        badProcessesKilled.incrementAndGet();
      } catch (KillException e) {
        LOG.warning("Failed to kill process " + proc.getPid());
      }
    }
  }

  @Timed("process_scanner_get_running_processes")
  private Set<ProcessInfo> fetchRunningProcesses() {
    return processScanner.getRunningProcesses();
  }

  private void startProcessScanner() {
    int scheduleInterval = processScannerTaskScheduleInterval.as(Time.SECONDS);
    LOG.info("Scheduled process scanner task with interval(seconds):" + scheduleInterval);
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        checkProcesses(fetchRunningProcesses());
      }
    }, scheduleInterval, scheduleInterval, TimeUnit.SECONDS);
  }
}
