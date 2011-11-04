package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.commons.io.FileSystemUtils;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanExecute;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.Exists;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BuildInfo;
import com.twitter.mesos.Message;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.executor.Task.TaskRunException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.comm.ExecutorStatus;
import com.twitter.mesos.gen.comm.SchedulerMessage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.mesos.gen.ScheduleStatus.*;

/**
 * ExecutorCore
 *
 * TODO(William Farner): When a LiveTask is terminated, should we replace its entry with a DeadTask?
 *
 * @author William Farner
 */
public class ExecutorCore implements TaskManager, Supplier<Map<String, ScheduleStatus>> {
  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());

  @CmdLine(name = "kill_orphan_task_schedule_interval",
           help = "the schedule interval of executing kill orphan task")
  private static final Arg<Amount<Integer, Time>> KILL_ORPHAN_TASK_SCHEDULE_INTERVAL =
      Arg.create(Amount.of(10, Time.MINUTES));

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

  private static final long SHUTDOWN_TIMEOUT_MS =
      Amount.of(20L, Time.SECONDS).as(Time.MILLISECONDS);

  private final Map<String, Task> tasks = Maps.newConcurrentMap();

  private final File executorRootDir;

  private final ScheduledExecutorService scheduledExecutorService =
      new ScheduledThreadPoolExecutor(1,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ExecutorSync-%d").build());

  private final AtomicReference<String> slaveId = new AtomicReference<String>();

  private final BuildInfo buildInfo;
  private final Function<AssignedTask, Task> taskFactory;
  private final ExecutorService taskExecutor;
  private final Function<Message, Integer> messageHandler;
  private final StateChangeListener stateChangeListener;
  private final ExceptionalClosure<KillCommand, KillException> processKiller;
  private final ProcessScanner processScanner;
  private final Predicate<String> isActiveTask;

  private final AtomicLong tasksReceived = Stats.exportLong("executor_tasks_received");
  private final AtomicLong taskFailures = Stats.exportLong("executor_task_launch_failures");
  private final AtomicLong tasksKilled = Stats.exportLong("executor_tasks_killed");


  @Inject
  public ExecutorCore(@ExecutorRootDir File executorRootDir, BuildInfo buildInfo,
      Function<AssignedTask, Task> taskFactory,
      @Named(TASK_EXECUTOR) ExecutorService taskExecutor,
      Function<Message, Integer> messageHandler,
      StateChangeListener stateChangeListener,
      ExceptionalClosure<KillCommand, KillException> processKiller) {
    this.executorRootDir = checkNotNull(executorRootDir);
    this.buildInfo = checkNotNull(buildInfo);
    this.taskFactory = checkNotNull(taskFactory);
    this.taskExecutor = checkNotNull(taskExecutor);
    this.messageHandler = checkNotNull(messageHandler);
    this.stateChangeListener = checkNotNull(stateChangeListener);
    this.processKiller = checkNotNull(processKiller);
    this.processScanner = new ProcessScanner(PROCESS_SCRAPER_SCRIPT.get());
    this.isActiveTask = new Predicate<String>() {
      @Override public boolean apply(String entry) {
        Task task = tasks.get(entry);
        return (task != null) && task.isRunning();
      }
    };
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
    new ResourceManager(this, executorRootDir, shutdownRegistry).start();
    startStateSync();
    startKillOrphanTask();
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

    stateChangeListener.changedState(taskId, STARTING, Optional.<String>absent());
    try {
      task.stage();
      stateChangeListener.changedState(taskId, RUNNING, Optional.<String>absent());
      task.run();
    } catch (TaskRunException e) {
      LOG.log(Level.SEVERE, "Failed to stage or run task " + taskId, e);
      taskFailures.incrementAndGet();
      task.terminate(FAILED);
      stateChangeListener.changedState(taskId, FAILED, Optional.of(e.getMessage()));
      deleteCompletedTask(taskId);
      return;
    }

    taskExecutor.execute(new Runnable() {
      @Override public void run() {
        LOG.info("Waiting for task " + taskId + " to complete.");
        ScheduleStatus state = task.blockUntilTerminated();
        LOG.info("Task " + taskId + " completed in state " + state);
        stateChangeListener.changedState(taskId, state, Optional.<String>absent());
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
    } else {
      LOG.info("Kill request for task in state " + task.getScheduleStatus() + " ignored.");
    }
    return task;
  }

  public Task getTask(String taskId) {
    return tasks.get(taskId);
  }

  public Iterable<Task> getTasks() {
    return Iterables.unmodifiableIterable(tasks.values());
  }

  @Override
  public Iterable<Task> getLiveTasks() {
    return Iterables.unmodifiableIterable(Iterables.filter(tasks.values(),
        new Predicate<Task>() {
          @Override public boolean apply(Task task) {
            return task.isRunning();
          }
        }));
  }

  @Override
  public boolean hasTask(String taskId) {
    return tasks.containsKey(taskId);
  }

  @Override
  public boolean isRunning(String taskId) {
    return hasTask(taskId) && tasks.get(taskId).isRunning();
  }

  @Override
  public void deleteCompletedTask(String taskId) {
    Preconditions.checkState(!isRunning(taskId), "Task " + taskId + " is still running!");
    tasks.remove(taskId);
    stateChangeListener.deleted(taskId);
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
    Iterable<Task> liveTasks = getLiveTasks();

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

  private void startStateSync() {
    // TODO(wfarner): Integrate this with the incremental sync messages.
    final Properties buildProperties = buildInfo.getProperties();

    final String DEFAULT = "unknown";
    final ExecutorStatus baseStatus = new ExecutorStatus()
        .setHost(Util.getHostName())
        .setBuildUser(buildProperties.getProperty(BuildInfo.Key.USER.value, DEFAULT))
        .setBuildMachine(buildProperties.getProperty(BuildInfo.Key.MACHINE.value, DEFAULT))
        .setBuildPath(buildProperties.getProperty(BuildInfo.Key.PATH.value, DEFAULT))
        .setBuildGitTag(buildProperties.getProperty(BuildInfo.Key.GIT_TAG.value, DEFAULT))
        .setBuildGitRevision(buildProperties.getProperty(BuildInfo.Key.GIT_REVISION.value, DEFAULT))
        .setBuildTimestamp(buildProperties.getProperty(BuildInfo.Key.TIMESTAMP.value, DEFAULT));

    Runnable syncer = new Runnable() {
      @Override public void run() {
        if (slaveId.get() == null) {
          LOG.severe("slaveID not set, can't send executor status to scheduler.");
          return;
        }

        ExecutorStatus status = new ExecutorStatus(baseStatus).setSlaveId(slaveId.get());

        try {
          status.setDiskFreeKb(FileSystemUtils.freeSpaceKb(executorRootDir.getAbsolutePath()));
        } catch (IOException e) {
          LOG.log(Level.INFO, "Failed to get disk free space.", e);
        }

        SchedulerMessage message = new SchedulerMessage();
        message.setExecutorStatus(status);
        messageHandler.apply(new Message(message));
      }
    };

    // TODO(William Farner): Make sync interval configurable.
    scheduledExecutorService.scheduleAtFixedRate(syncer, 30, 30, TimeUnit.SECONDS);
  }

  private void startKillOrphanTask() {
    Runnable killOrphanTask = new Runnable() {
      @Override public void run() {
        Map<Integer, String> runningProcesses = processScanner.getRunningProcesses();
        // Kill running tasks that we don't think they are running.
        Map<Integer, String> orphanTasks =
            Maps.filterValues(runningProcesses, Predicates.not(isActiveTask));
        LOG.info("Found orphan tasks: " + orphanTasks);
        for (Entry<Integer, String> entry : orphanTasks.entrySet()) {
          int pid = entry.getKey();
          String taskId = entry.getValue();
          LOG.info(String.format("Killing orphan task pid:%d task id: %s.", pid, taskId));
          try {
            processKiller.execute(new KillCommand(pid));
          } catch (KillException e) {
            LOG.warning(String.format(
                "Failed to kill orphan task pid:%d task id: %s.", pid, taskId));
          }
        }
      }
    };

    int scheduleInterval = KILL_ORPHAN_TASK_SCHEDULE_INTERVAL.get().as(Time.SECONDS);
    LOG.info("Scheduled kill orphan task with interval(seconds):" + scheduleInterval);
    scheduledExecutorService.scheduleAtFixedRate(
        killOrphanTask, scheduleInterval, scheduleInterval, TimeUnit.SECONDS);
  }
}
