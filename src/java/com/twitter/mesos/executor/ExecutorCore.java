package com.twitter.mesos.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.twitter.common.util.BuildInfo;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.gen.ExecutorStatus;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerMessage;
import com.twitter.mesos.gen.TwitterTaskInfo;
import mesos.ExecutorDriver;
import mesos.FrameworkMessage;
import mesos.TaskState;
import mesos.TaskStatus;
import org.apache.commons.io.FileSystemUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ExecutorCore
 *
 * TODO(wfarner): When a RunningTask is terminated, should we replace its entry with a DeadTask?
 *
 * @author wfarner
 */
public class ExecutorCore implements TaskManager {
  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());

  /**
   * {@literal @Named} binding key for the executor root directory.
   */
  static final String EXECUTOR_ROOT_DIR =
      "com.twitter.mesos.executor.ExecutorCore.EXECUTOR_ROOT_DIR";

  private final Map<Integer, Task> tasks = Maps.newConcurrentMap();

  private static final byte[] EMPTY_MSG = new byte[0];

  private final File executorRootDir;

  private final ResourceManager resourceManager;

  private final AtomicReference<ExecutorDriver> driver = new AtomicReference<ExecutorDriver>();

  private final ExecutorService executorService = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("MesosExecutor-[%d]").build());

  private final ScheduledExecutorService syncExecutor = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ExecutorSync-%d").build());

  @Inject ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier;
  @Inject SocketManager socketManager;
  @Inject ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker;
  @Inject ExceptionalClosure<KillCommand, KillException> processKiller;
  @Inject ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher;

  private final AtomicReference<String> slaveId = new AtomicReference<String>();
  private final BuildInfo buildInfo;

  @Inject
  private ExecutorCore(@Named(EXECUTOR_ROOT_DIR) File taskRootDir, BuildInfo buildInfo) {
    executorRootDir = Preconditions.checkNotNull(taskRootDir);
    if (!executorRootDir.exists()) {
      Preconditions.checkState(executorRootDir.mkdirs(), "Failed to create executor root dir.");
    }

    this.buildInfo = Preconditions.checkNotNull(buildInfo);

    resourceManager = new ResourceManager(this, executorRootDir);
    resourceManager.start();

    loadDeadTasks();
    startStateSync();
    startRegisteredTaskPusher();
  }

  void setSlaveId(String slaveId) {
    this.slaveId.set(Preconditions.checkNotNull(slaveId));
  }

  // TODO(flo): Handle loss of connection with the ExecutorDriver.
  // TODO(flo): Do input validation on parameters.
  public void executePendingTask(final ExecutorDriver driver, final TwitterTaskInfo taskInfo,
                                 final int taskId) {
    this.driver.set(driver);

    LOG.info(String.format("Received task for execution: %s/%s - %d", taskInfo.getOwner(),
        taskInfo.getJobName(), taskId));
    final RunningTask runningTask = new RunningTask(socketManager, healthChecker, processKiller,
        pidFetcher, getTaskRoot(taskId), taskId, taskInfo, fileCopier);

    tasks.put(taskId, runningTask);

    try {
      runningTask.stage();
      runningTask.run();
      sendStatusUpdate(driver, new TaskStatus(taskId, TaskState.TASK_RUNNING, EMPTY_MSG));
    } catch (RunningTask.TaskRunException e) {
      LOG.log(Level.SEVERE, "Failed to stage task " + taskId, e);
      runningTask.terminate(ScheduleStatus.FAILED);
      deleteCompletedTask(taskId);
      sendStatusUpdate(driver, new TaskStatus(taskId, TaskState.TASK_FAILED, EMPTY_MSG));
      return;
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Unhandled exception while launching task.", t);
      runningTask.terminate(ScheduleStatus.FAILED);
      deleteCompletedTask(taskId);
      sendStatusUpdate(driver, new TaskStatus(taskId, TaskState.TASK_FAILED, EMPTY_MSG));
      return;
    }

    executorService.execute(new Runnable() {
      @Override public void run() {
        LOG.info("Waiting for task " + taskId + " to complete.");
        ScheduleStatus state = runningTask.waitFor();
        LOG.info("Task " + taskId + " completed in state " + state);

        sendStatusUpdate(driver, new TaskStatus(taskId, StateTranslator.get(state), EMPTY_MSG));
      }
    });
  }

  private File getTaskRoot(int taskId) {
    return new File(executorRootDir, String.valueOf(taskId));
  }

  public void stopRunningTask(int taskId) {
    Task task = tasks.get(taskId);

    if (task != null && task.isRunning()) {
      LOG.info("Killing task: " + task);
      task.terminate(ScheduleStatus.KILLED);
    } else if (task == null) {
      LOG.severe("No such task found: " + taskId);
    }
  }

  public Task getTask(int taskId) {
    return tasks.get(taskId);
  }

  public Iterable<Task> getTasks() {
    return Iterables.unmodifiableIterable(tasks.values());
  }

  @Override
  public Iterable<Task> getRunningTasks() {
    return Iterables.unmodifiableIterable(Iterables.filter(tasks.values(),
        new Predicate<Task>() {
          @Override public boolean apply(Task task) {
            return task.isRunning();
          }
        }));
  }

  @Override
  public boolean hasTask(int taskId) {
    return tasks.containsKey(taskId);
  }

  @Override
  public boolean isRunning(int taskId) {
    return hasTask(taskId) && tasks.get(taskId).isRunning();
  }

  @Override
  public void deleteCompletedTask(int taskId) {
    Preconditions.checkArgument(!isRunning(taskId), "Task " + taskId + " is still running!");
    tasks.remove(taskId);
  }

  /**
   * Cleanly shuts down the executor.
   *
   * @return The active tasks that were killed as the executor shut down.
   */
  public Iterable<Task> shutdownCore() {
    LOG.info("Shutting down executor core.");
    Iterable<Task> runningTasks = getRunningTasks();
    for (Task task : runningTasks) {
      stopRunningTask(task.getId());
    }

    return runningTasks;
  }

  @VisibleForTesting
  void sendStatusUpdate(ExecutorDriver driver, TaskStatus status) {
    Preconditions.checkNotNull(status);
    if (driver != null) {
      LOG.info("Notifying task " + status.getTaskId() + " in state " + status.getState());
      driver.sendStatusUpdate(status);
    } else {
      LOG.severe("No executor driver available, unable to send signals.");
    }
  }

  private static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Failed to look up own hostname.", e);
      return null;
    }
  }

  private static final FileFilter DIR_FILTER = new FileFilter() {
    @Override public boolean accept(File file) {
      return file.isDirectory();
    }
  };

  private void loadDeadTasks() {
    LOG.info("Attempting to recover information about dead tasks from " + executorRootDir);
    for (File file : executorRootDir.listFiles(DIR_FILTER)) {
      try {
        DeadTask task = DeadTask.loadFrom(file);
        TwitterTaskInfo taskInfo = task.getTaskInfo();
        LOG.info("Recovered task " + task.getId() + " "
                 + taskInfo.getOwner() + "/" + taskInfo.getJobName());
        tasks.put(task.getId(), task);
      } catch (DeadTask.DeadTaskLoadException e) {
        LOG.log(Level.INFO, "Unable to restore task from " + file, e);
      }
    }
  }

  private void startStateSync() {
    final Properties buildProperties = buildInfo.getProperties();

    final String DEFAULT = "unknown";
    final ExecutorStatus baseStatus = new ExecutorStatus()
        .setHost(getHostName())
        .setBuildUser(buildProperties.getProperty(BuildInfo.Key.USER.value, DEFAULT))
        .setBuildMachine(buildProperties.getProperty(BuildInfo.Key.MACHINE.value, DEFAULT))
        .setBuildPath(buildProperties.getProperty(BuildInfo.Key.PATH.value, DEFAULT))
        .setBuildGitTag(buildProperties.getProperty(BuildInfo.Key.GIT_TAG.value, DEFAULT))
        .setBuildGitRevision(buildProperties.getProperty(BuildInfo.Key.GIT_REVISION.value, DEFAULT))
        .setBuildTimestamp(buildProperties.getProperty(BuildInfo.Key.TIMESTAMP.value, DEFAULT));

    Runnable syncer = new Runnable() {
      @Override public void run() {
        if (slaveId.get() == null) return;

        ExecutorStatus status = new ExecutorStatus(baseStatus)
            .setSlaveId(slaveId.get());

        try {
          status.setDiskFreeKb(FileSystemUtils.freeSpaceKb(executorRootDir.getAbsolutePath()));
        } catch (IOException e) {
          LOG.log(Level.INFO, "Failed to get disk free space.", e);
        }

        LOG.info("Sending executor status update: " + status);


          SchedulerMessage message = new SchedulerMessage();
          message.setExecutorStatus(status);
        try {
          sendSchedulerMessage(message);
        } catch (CodingException e) {
          LOG.log(Level.WARNING, "Failed to send executor status.", e);
        }
      }
    };

    // TODO(wfarner): Make sync interval configurable.
    syncExecutor.scheduleAtFixedRate(syncer, 30, 30, TimeUnit.SECONDS);
  }

  private void sendSchedulerMessage(SchedulerMessage message) throws CodingException {
    ExecutorDriver driverRef = driver.get();
    if (driverRef == null) {
      LOG.info("No driver available, unable to send executor status.");
      return;
    }

    FrameworkMessage frameworkMessage = new FrameworkMessage();
    frameworkMessage.setData(ThriftBinaryCodec.encode(message));

    int result = driverRef.sendFrameworkMessage(frameworkMessage);
    if (result != 0) {
      LOG.warning("Scheduler message failed to send, return code " + result);
    }
  }

  private void startRegisteredTaskPusher() {
    Runnable pusher = new Runnable() {
      @Override public void run() {
        RegisteredTaskUpdate update = new RegisteredTaskUpdate()
            .setSlaveHost(getHostName());

        for (Map.Entry<Integer, Task> task : tasks.entrySet()) {
          LiveTaskInfo info = new LiveTaskInfo();
          info.setTaskId(task.getKey());
          Task runningTask = task.getValue();
          info.setTaskInfo(runningTask.getTaskInfo());
          info.setResources(runningTask.getResourceConsumption());
          info.setStatus(runningTask.getScheduleStatus());

          update.addToTaskInfos(info);
        }

        try {
          SchedulerMessage message = new SchedulerMessage();
          message.setTaskUpdate(update);

          sendSchedulerMessage(message);
        } catch (CodingException e) {
          LOG.log(Level.WARNING, "Failed to send executor status.", e);
        }
      }
    };

    // TODO(wfarner): Make push interval configurable.
    syncExecutor.scheduleAtFixedRate(pusher, 5, 5, TimeUnit.SECONDS);
  }
}
