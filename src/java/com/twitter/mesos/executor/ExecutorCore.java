package com.twitter.mesos.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.twitter.common.util.BuildInfo;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.FrameworkMessageCodec;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.Codec;
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
 * TODO(wfarner): Allow the Scheduler to pass a message to the Executor that instructs it to drain
 * itself of tasks.
 *
 * @author wfarner
 */
public class ExecutorCore {
  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());

  private final Map<Integer, RunningTask> tasks = Maps.newConcurrentMap();

  private static final byte[] EMPTY_MSG = new byte[0];

  private final File executorRootDir;

  private final AtomicReference<ExecutorDriver> driver = new AtomicReference<ExecutorDriver>();

  private final Amount<Long, Time> fileExpirationTime = Amount.of(8L, Time.HOURS);
  // TODO(wfarner): This needs to be configurable.
  private final Amount<Long, Data> maxDiskSpace = Amount.of(20L, Data.GB);

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
  private ExecutorCore(ExecutorMain.TwitterExecutorOptions options, BuildInfo buildInfo) {
    executorRootDir = Preconditions.checkNotNull(options.taskRootDir);
    if (!executorRootDir.exists()) {
      Preconditions.checkState(executorRootDir.mkdirs());
    }

    this.buildInfo = Preconditions.checkNotNull(buildInfo);

    // TODO(wfarner): This causes problems when multiple executors are running on the same machine.
    //    Two options - prevent multiple executors, or bake the slave ID into the executorRootDir.
    startGarbageCollector();
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
        pidFetcher, executorRootDir, taskId, taskInfo, fileCopier);

    try {
      runningTask.stage();
      runningTask.launch();
      sendStatusUpdate(driver, new TaskStatus(taskId, TaskState.TASK_RUNNING, EMPTY_MSG));
    } catch (RunningTask.ProcessException e) {
      LOG.log(Level.SEVERE, "Failed to stage task " + taskId, e);
      sendStatusUpdate(driver, new TaskStatus(taskId, TaskState.TASK_FAILED, EMPTY_MSG));
      return;
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Unhandled exception while launching task.", t);
      sendStatusUpdate(driver, new TaskStatus(taskId, TaskState.TASK_FAILED, EMPTY_MSG));
      return;
    }

    tasks.put(taskId, runningTask);

    executorService.execute(new Runnable() {
      @Override public void run() {
        LOG.info("Waiting for task " + taskId + " to complete.");
        ScheduleStatus state = runningTask.waitFor();
        LOG.info("Task " + taskId + " completed in state " + state);

        sendStatusUpdate(driver, new TaskStatus(taskId, StateTranslator.get(state), EMPTY_MSG));
      }
    });
  }

  public void stopRunningTask(int taskId) {
    RunningTask task = tasks.get(taskId);

    if (task != null) {
      LOG.info("Killing task: " + task);
      task.terminate(ScheduleStatus.KILLED);
    } else {
      LOG.severe("No such task found: " + taskId);
    }
  }

  public RunningTask getTask(int taskId) {
    return tasks.get(taskId);
  }

  public Iterable<RunningTask> getTasks() {
    return tasks.values();
  }

  public void shutdownCore() {
    for (Map.Entry<Integer, RunningTask> entry : tasks.entrySet()) {
      System.out.println("Killing task " + entry.getKey());
      stopRunningTask(entry.getKey());
    }
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

  private void startGarbageCollector() {
    FileFilter expiredOrUnknown = new FileFilter() {
        @Override public boolean accept(File file) {
          if (!file.isDirectory()) return false;

          String dirName = file.getName();
          int taskId;
          try {
            taskId = Integer.parseInt(dirName);
          } catch (NumberFormatException e) {
            return true;
          }

          // Always delete unknown directories.
          if (!tasks.containsKey(taskId)) return true;

          // If the directory is for a known task, only delete when it has expired.
          long timeSinceLastModify =
              System.currentTimeMillis() - DiskGarbageCollector.recursiveLastModified(file);
          return timeSinceLastModify > fileExpirationTime.as(Time.MILLISECONDS);
        }
      };

    Closure<File> gcCallback = new Closure<File>() {
      @Override public void execute(File file) throws RuntimeException {
        String dirName = file.getName();
        int taskId;
        try {
          taskId = Integer.parseInt(dirName);
        } catch (NumberFormatException e) {
          return; // No-op.
        }

        LOG.info("Removing record for garbage-collected task "  + taskId);
        tasks.remove(taskId);
      }
    };

    // The expired file GC always runs, and expunges all directories that are unknown or too old.
    DiskGarbageCollector expiredDirGc = new DiskGarbageCollector("ExpiredOrUnknownDir",
        executorRootDir, expiredOrUnknown, gcCallback);

    FileFilter completedTaskFileFilter = new FileFilter() {
        @Override public boolean accept(File file) {
          if (!file.isDirectory()) return false;

          String dirName = file.getName();
          int taskId;
          try {
            taskId = Integer.parseInt(dirName);
          } catch (NumberFormatException e) {
            LOG.info("Unrecognized file found while garbage collecting: " + file);
            return true;
          }

          return tasks.containsKey(taskId) && tasks.get(taskId).isCompleted();
        }
      };

    // The completed task GC only runs when disk is exhausted, and removes directories for tasks
    // that have completed.
    DiskGarbageCollector completedTaskGc = new DiskGarbageCollector("CompletedTask",
        executorRootDir, completedTaskFileFilter, maxDiskSpace, gcCallback);

    // TODO(wfarner): Make GC intervals configurable.
    ScheduledExecutorService gcExecutor = new ScheduledThreadPoolExecutor(2,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Disk GC-%d").build());
    gcExecutor.scheduleAtFixedRate(expiredDirGc, 1, 5, TimeUnit.MINUTES);
    gcExecutor.scheduleAtFixedRate(completedTaskGc, 2, 1, TimeUnit.MINUTES);
  }

  private static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Failed to look up own hostname.", e);
      return null;
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
        } catch (Codec.CodingException e) {
          LOG.log(Level.WARNING, "Failed to send executor status.", e);
        }
      }
    };

    // TODO(wfarner): Make sync interval configurable.
    syncExecutor.scheduleAtFixedRate(syncer, 30, 30, TimeUnit.SECONDS);
  }

  private void sendSchedulerMessage(SchedulerMessage message) throws Codec.CodingException {
    ExecutorDriver driverRef = driver.get();
    if (driverRef == null) {
      LOG.info("No driver available, unable to send executor status.");
      return;
    }

    int result = driverRef.sendFrameworkMessage(
        new FrameworkMessageCodec<SchedulerMessage>(SchedulerMessage.class).encode(message));
    if (result != 0) {
      LOG.warning("Scheduler message failed to send, return code " + result);
    }
  }

  private void startRegisteredTaskPusher() {
    Runnable pusher = new Runnable() {
      @Override public void run() {
        RegisteredTaskUpdate update = new RegisteredTaskUpdate()
            .setSlaveHost(getHostName());

        for (Map.Entry<Integer, RunningTask> task : tasks.entrySet()) {
          LiveTaskInfo info = new LiveTaskInfo();
          info.setTaskId(task.getKey());
          RunningTask runningTask = task.getValue();
          info.setTaskInfo(runningTask.getTask());
          info.setResources(runningTask.getResourceConsumption());
          info.setStatus(runningTask.getStatus());

          update.addToTaskInfos(info);
        }

        try {
          SchedulerMessage message = new SchedulerMessage();
          message.setTaskUpdate(update);

          sendSchedulerMessage(message);
        } catch (Codec.CodingException e) {
          LOG.log(Level.WARNING, "Failed to send executor status.", e);
        }
      }
    };

    // TODO(wfarner): Make push interval configurable.
    syncExecutor.scheduleAtFixedRate(pusher, 5, 5, TimeUnit.SECONDS);
  }
}
