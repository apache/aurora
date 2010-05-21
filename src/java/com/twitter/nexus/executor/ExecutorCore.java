package com.twitter.nexus.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.ExecutorDriver;
import nexus.TaskDescription;
import nexus.TaskState;
import nexus.TaskStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ExecutorCore
 *
 * TODO(wfarner): Allow the Scheduler to pass a message to the Executor that instructs it to drain
 * itself of tasks.
 *
 * @author Florian Leibert
 */
public class ExecutorCore {
  private static final Logger LOG = Logger.getLogger(ExecutorHub.class.getName());

  private final Map<Integer, RunningTask> tasks = Maps.newConcurrentMap();

  private static final byte[] EMPTY_MSG = new byte[0];

  private final File taskRootDir;

  private final ExecutorService executorService = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("NexusExecutor-[%d]").build());

  @Inject
  private FileSystem hadoopFileSystem;

  @Inject
  private ExecutorCore(ExecutorMain.TwitterExecutorOptions options) {
    taskRootDir = Preconditions.checkNotNull(options.taskRootDir);
    if (!taskRootDir.exists()) {
      Preconditions.checkState(taskRootDir.mkdirs());
    }
  }

  // TODO(flo): Handle loss of connection with the ExecutorDriver.
  // TODO(flo): Do input validation on parameters.
  public void executePendingTask(final ExecutorDriver driver, final TwitterTaskInfo taskInfo,
                                 final TaskDescription task) {
    LOG.info("Received task for execution: " + taskInfo);
    final RunningTask runningTask = new RunningTask(taskRootDir, task.getTaskId(), taskInfo);

    try {
      runningTask.stage();
      runningTask.launch();
    } catch (RunningTask.ProcessException e) {
      LOG.log(Level.SEVERE, "Failed to stage task " + task.getTaskId(), e);
      sendStatusUpdate(driver, new TaskStatus(task.getTaskId(), TaskState.TASK_FAILED, EMPTY_MSG));
      return;
    }

    tasks.put(task.getTaskId(), runningTask);

    executorService.execute(new Runnable() {
      @Override public void run() {
        LOG.info("Waiting for process to complete...");
        TaskState state = runningTask.waitFor();
        LOG.info("Process completed.");

        tasks.remove(task.getTaskId());
        sendStatusUpdate(driver, new TaskStatus(task.getTaskId(), state, EMPTY_MSG));
      }
    });
  }

  public void stopRunningTask(ExecutorDriver driver, int taskId) {
    RunningTask task = tasks.remove(taskId);

    if (task != null) {
      LOG.info("Killing task: " + task);
      task.kill();

      if (driver != null) {
        sendStatusUpdate(driver, new TaskStatus(taskId, TaskState.TASK_KILLED, EMPTY_MSG));
      } else {
        // TODO(flo): Remove this once a driver reference is retained and auto-reconnects.
        LOG.warning("No driver available, unable to send status signals.");
      }
    } else {
      LOG.severe("No such task found: " + taskId);
    }
  }

  public void shutdownCore(ExecutorDriver driver) {
    for (Map.Entry<Integer, RunningTask> entry : tasks.entrySet()) {
      System.out.println("Killing task " + entry.getKey());
      stopRunningTask(driver, entry.getKey());
    }
  }

  @VisibleForTesting
  void sendStatusUpdate(ExecutorDriver driver, TaskStatus status) {
    Preconditions.checkNotNull(status);
    if (driver != null) {
      driver.sendStatusUpdate(status);
    } else {
      LOG.severe("No executor driver available, unable to send signals.");
    }
  }
}
