package com.twitter.nexus.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.ExecutorDriver;
import nexus.TaskDescription;
import nexus.TaskState;
import nexus.TaskStatus;

import java.io.File;
import java.io.IOException;
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
  private ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier;

  @Inject
  SocketManager socketManager;

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
    final RunningTask runningTask = new RunningTask(socketManager,
        taskRootDir, task.getTaskId(), taskInfo, fileCopier);

    try {
      runningTask.stage();
      runningTask.launch();
    } catch (RunningTask.ProcessException e) {
      LOG.log(Level.SEVERE, "Failed to stage task " + task.getTaskId(), e);
      sendStatusUpdate(driver, new TaskStatus(task.getTaskId(), TaskState.TASK_FAILED, EMPTY_MSG));
      return;
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Unhandled exception while launching task.", t);
      sendStatusUpdate(driver, new TaskStatus(task.getTaskId(), TaskState.TASK_FAILED, EMPTY_MSG));
      return;
    }

    tasks.put(task.getTaskId(), runningTask);

    executorService.execute(new Runnable() {
      @Override public void run() {
        LOG.info("Waiting for process to complete...");
        TaskState state = runningTask.waitFor();
        LOG.info("Process completed in state " + state);

        tasks.remove(task.getTaskId());
        sendStatusUpdate(driver, new TaskStatus(task.getTaskId(), state, EMPTY_MSG));
      }
    });
  }

  public void stopRunningTask(ExecutorDriver driver, int taskId) {
    RunningTask task = tasks.remove(taskId);

    if (task != null) {
      LOG.info("Killing task: " + task);
      task.terminate(TaskState.TASK_KILLED);
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
