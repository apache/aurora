package com.twitter.mesos.executor;

import com.google.common.base.Preconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.States;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TwitterTaskInfo;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A task that has completed execution.
 *
 * @author wfarner
 */
public class DeadTask implements Task {

  private static Logger LOG = Logger.getLogger(DeadTask.class.getName());

  private final File taskRoot;
  private final int taskId;
  private final TwitterTaskInfo task;
  private final ScheduleStatus state;

  // Lazy-loaded fields.
  private Amount<Long, Data> diskConsumed = null;

  DeadTask(File taskRoot, int taskId, TwitterTaskInfo task, ScheduleStatus state)  {
    this.taskRoot = Preconditions.checkNotNull(taskRoot);
    this.taskId = taskId;
    this.task = Preconditions.checkNotNull(task);
    this.state = Preconditions.checkNotNull(state);
    Preconditions.checkState(!States.isActive(state),
        "A dead task may not be assigned an active state.");
  }

  /**
   * Loads a dead task from information stored in its sandbox directory.
   *
   * @param taskRoot Sandbox directory for the dead task.
   * @return A new dead task object.
   * @throws DeadTaskLoadException If there was a problem loading the task.
   */
  static DeadTask loadFrom(File taskRoot) throws DeadTaskLoadException {
    Preconditions.checkNotNull(taskRoot);
    Preconditions.checkArgument(taskRoot.exists(), "Sandbox does not exist: " + taskRoot);
    Preconditions.checkArgument(taskRoot.isDirectory(), "Not a directory: " + taskRoot);

    LOG.info("Loading dead task from " + taskRoot);

    // Default to KILLED state.
    ScheduleStatus state = ScheduleStatus.KILLED;
    try {
      ScheduleStatus recoveredState = TaskUtils.getTaskStatus(taskRoot);
      if (!States.isActive(recoveredState)) state = recoveredState;
    } catch (FileToInt.FetchException e) {
      LOG.log(Level.WARNING, "Failed to load task status from " + taskRoot, e);
    } catch (FileNotFoundException e) {
      LOG.log(Level.INFO, "No task status file found in " + taskRoot);
    }

    try {
      return new DeadTask(taskRoot, TaskUtils.getTaskId(taskRoot), TaskUtils.fetchTask(taskRoot),
          state);
    } catch (IOException e) {
      throw new DeadTaskLoadException("Failed to read persisted task state from " + taskRoot);
    } catch (ThriftBinaryCodec.CodingException e) {
      throw new DeadTaskLoadException("Failed to deserialize task information from " + taskRoot, e);
    }
  }

  @Override
  public int getId() {
    return taskId;
  }

  @Override
  public File getRootDir() {
    return taskRoot;
  }

  @Override
  public boolean isRunning() {
    return false;
  }

  @Override
  public void terminate(ScheduleStatus terminalState) {
    throw new UnsupportedOperationException("The state of a dead task cannot be changed.");
  }

  @Override
  public TwitterTaskInfo getTaskInfo() {
    return task;
  }

  @Override
  public ScheduleStatus getScheduleStatus() {
    return state;
  }

  @Override
  public ResourceConsumption getResourceConsumption() {
    if (diskConsumed == null) {
      diskConsumed = Amount.of(FileUtils.sizeOfDirectory(taskRoot), Data.BYTES);
    }

    return new ResourceConsumption().setDiskUsedMb(diskConsumed.as(Data.MB).intValue());
  }

  @Override
  public void run() {
    throw new UnsupportedOperationException("A dead task cannot be run.");
  }

  public static class DeadTaskLoadException extends Exception {
    public DeadTaskLoadException(String msg) {
      super(msg);
    }
    public DeadTaskLoadException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
