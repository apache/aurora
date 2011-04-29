package com.twitter.mesos.executor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task that has state persisted on disk, and may be restored from disk.
 *
 * The persisted hierarchy is as follows:
 *
 * $TASK_ROOT - root task directory.
 * $TASK_ROOT/task.dump - Serialized AssignedTask thrift message.
 * $TASK_ROOT/task.status - Last known status of the task.
 *
 * The implementing task may create other files, as long as they do not interfere with the above.
 *
 * @author William Farner
 */
public abstract class TaskOnDisk implements Task {

  private static final Logger LOG = Logger.getLogger(TaskOnDisk.class.getName());

  // Path to file for the serialized AssignedTask dump file.
  @VisibleForTesting static final String TASK_DUMP_FILE = "task.dump";

  // Path to file for the serialized task status.
  @VisibleForTesting static final String TASK_STATUS_FILE = "task.status";

  @VisibleForTesting static final String SANDBOX_DIR_NAME = "sandbox";

  // Reads the status value from a file.
  private static final FileToInt STATUS_FETCHER = new FileToInt();

  @VisibleForTesting protected final File taskRoot;
  @VisibleForTesting protected final File sandboxDir;

  public TaskOnDisk(File taskRoot) {
    this.taskRoot = checkNotNull(taskRoot);
    this.sandboxDir = new File(taskRoot, SANDBOX_DIR_NAME);
  }

  /**
   * Persists the task assignment for this task.
   *
   * @throws TaskStorageException If there was a problem recording the task.
   */
  public void recordTask() throws TaskStorageException {
    checkTaskRoot(taskRoot);

    AssignedTask task = getAssignedTask();
    checkNotNull(task);

    try {
      Files.write(ThriftBinaryCodec.encode(task), new File(taskRoot, TASK_DUMP_FILE));
    } catch (IOException e) {
      throw new TaskStorageException("Failed to write task.", e);
    } catch (CodingException e) {
      throw new TaskStorageException("Failed to serialize task for storage.", e);
    }
  }

  /**
   * Restores the serialized task.
   *
   * @return The restored task assignment.
   * @throws TaskStorageException If the task could not be restored.
   */
  protected AssignedTask restoreTask() throws TaskStorageException {
    LOG.info("Restoring task from " + taskRoot);

    checkTaskRoot(taskRoot);

    File serializedTask = new File(taskRoot, TASK_DUMP_FILE);
    if (!serializedTask.exists()) {
      throw new TaskStorageException("Expected to find task dump file: " + serializedTask);
    }

    try {
      return ThriftBinaryCodec.decode(AssignedTask.class, Files.toByteArray(serializedTask));
    } catch (CodingException e) {
      throw new TaskStorageException("Failed to deserialize stored task.", e);
    } catch (IOException e) {
      throw new TaskStorageException("Failed to read stored task.", e);
    }
  }

  /**
   * Records the status for this task.
   *
   * @param status The status value to record.
   * @throws TaskStorageException If the status could not be recorded.
   */
  protected void recordStatus(ScheduleStatus status) throws TaskStorageException {
    try {
    Files.write(String.valueOf(status.getValue()), new File(taskRoot, TASK_STATUS_FILE),
        Charsets.US_ASCII);
    } catch (IOException e) {
      throw new TaskStorageException("Failed to write status.", e);
    }
  }

  /**
   * Restores the status associated with this task.
   * @return The recoeded status.
   * @throws TaskStorageException If the status could not be restored.
   */
  protected ScheduleStatus restoreStatus() throws TaskStorageException {
    // Default to KILLED state.
    ScheduleStatus state = ScheduleStatus.KILLED;
    try {
      File statusFile = new File(taskRoot, TASK_STATUS_FILE);
      if (!statusFile.exists()) {
        throw new FileNotFoundException("Status file does not exist: " + statusFile);
      }

      ScheduleStatus recoveredState = ScheduleStatus.findByValue(STATUS_FETCHER.apply(statusFile));
      if (!Tasks.isActive(recoveredState)) state = recoveredState;
    } catch (FileToInt.FetchException e) {
      LOG.log(Level.WARNING, "Failed to load task status from " + taskRoot, e);
    } catch (FileNotFoundException e) {
      LOG.log(Level.INFO, "No task status file found in " + taskRoot);
    }

    return state;
  }

  @Override
  public File getSandboxDir() {
    return sandboxDir;
  }

  public static class TaskStorageException extends Exception {
    public TaskStorageException(String msg) {
      super(msg);
    }

    public TaskStorageException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  private static void checkTaskRoot(File taskRoot) {
    checkNotNull(taskRoot);
    Preconditions.checkArgument(taskRoot.exists(), "Task root does not exist: " + taskRoot);
    Preconditions.checkArgument(taskRoot.isDirectory(), "Not a directory: " + taskRoot);
  }
}
