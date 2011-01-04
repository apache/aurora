package com.twitter.mesos.executor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.executor.TaskOnDisk.TaskStorageException;
import com.twitter.mesos.gen.TwitterTaskInfo;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles loading of dead tasks that were previously persisted.
 *
 * @author wfarner
 */
public class DeadTaskLoader implements Supplier<Iterable<Task>> {
  private static final Logger LOG = Logger.getLogger(DeadTaskLoader.class.getName());

  private final File taskRoot;

  /**
   * Creates a new dead task loader.
   *
   * @param taskRoot Root directory to scan for persisted dead task state.
   */
  @Inject
  public DeadTaskLoader(@ExecutorRootDir File taskRoot) {
    this.taskRoot = Preconditions.checkNotNull(taskRoot);
  }

  private static final FileFilter DIR_FILTER = new FileFilter() {
    @Override public boolean accept(File file) {
      return file.isDirectory();
    }
  };

  private static final Function<File, Task> TASK_LOADER = new Function<File, Task>() {
    @Override public Task apply(File taskDir) {
      try {
        LOG.info("Restoring task " + taskDir);
        DeadTask task = new DeadTask(taskDir);
        TwitterTaskInfo taskInfo = task.getAssignedTask().getTask();
        if (taskInfo != null) {
          if (StringUtils.isEmpty(task.getId())) {
            LOG.warning("Restored task, but task ID was empty: " + taskDir);
            return null;
          }

          LOG.info("Recovered task " + task.getId() + " " + Tasks.jobKey(taskInfo));
          return task;
        } else {
          LOG.info("Failed to restore task from " + taskDir);
          return null;
        }
      } catch (TaskStorageException e) {
        LOG.log(Level.INFO, "Unable to restore task from " + taskDir, e);
        return null;
      }
    }
  };

  @Override public Iterable<Task> get() {
    LOG.info("Attempting to recover information about dead tasks from " + taskRoot);

    List<File> taskDirs = Arrays.asList(taskRoot.listFiles(DIR_FILTER));
    return ImmutableList.copyOf(Iterables.filter(
        Iterables.transform(taskDirs, TASK_LOADER), Predicates.notNull()));
  }
}
