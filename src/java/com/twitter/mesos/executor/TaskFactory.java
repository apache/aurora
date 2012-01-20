package com.twitter.mesos.executor;

import java.io.File;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.mesos.executor.FileToInt.FetchException;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.gen.AssignedTask;

/**
 * A factory that will create new tasks that can be run.
 *
 * @author William Farner
 */
public class TaskFactory implements Function<AssignedTask, Task> {

  private final ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker;
  private final ExceptionalClosure<KillCommand, KillException> processKiller;
  private final ExceptionalFunction<File, Integer, FetchException> pidFetcher;
  private final FileCopier fileCopier;
  private final File executorRootDir;
  private final boolean multiUser;

  @Inject
  public TaskFactory(@ExecutorRootDir File executorRootDir,
      ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker,
      ExceptionalClosure<KillCommand, KillException> processKiller,
      ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher,
      FileCopier fileCopier,
      @MultiUserMode boolean multiUser) {

    this.executorRootDir = Preconditions.checkNotNull(executorRootDir);
    this.healthChecker = Preconditions.checkNotNull(healthChecker);
    this.processKiller = Preconditions.checkNotNull(processKiller);
    this.pidFetcher = Preconditions.checkNotNull(pidFetcher);
    this.fileCopier = Preconditions.checkNotNull(fileCopier);
    this.multiUser = multiUser;
  }

  @Override
  public Task apply(AssignedTask task) {
    Preconditions.checkNotNull(task);

    return new LiveTask(healthChecker,
        processKiller,
        pidFetcher,
        new File(executorRootDir, task.getTaskId()),
        task,
        fileCopier,
        multiUser);
  }
}
