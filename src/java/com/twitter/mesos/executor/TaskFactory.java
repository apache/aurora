package com.twitter.mesos.executor;

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
import com.twitter.mesos.scheduler.ExecutorRootDir;

import java.io.File;
import java.io.IOException;

/**
 * A factory that will create new tasks that can be run.
 *
 * @author wfarner
 */
public class TaskFactory implements Function<AssignedTask, Task> {

  private final SocketManager socketManager;
  private final ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker;
  private final ExceptionalClosure<KillCommand, KillException> processKiller;
  private final ExceptionalFunction<File, Integer, FetchException> pidFetcher;
  private final ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier;
  private final File executorRootDir;

  @Inject
  public TaskFactory(@ExecutorRootDir File executorRootDir,
      SocketManager socketManager,
      ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker,
      ExceptionalClosure<KillCommand, KillException> processKiller,
      ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher,
      ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier) {

    this.executorRootDir = Preconditions.checkNotNull(executorRootDir);
    this.socketManager = Preconditions.checkNotNull(socketManager);
    this.healthChecker = Preconditions.checkNotNull(healthChecker);
    this.processKiller = Preconditions.checkNotNull(processKiller);
    this.pidFetcher = Preconditions.checkNotNull(pidFetcher);
    this.fileCopier = Preconditions.checkNotNull(fileCopier);
  }

  @Override
  public Task apply(AssignedTask task) {
    Preconditions.checkNotNull(task);

    return new LiveTask(socketManager, healthChecker, processKiller,
      pidFetcher, new File(executorRootDir, task.getTaskId()), task, fileCopier);
  }
}
