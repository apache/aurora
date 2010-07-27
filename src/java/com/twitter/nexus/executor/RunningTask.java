package com.twitter.nexus.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.twitter.common.Pair;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.StateMachine;
import com.twitter.nexus.executor.HealthChecker.HealthCheckException;
import com.twitter.nexus.executor.ProcessKiller.KillCommand;
import com.twitter.nexus.executor.ProcessKiller.KillException;
import com.twitter.nexus.gen.ResourceConsumption;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.TaskState;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles storing information and performing duties related to a running task.
 *
 * @author wfarner
 */
public class RunningTask {
  private static final Logger LOG = Logger.getLogger(RunningTask.class.getName());

  private static final Pattern PORT_REQUEST_PATTERN = Pattern.compile("%port:(\\w+)%");

  private static final String HEALTH_CHECK_PORT_NAME = "health";
  private static final Amount<Long, Time> LAUNCH_PIDFILE_GRACE_PERIOD = Amount.of(1L, Time.SECONDS);

  private final StateMachine<TaskState> stateMachine;

  private final SocketManager socketManager;
  private final ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker;
  private final ExceptionalClosure<KillCommand, KillException> processKiller;
  private final ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher;
  private KillCommand killCommand;

  private final int taskId;

  private final TwitterTaskInfo task;

  private int healthCheckPort = -1;

  @VisibleForTesting
  final File taskRoot;

  @VisibleForTesting
  protected final Map<String, Integer> leasedPorts = Maps.newHashMap();
  private Process process;

  private int exitCode = 0;
  private final ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier;

  public RunningTask(SocketManager socketManager,
      ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker,
      ExceptionalClosure<KillCommand, KillException> processKiller,
      ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher,
      File executorRoot, int taskId,
      TwitterTaskInfo task,
      ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier) {

    this.socketManager = Preconditions.checkNotNull(socketManager);
    this.healthChecker = Preconditions.checkNotNull(healthChecker);
    this.processKiller = Preconditions.checkNotNull(processKiller);
    this.pidFetcher = Preconditions.checkNotNull(pidFetcher);
    this.taskId = taskId;
    this.task = Preconditions.checkNotNull(task);

    Preconditions.checkNotNull(executorRoot);
    Preconditions.checkState(executorRoot.exists() && executorRoot.isDirectory());
    taskRoot = new File(executorRoot, String.format(
        "%s/%s/%d-%d", task.getOwner(), task.getJobName(), taskId, System.nanoTime()));
    this.fileCopier = Preconditions.checkNotNull(fileCopier);

    stateMachine = StateMachine.<TaskState>builder(toString())
          .initialState(TaskState.TASK_STARTING)
          .addState(TaskState.TASK_STARTING, TaskState.TASK_RUNNING, TaskState.TASK_FAILED)
          .addState(TaskState.TASK_RUNNING, TaskState.TASK_FINISHED,
                                            TaskState.TASK_FAILED,
                                            TaskState.TASK_KILLED,
                                            TaskState.TASK_LOST)
          .build();
  }

  /**
   * Performs staging operations necessary to launch a task.
   * This will prepare the working directory for the task, and download the binary to run.
   *
   * @throws RunningTask.ProcessException If there was an error that caused staging to fail.
   */
  public void stage() throws ProcessException {
    LOG.info(String.format("Staging task for job %s/%s", task.getOwner(), task.getJobName()));
    Preconditions.checkState(!taskRoot.exists());

    LOG.info("Building task directory hierarchy.");
    if (!taskRoot.mkdirs()) {
      throw new ProcessException(
          "Failed to create working directory: " + taskRoot.getAbsolutePath());
    }

    LOG.info("Fetching payload.");
    File payload;
    LOG.info("File copier: " + fileCopier);
    try {
      payload = fileCopier.apply(
          new FileCopyRequest(task.getHdfsPath(), taskRoot.getAbsolutePath()));
    } catch (IOException e) {
      throw new ProcessException("Failed to fetch task binary.", e);
    }

    if (!payload.exists()) {
      throw new ProcessException("Unexpected state - binary does not exist!");
    }
  }

  /**
   * Performs command-line expansion to assign managed port values where requested.
   *
   * @return A pair containing the expanded command line, and a map from port name to assigned
   *    port number.
   * @throws SocketManager.SocketLeaseException If there was a problem leasing a socket.
   * @throws ProcessException If multiple ports with the same name were requested.
   */
  @VisibleForTesting
  protected Pair<String, Map<String, Integer>> expandCommandLine()
      throws SocketManagerImpl.SocketLeaseException, ProcessException {
    Map<String, Integer> leasedPorts = Maps.newHashMap();

    LOG.info("Expanding command line " + task.getStartCommand());

    Matcher m = PORT_REQUEST_PATTERN.matcher(task.getStartCommand());

    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String portName = m.group(1);
      if (leasedPorts.containsKey(portName)) {
        throw new ProcessException(
            String.format("Port with name [%s] requested multiple times.", portName));
      }

      int portNumber = socketManager.leaseSocket();
      leasedPorts.put(portName, portNumber);
      m.appendReplacement(sb, String.valueOf(portNumber));
    }
    m.appendTail(sb);

    return Pair.of(sb.toString(), leasedPorts);
  }

  public void launch() throws ProcessException {
    LOG.info("Executing from working directory: " + taskRoot.getAbsolutePath());

    Pair<String, Map<String, Integer>> expansion;
    try {
      expansion = expandCommandLine();
    } catch (SocketManagerImpl.SocketLeaseException e) {
      LOG.info("Failed to get sockets!");
      throw new ProcessException("Failed to obtain requested sockets.", e);
    }

    String startCommand = expansion.getFirst();

    LOG.info("Obtained leases on ports: " + expansion.getSecond());
    leasedPorts.putAll(expansion.getSecond());

    if (leasedPorts.containsKey(HEALTH_CHECK_PORT_NAME)) {
      healthCheckPort = leasedPorts.get(HEALTH_CHECK_PORT_NAME);
    }

    List<String> commandLine = Arrays.asList(
        "bash",
        "-c",
        String.format("echo $$ > pidfile; %s >stdout 2>stderr", startCommand)
    );

    LOG.info("Executing shell command: " + commandLine);

    ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
    processBuilder.directory(taskRoot);

    try {
      process = processBuilder.start();

      if (supportsHttpSignals()) {
        // TODO(wfarner): Change to use ScheduledExecutorService, making sure to shut it down.
        new Timer(String.format("Task-%d-HealthCheck", taskId), true).scheduleAtFixedRate(
            new TimerTask() {
              @Override public void run() {
                if (!isHealthy()) {
                  LOG.info("Task not healthy!");
                  terminate(TaskState.TASK_FAILED);
                }
              }
            },
            // Configure health check interval, allowing 2x configured time for startup.
            // TODO(wfarner): Add a configuration option for the task start-up grace period
            // before health checking begins.
            2 * Amount.of(task.getHealthCheckIntervalSecs(), Time.SECONDS).as(Time.MILLISECONDS),
            Amount.of(task.getHealthCheckIntervalSecs(), Time.SECONDS).as(Time.MILLISECONDS));
      }

      // TODO(wfarner): After a grace period, read the pidfile to get the parent PID and construct
      //    the KillCommand
      try {
        Thread.currentThread().sleep(LAUNCH_PIDFILE_GRACE_PERIOD.as(Time.MILLISECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ProcessException("Interrupted while waiting for launch grace period.", e);
      }
      buildKillCommand(supportsHttpSignals() ? leasedPorts.get(HEALTH_CHECK_PORT_NAME) : -1);

      stateMachine.transition(TaskState.TASK_RUNNING);
    } catch (IOException e) {
      stateMachine.transition(TaskState.TASK_FAILED);
      throw new ProcessException("Failed to launch process.", e);
    }
  }

  private void buildKillCommand(int healthCheckPort) throws ProcessException {
    int pid = 0;
    try {
      pid = pidFetcher.apply(new File(taskRoot, "pidfile"));
    } catch (FileToInt.FetchException e) {
      LOG.log(Level.WARNING, "Failed to read pidfile for " + this, e);
      throw new ProcessException("Failed to read pidfile.", e);
    }

    killCommand = new ProcessKiller.KillCommand(pid, healthCheckPort);
  }

  private boolean supportsHttpSignals() {
    return healthCheckPort != -1;
  }

  /**
   * Waits for the launched task to terminate.
   *
   * @return The state that the task was in upon termination.
   */
  public TaskState waitFor() {
    Preconditions.checkNotNull(process);

    while (stateMachine.getState() == TaskState.TASK_RUNNING) {
      try {
        exitCode = process.waitFor();
        LOG.info("Process terminated with exit code: " + exitCode);

        if (stateMachine.getState() != TaskState.TASK_KILLED) {
          stateMachine.transition(exitCode == 0 ? TaskState.TASK_FINISHED : TaskState.TASK_FAILED);
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING,
            "Warning, Thread interrupted while waiting for process to finish.", e);
      }
    }

    // Return leased ports.
    for (int port : leasedPorts.values()) {
      socketManager.returnSocket(port);
    }
    leasedPorts.clear();

    return stateMachine.getState();
  }

  public boolean isRunning() {
    return stateMachine.getState() == TaskState.TASK_RUNNING;
  }

  public int getExitCode() {
    return exitCode;
  }

  public void tearDown() throws IOException {
    Preconditions.checkNotNull(taskRoot);
    File jobDir = taskRoot.getParentFile();
    File ownerDir = jobDir.getParentFile();
    FileUtils.deleteDirectory(taskRoot);
    if (jobDir.list().length == 0) FileUtils.deleteDirectory(jobDir);
    if (ownerDir.list().length == 0) FileUtils.deleteDirectory(ownerDir);
  }

  public void terminate(TaskState terminalState) {
    Preconditions.checkNotNull(process);
    LOG.info("Terminating task " + this);
    stateMachine.transition(terminalState);

    try {
      processKiller.execute(killCommand);
    } catch (ProcessKiller.KillException e) {
      LOG.log(Level.WARNING, "Failed to kill process " + this, e);
    }

    process.destroy();
    waitFor();
  }

  public ResourceConsumption getResourceConsumption() {
    return new ResourceConsumption()
        .setLeasedPorts(ImmutableMap.copyOf(leasedPorts));
  }

  private boolean isHealthy() {
    if (!supportsHttpSignals()) return true;
    try {
      return healthChecker.apply(healthCheckPort);
    } catch (HealthCheckException e) {
      LOG.log(Level.INFO, String.format("Health check for %s on port %d failed.",
          this, healthCheckPort), e);
      return false;
    }
  }

  public String toString() {
    return String.format("%s/%s/%d", task.getOwner(), task.getJobName(), taskId);
  }

  class ProcessException extends Exception {
    public ProcessException(String msg, Throwable t) {
      super(msg, t);
    }

    public ProcessException(String msg) {
      super(msg);
    }
  }
}
