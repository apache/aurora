package com.twitter.mesos.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.twitter.common.collections.Pair;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TwitterTaskInfo;

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
 * This will create a directory to hold everything realted to a task, as well as a sandbox that the
 * launch command has access to.  The following files will be created:
 *
 * $TASK_ROOT - root task directory.
 * $TASK_ROOT/pidfile  - PID of the launched process.
 * $TASK_ROOT/sandbox/  - Sandbox, working directory for the task.
 * $TASK_ROOT/sandbox/run.sh - Configured run command, written to a shell script file.
 * $TASK_ROOT/sandbox/$PAYLOAD  - Payload specified in task configuration (fetched from HDFS).
 * $TASK_ROOT/sandbox/stderr  - Captures standard error stream.
 * $TASK_ROOT/sandbox/stdout - Captures standard output stream.
 *
 * @author wfarner
 */
public class RunningTask implements Task {
  private static final Logger LOG = Logger.getLogger(RunningTask.class.getName());

  private static final String PIDFILE_NAME = "pidfile";
  private static final String SANDBOX_DIR_NAME = "sandbox";
  private static final String RUN_SCRIPT_NAME = "run.sh";

  private static final Pattern PORT_REQUEST_PATTERN = Pattern.compile("%port:(\\w+)%");
  private static final String SHARD_ID_REGEXP = "%shard_id%";

  private static final String HEALTH_CHECK_PORT_NAME = "health";
  private static final Amount<Long, Time> LAUNCH_PIDFILE_GRACE_PERIOD = Amount.of(1L, Time.SECONDS);

  private final StateMachine<ScheduleStatus> stateMachine;

  private final ResourceConsumption resourceConsumption = new ResourceConsumption();

  private final SocketManager socketManager;
  private final ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker;
  private final ExceptionalClosure<KillCommand, KillException> processKiller;
  private final ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher;
  private KillCommand killCommand;

  private int pid = -1;

  private final AssignedTask task;

  private int healthCheckPort = -1;

  private final File taskRoot;
  @VisibleForTesting final File sandboxDir;

  @VisibleForTesting protected final Map<String, Integer> leasedPorts = Maps.newHashMap();

  private Process process;

  private int exitCode = 0;
  private final ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier;

  public RunningTask(SocketManager socketManager,
      ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker,
      ExceptionalClosure<KillCommand, KillException> processKiller,
      ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher,
      File taskRoot, AssignedTask task,
      ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier) {

    this.socketManager = Preconditions.checkNotNull(socketManager);
    this.healthChecker = Preconditions.checkNotNull(healthChecker);
    this.processKiller = Preconditions.checkNotNull(processKiller);
    this.pidFetcher = Preconditions.checkNotNull(pidFetcher);
    this.task = Preconditions.checkNotNull(task);

    this.taskRoot = Preconditions.checkNotNull(taskRoot);
    sandboxDir = new File(taskRoot, SANDBOX_DIR_NAME);
    this.fileCopier = Preconditions.checkNotNull(fileCopier);

    stateMachine = StateMachine.<ScheduleStatus>builder(toString())
          .initialState(ScheduleStatus.STARTING)
          .addState(ScheduleStatus.STARTING, ScheduleStatus.RUNNING, ScheduleStatus.FAILED)
          .addState(ScheduleStatus.RUNNING, ScheduleStatus.FINISHED,
                                            ScheduleStatus.FAILED,
                                            ScheduleStatus.KILLED,
                                            ScheduleStatus.LOST)
          .build();
  }

  /**
   * Performs staging operations necessary to launch a task.
   * This will prepare the working directory for the task, and download the binary to run.
   *
   * @throws TaskRunException If there was an error that caused staging to fail.
   */
  public void stage() throws TaskRunException {
    LOG.info(String.format("Staging task for job %s/%s", task.getTask().getOwner(),
        task.getTask().getJobName()));

    LOG.info("Building task directory hierarchy.");
    if (!sandboxDir.mkdirs()) {
      LOG.severe("Failed to create sandbox directory " + sandboxDir);
      throw new TaskRunException("Failed to create sandbox directory.");
    }

    // Store the task information in the sandbox.
    try {
      TaskUtils.storeTask(taskRoot, task);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to record task state.", e);
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to encode task state.", e);
    }

    LOG.info("Fetching payload.");
    File payload;
    LOG.info("File copier: " + fileCopier);
    try {
      payload = fileCopier.apply(
          new FileCopyRequest(task.getTask().getHdfsPath(), sandboxDir.getAbsolutePath()));
    } catch (IOException e) {
      throw new TaskRunException("Failed to fetch task binary.", e);
    }

    if (!payload.exists()) {
      throw new TaskRunException(String.format(
          "Unexpected state - payload does not exist: HDFS %s -> %s", task.getTask().getHdfsPath(),
          sandboxDir));
    }
  }

  @Override
  public int getId() {
    return task.getTaskId();
  }

  @Override
  public File getRootDir() {
    return taskRoot;
  }

  @Override
  public ScheduleStatus getScheduleStatus() {
    return stateMachine.getState();
  }

  @Override
  public TwitterTaskInfo getTaskInfo() {
    return task.getTask();
  }

  public File getSandboxDir() {
    return sandboxDir;
  }

  public Map<String, Integer> getLeasedPorts() {
    return ImmutableMap.copyOf(leasedPorts);
  }

  /**
   * Performs command-line expansion to assign managed port values where requested.
   *
   * @return A pair containing the expanded command line, and a map from port name to assigned
   *    port number.
   * @throws SocketManager.SocketLeaseException If there was a problem leasing a socket.
   * @throws TaskRunException If multiple ports with the same name were requested.
   */
  @VisibleForTesting
  protected Pair<String, Map<String, Integer>> expandCommandLine()
      throws SocketManager.SocketLeaseException, TaskRunException {
    String command = task.getTask().getStartCommand();

    LOG.info("Expanding command line " + command);
    return Pair.of(expandShardId(expandPortRequests(command)), leasedPorts);
  }

  private String expandPortRequests(String commandLine)
      throws TaskRunException, SocketManager.SocketLeaseException {
    Matcher m = PORT_REQUEST_PATTERN.matcher(commandLine);

    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String portName = m.group(1);
      if (portName.isEmpty()) throw new TaskRunException("Port name may not be empty.");

      int portNumber = leasedPorts.containsKey(portName) ? leasedPorts.get(portName)
          : socketManager.leaseSocket();

      leasedPorts.put(portName, portNumber);
      m.appendReplacement(sb, String.valueOf(portNumber));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  private String expandShardId(String commandLine) {
    return commandLine.replaceAll(SHARD_ID_REGEXP, String.valueOf(task.getShardId()));
  }

  @Override
  public void run() throws TaskRunException {
    LOG.info("Executing from working directory: " + sandboxDir);

    Pair<String, Map<String, Integer>> expansion;
    try {
      expansion = expandCommandLine();
    } catch (SocketManagerImpl.SocketLeaseException e) {
      LOG.info("Failed to get sockets!");
      throw new TaskRunException("Failed to obtain requested sockets.", e);
    }

    // Write the start command to a file.
    try {
      Files.write(expansion.getFirst(), new File(sandboxDir, RUN_SCRIPT_NAME), Charsets.US_ASCII);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to record run command.", e);
      throw new TaskRunException("Failed build staging directory.", e);
    }

    LOG.info("Obtained leases on ports: " + expansion.getSecond());
    leasedPorts.putAll(expansion.getSecond());

    if (leasedPorts.containsKey(HEALTH_CHECK_PORT_NAME)) {
      healthCheckPort = leasedPorts.get(HEALTH_CHECK_PORT_NAME);
    }

    List<String> commandLine = Arrays.asList(
        "bash", "-c",  // Read commands from the following string.
        String.format("echo $$ > ../%s; bash %s >stdout 2>stderr",
            PIDFILE_NAME, RUN_SCRIPT_NAME)
    );

    LOG.info("Executing shell command: " + commandLine);

    ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
    processBuilder.directory(sandboxDir);

    try {
      process = processBuilder.start();

      if (supportsHttpSignals()) {
        // TODO(wfarner): Change to use ScheduledExecutorService, making sure to shut it down.
        int healthCheckInterval = task.getTask().getHealthCheckIntervalSecs();
        new Timer(String.format("Task-%d-HealthCheck", task.getTaskId()), true).scheduleAtFixedRate(
            new TimerTask() {
              @Override public void run() {
                if (!isHealthy()) {
                  LOG.info("Task not healthy!");
                  terminate(ScheduleStatus.FAILED);
                }
              }
            },
            // Configure health check interval, allowing 2x configured time for startup.
            // TODO(wfarner): Add a configuration option for the task start-up grace period
            // before health checking begins.
            2 * Amount.of(healthCheckInterval, Time.SECONDS).as(Time.MILLISECONDS),
            Amount.of(healthCheckInterval, Time.SECONDS).as(Time.MILLISECONDS));
      }

      // TODO(wfarner): After a grace period, read the pidfile to get the parent PID and construct
      //    the KillCommand
      try {
        Thread.sleep(LAUNCH_PIDFILE_GRACE_PERIOD.as(Time.MILLISECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TaskRunException("Interrupted while waiting for launch grace period.", e);
      }
      buildKillCommand(supportsHttpSignals() ? leasedPorts.get(HEALTH_CHECK_PORT_NAME) : -1);

      stateMachine.transition(ScheduleStatus.RUNNING);
    } catch (IOException e) {
      stateMachine.transition(ScheduleStatus.FAILED);
      throw new TaskRunException("Failed to launch process.", e);
    }
  }

  private void buildKillCommand(int healthCheckPort) throws TaskRunException {
    try {
      pid = pidFetcher.apply(new File(taskRoot, PIDFILE_NAME));
    } catch (FileToInt.FetchException e) {
      LOG.log(Level.WARNING, "Failed to read pidfile for " + this, e);
      throw new TaskRunException("Failed to read pidfile.", e);
    }

    killCommand = new ProcessKiller.KillCommand(pid, healthCheckPort);
  }

  private boolean supportsHttpSignals() {
    return healthCheckPort != -1;
  }

  private static void cleanUp(Process p) {
    if (p == null) return;
    Closeables.closeQuietly(p.getInputStream());
    Closeables.closeQuietly(p.getOutputStream());
    Closeables.closeQuietly(p.getErrorStream());
    p.destroy();
  }

  /**
   * Waits for the launched task to terminate.
   *
   * @return The state that the task was in upon termination.
   */
  public ScheduleStatus waitFor() {
    Preconditions.checkNotNull(process);

    while (stateMachine.getState() == ScheduleStatus.RUNNING) {
      try {
        exitCode = process.waitFor();
        LOG.info("Process terminated with exit code: " + exitCode);

        if (stateMachine.getState() != ScheduleStatus.KILLED) {
          stateMachine.transition(exitCode == 0 ? ScheduleStatus.FINISHED : ScheduleStatus.FAILED);
          recordStatus();
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING,
            "Warning, Thread interrupted while waiting for process to finish.", e);
      } finally {
        cleanUp(process);
      }
    }

    // Return leased ports.
    for (int port : leasedPorts.values()) {
      socketManager.returnSocket(port);
    }
    leasedPorts.clear();

    return stateMachine.getState();
  }

  public int getTaskId() {
    return task.getTaskId();
  }

  /**
   * Gets the OS process ID.
   *
   * @return The process ID for this task, or -1 if the process ID is not yet known.
   */
  public int getProcessId() {
    return pid;
  }

  public boolean isRunning() {
    return stateMachine.getState() == ScheduleStatus.RUNNING;
  }

  public ScheduleStatus getStatus() {
    return stateMachine.getState();
  }

  public int getExitCode() {
    return exitCode;
  }

  private void recordStatus() {
    try {
      TaskUtils.saveTaskStatus(taskRoot, stateMachine.getState());
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Failed to store task status.", e);
    }
  }

  public void terminate(ScheduleStatus terminalState) {
    if (process == null) return;
    LOG.info("Terminating task " + this);
    stateMachine.transition(terminalState);
    recordStatus();

    try {
      processKiller.execute(killCommand);
    } catch (ProcessKiller.KillException e) {
      LOG.log(Level.WARNING, "Failed to kill process " + this, e);
    } finally {
      cleanUp(process);
    }

    waitFor();
  }

  public ResourceConsumption getResourceConsumption() {
    return resourceConsumption
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
    return String.format("%s/%s/%d", task.getTask().getOwner(), task.getTask().getJobName(),
        task.getTaskId());
  }
}
