package com.twitter.mesos.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.collections.Pair;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.twitter.mesos.gen.ScheduleStatus.*;

/**
 * Handles storing information and performing duties related to a running task.
 *
 * This will create the task $TASK_ROOT, as well as a sandbox directory that the launch command
 * has access to:
 *
 * $TASK_ROOT - root task directory.
 * $TASK_ROOT/pidfile  - PID of the launched process.
 * $TASK_ROOT/sandbox/  - Sandbox, working directory for the task.
 * $TASK_ROOT/sandbox/run.sh - Configured run command, written to a shell script file.
 * $TASK_ROOT/sandbox/$PAYLOAD  - Payload specified in task configuration (fetched from HDFS).
 * $TASK_ROOT/sandbox/stderr  - Captured standard error stream.
 * $TASK_ROOT/sandbox/stdout - Captured standard output stream.
 *
 * @author wfarner
 */
public class LiveTask extends TaskOnDisk {
  private static final Logger LOG = Logger.getLogger(LiveTask.class.getName());

  @VisibleForTesting static final String PIDFILE_NAME = "pidfile";
  @VisibleForTesting static final String SANDBOX_DIR_NAME = "sandbox";
  @VisibleForTesting static final String RUN_SCRIPT_NAME = "run.sh";
  @VisibleForTesting static final String STDERR_CAPTURE_FILE = "stderr";
  @VisibleForTesting static final String STDOUT_CAPTURE_FILE = "stdout";

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

  private ExecutorService healthCheckExecutor;

  private final AssignedTask task;

  private int healthCheckPort = -1;

  @VisibleForTesting final File sandboxDir;

  @VisibleForTesting protected final Map<String, Integer> leasedPorts = Maps.newHashMap();

  private Process process;

  private int exitCode = 0;
  private final ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier;

  public LiveTask(SocketManager socketManager,
      ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker,
      ExceptionalClosure<KillCommand, KillException> processKiller,
      ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher,
      File taskRoot, AssignedTask task,
      ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier) {
    super(taskRoot);

    this.socketManager = Preconditions.checkNotNull(socketManager);
    this.healthChecker = Preconditions.checkNotNull(healthChecker);
    this.processKiller = Preconditions.checkNotNull(processKiller);
    this.pidFetcher = Preconditions.checkNotNull(pidFetcher);
    this.task = Preconditions.checkNotNull(task);

    sandboxDir = new File(taskRoot, SANDBOX_DIR_NAME);
    this.fileCopier = Preconditions.checkNotNull(fileCopier);

    stateMachine = StateMachine.<ScheduleStatus>builder(toString())
          .initialState(STARTING)
          .addState(STARTING, RUNNING, FAILED)
          .addState(RUNNING, FINISHED, FAILED, KILLED, LOST)
          .build();
  }

  /**
   * Performs staging operations necessary to launch a task.
   * This will prepare the working directory for the task, and download the binary to run.
   *
   * @throws TaskRunException If there was an error that caused staging to fail.
   */
  @Override
  public void stage() throws TaskRunException {
    LOG.info(String.format("Staging task for job %s", Tasks.jobKey(task)));

    LOG.info("Building task directory hierarchy.");
    if (!sandboxDir.mkdirs()) {
      LOG.severe("Failed to create sandbox directory " + sandboxDir);
      throw new TaskRunException("Failed to create sandbox directory.");
    }

    // Store the task information in the sandbox.
    try {
      recordTask();
    } catch (TaskStorageException e) {
      throw new TaskRunException("Failed to store task.", e);
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
  public String getId() {
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
  public AssignedTask getAssignedTask() {
    return task.deepCopy();
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
    return commandLine.replaceAll(SHARD_ID_REGEXP, String.valueOf(task.getTask().getShardId()));
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
        String.format("echo $$ > ../%s; bash %s", PIDFILE_NAME, RUN_SCRIPT_NAME)
    );

    LOG.info("Executing shell command: " + commandLine);

    ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
    processBuilder.directory(sandboxDir);

    try {
      process = processBuilder.start();

      captureProcessOutput(process);

      if (supportsHttpSignals()) {
        ThreadFactory factory = new ThreadFactoryBuilder()
            .setNameFormat(String.format("Task-%s-HealthCheck", task.getTaskId()))
            .setDaemon(true)
            .build();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, factory);
        healthCheckExecutor = executor;

        executor.scheduleAtFixedRate(
            new Runnable() {
              @Override public void run() {
                if (!isHealthy()) {
                  LOG.info("Task not healthy!");
                  terminate(FAILED);
                }
              }
            },
            // Configure health check interval, allowing 2x configured time for startup.
            // TODO(wfarner): Add a configuration option for the task start-up grace period
            // before health checking begins.
            2 * task.getTask().getHealthCheckIntervalSecs(),
            task.getTask().getHealthCheckIntervalSecs(),
            TimeUnit.SECONDS
        );
      }

      try {
        Thread.sleep(LAUNCH_PIDFILE_GRACE_PERIOD.as(Time.MILLISECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TaskRunException("Interrupted while waiting for launch grace period.", e);
      }
      killCommand = buildKillCommand(
          supportsHttpSignals() ? leasedPorts.get(HEALTH_CHECK_PORT_NAME) : -1);

      setStatus(RUNNING);
    } catch (IOException e) {
      terminate(FAILED);
      throw new TaskRunException("Failed to launch process.", e);
    }
  }

  private void captureProcessOutput(final Process process) {
      ThreadFactory captureFactory = new ThreadFactoryBuilder()
          .setNameFormat(Tasks.jobKey(task) + "-OutputCapture-%d").setDaemon(true).build();
      captureFactory.newThread(new Runnable() {
        @Override public void run() {
          File stdoutFile = new File(sandboxDir, STDOUT_CAPTURE_FILE);
          LOG.info("Capturing stdout to " + stdoutFile);
          captureStream(process.getInputStream(), stdoutFile);
        }
      }).start();
      captureFactory.newThread(new Runnable() {
        @Override public void run() {
          File stderrFile = new File(sandboxDir, STDERR_CAPTURE_FILE);
          LOG.info("Capturing stderr to " + stderrFile);
          captureStream(process.getErrorStream(), stderrFile);
        }
      }).start();
  }

  private void captureStream(InputStream stream, File outputFile) {
    FileOutputStream outputStream = null;
    try {
      outputStream = new FileOutputStream(outputFile);
      ByteStreams.copy(stream, outputStream);
      LOG.info("Stream capture to " + outputFile + " completed.");
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to capture stream.", e);
    } finally {
      Closeables.closeQuietly(outputStream);
    }
  }

  private KillCommand buildKillCommand(int healthCheckPort) throws TaskRunException {
    try {
      return new KillCommand(pidFetcher.apply(new File(taskRoot, PIDFILE_NAME)), healthCheckPort);
    } catch (FileToInt.FetchException e) {
      LOG.log(Level.WARNING, "Failed to read pidfile for " + this, e);
      throw new TaskRunException("Failed to read pidfile.", e);
    }
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
  @Override
  public ScheduleStatus blockUntilTerminated() {
    Preconditions.checkNotNull(process);

    while (stateMachine.getState() == RUNNING) {
      try {
        exitCode = process.waitFor();
        LOG.info("Process terminated with exit code: " + exitCode);

        if (stateMachine.getState() != KILLED && stateMachine.getState() != FAILED) {
          setStatus(exitCode == 0 ? FINISHED : FAILED);
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

  public String getTaskId() {
    return task.getTaskId();
  }

  public boolean isRunning() {
    return stateMachine.getState() == RUNNING;
  }

  public ScheduleStatus getStatus() {
    return stateMachine.getState();
  }

  public int getExitCode() {
    return exitCode;
  }

  private void setStatus(ScheduleStatus status) {
    stateMachine.transition(status);
    try {
      recordStatus(stateMachine.getState());
    } catch (TaskStorageException e) {
      LOG.log(Level.WARNING, "Failed to store task status.", e);
    }
  }

  public void terminate(ScheduleStatus terminalState) {
    LOG.info("Terminating " + this + " with status " + terminalState);

    if (healthCheckExecutor != null) {
      try {
        if (!healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          healthCheckExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupted while shutting down health check executor.");
        Thread.currentThread().interrupt();
      }
    }

    if (process == null) return;
    ScheduleStatus currentStatus = stateMachine.getState();
    if (Tasks.isTerminated(currentStatus)) {
      LOG.info("Task " + this + " is already terminated, not changing state to " + terminalState);
      return;
    }

    LOG.info("Terminating task " + this);
    setStatus(terminalState);

    if (killCommand != null) {
      try {
        processKiller.execute(killCommand);
      } catch (ProcessKiller.KillException e) {
        LOG.log(Level.WARNING, "Failed to kill process " + this, e);
      } finally {
        cleanUp(process);
      }
    }

    blockUntilTerminated();
  }

  @Override
  public ResourceConsumption getResourceConsumption() {
    return resourceConsumption.setLeasedPorts(ImmutableMap.copyOf(leasedPorts));
  }

  private boolean isHealthy() {
    LOG.info("Checking task health.");
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
    return String.format("%s/%s", Tasks.jobKey(task), task.getTaskId());
  }
}
