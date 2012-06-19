package com.twitter.mesos.executor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;

import static com.twitter.mesos.executor.FileCopier.FileCopyException;

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
 */
public class LiveTask extends TaskOnDisk {
  private static final Logger LOG = Logger.getLogger(LiveTask.class.getName());

  private static final AuditedStatus FAILED = new AuditedStatus(ScheduleStatus.FAILED);
  private static final AuditedStatus FINISHED = new AuditedStatus(ScheduleStatus.FINISHED);
  private static final AuditedStatus KILLED = new AuditedStatus(ScheduleStatus.KILLED);
  private static final AuditedStatus LOST = new AuditedStatus(ScheduleStatus.LOST);
  private static final AuditedStatus RUNNING = new AuditedStatus(ScheduleStatus.RUNNING);
  private static final AuditedStatus STARTING = new AuditedStatus(ScheduleStatus.STARTING);

  @VisibleForTesting static final String PIDFILE_NAME = "pidfile";
  @VisibleForTesting static final String RUN_SCRIPT_NAME = "run.sh";
  @VisibleForTesting static final String STDERR_CAPTURE_FILE = "stderr";
  @VisibleForTesting static final String STDOUT_CAPTURE_FILE = "stdout";
  @VisibleForTesting static final String HEALTH_CHECK_PORT_NAME = "health";

  private static final Amount<Long, Time> LAUNCH_PIDFILE_GRACE_PERIOD = Amount.of(1L, Time.SECONDS);

  private final StateMachine<AuditedStatus> stateMachine;

  private final ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker;
  private final ExceptionalClosure<KillCommand, KillException> processKiller;
  private final ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher;
  private KillCommand killCommand;

  private ExecutorService healthCheckExecutor;

  private final AssignedTask task;

  private volatile int healthCheckPort = -1;

  private Process process;

  private volatile int exitCode = 0;
  private volatile boolean completed = false;
  private final FileCopier fileCopier;
  private final boolean multiUser;

  public LiveTask(
      ExceptionalFunction<Integer, Boolean, HealthCheckException> healthChecker,
      ExceptionalClosure<KillCommand, KillException> processKiller,
      ExceptionalFunction<File, Integer, FileToInt.FetchException> pidFetcher,
      File taskRoot, AssignedTask task,
      FileCopier fileCopier,
      boolean multiUser) {
    super(taskRoot);

    this.healthChecker = Preconditions.checkNotNull(healthChecker);
    this.processKiller = Preconditions.checkNotNull(processKiller);
    this.pidFetcher = Preconditions.checkNotNull(pidFetcher);
    this.task = Preconditions.checkNotNull(task);

    this.fileCopier = Preconditions.checkNotNull(fileCopier);
    this.multiUser = multiUser;

    stateMachine = StateMachine.<AuditedStatus>builder(toString())
          .initialState(STARTING)
          .addState(STARTING, RUNNING, FAILED, KILLED)
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
  public synchronized void stage() throws TaskRunException {
    LOG.info(String.format("Staging task %s", task.getTaskId()));

    LOG.info("Building task directory hierarchy.");
    if (!sandboxDir.mkdirs()) {
      LOG.severe("Failed to create sandbox directory " + sandboxDir);
      throw new TaskRunException("Failed to create sandbox directory.");
    }

    // Store the task information in the sandbox.
    try {
      recordTask();
    } catch (TaskStorageException e) {
      throw new TaskRunException("Failed to store task on disk.", e);
    }

    if (!StringUtils.isBlank(task.getTask().getHdfsPath())) {
      LOG.info("Fetching payload.");
      File payload;
      LOG.info("File copier: " + fileCopier);
      try {
        payload = fileCopier.apply(
            new FileCopyRequest(task.getTask().getHdfsPath(), sandboxDir.getAbsolutePath()));
      } catch (FileCopyException e) {
        throw new TaskRunException("Failed to fetch task binary from "
            + task.getTask().getHdfsPath(), e);
      }

      if (!payload.exists()) {
        throw new TaskRunException(String.format(
            "Unexpected state - payload does not exist: HDFS %s -> %s", task.getTask().getHdfsPath(),
            sandboxDir));
      }
    } else {
      LOG.info("No payload specified in " + task.getTaskId());
    }
  }

  @Override
  public String getId() {
    return task.getTaskId();
  }

  @Override
  public AuditedStatus getAuditedStatus() {
    return stateMachine.getState();
  }

  @Override
  public AssignedTask getAssignedTask() {
    return task.deepCopy();
  }

  @VisibleForTesting static final String HEALTH_CHECK_FAIL_MSG = "HTTP health check failure.";

  @Override
  public void run() throws TaskRunException {
    LOG.info("Executing from working directory: " + sandboxDir);
    stateMachine.checkState(STARTING);

    // Write the start command to a file.
    try {
      Files.write(task.getTask().getStartCommand(), new File(sandboxDir, RUN_SCRIPT_NAME),
          Charsets.US_ASCII);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to record run command.", e);
      throw new TaskRunException("Failed build staging directory.", e);
    }

    if ((task.getAssignedPortsSize() > 0)
        && task.getAssignedPorts().containsKey(HEALTH_CHECK_PORT_NAME)) {
      healthCheckPort = task.getAssignedPorts().get(HEALTH_CHECK_PORT_NAME);
    }

    List<String> commands = Lists.newArrayList();

    // Create the pidfile.
    commands.add("echo $$ > ../" + PIDFILE_NAME);

    if (multiUser) {
      String role = task.getTask().getOwner().getRole();
      LOG.info("Launching as role " + role);
      // chown the sandbox.
      commands.add(String.format("chown -R %s .", role));
      // Run as the role account.
      commands.add(String.format("su %s -c \"bash %s\"", role, RUN_SCRIPT_NAME));
    } else {
      // Execute the launch script containing the user shell code.
      commands.add("bash " + RUN_SCRIPT_NAME);
    }

    List<String> commandLine = Arrays.asList(
        "bash", "-c",  // Read commands from the following string.
        Joiner.on("; ").join(commands)
    );

    LOG.info("Executing shell command: " + commandLine);

    ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
    processBuilder.directory(sandboxDir);

    try {
      process = processBuilder.start();
    } catch (IOException e) {
      terminate(new AuditedStatus(ScheduleStatus.FAILED, "Failed to launch process."));
      throw new TaskRunException("Failed to launch process.", e);
    }

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
                terminate(new AuditedStatus(ScheduleStatus.FAILED, HEALTH_CHECK_FAIL_MSG));
              }
            }
          },
          // Configure health check interval, allowing 2x configured time for startup.
          // TODO(William Farner): Add a configuration option for the task start-up grace period
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
    killCommand = buildKillCommand(supportsHttpSignals() ? healthCheckPort : -1);

    setStatus(RUNNING);
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

  private static void captureStream(InputStream stream, File outputFile) {
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
      return new KillCommand(pidFetcher.apply(new File(taskRoot, PIDFILE_NAME)),
          this, healthCheckPort);
    } catch (FileToInt.FetchException e) {
      LOG.log(Level.WARNING, "Failed to read pidfile for " + this, e);
      throw new TaskRunException("Failed to read pidfile.", e);
    }
  }

  private boolean supportsHttpSignals() {
    return healthCheckPort != -1;
  }

  private static void cleanUp(Process p) {
    if (p == null) {
      return;
    }
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
  public AuditedStatus blockUntilTerminated() {
    Preconditions.checkNotNull(process);

    while (stateMachine.getState().equals(RUNNING)) {
      try {
        exitCode = process.waitFor();
        LOG.info("Process terminated with exit code: " + exitCode);

        if (!stateMachine.getState().equals(KILLED) && !stateMachine.getState().equals(FAILED)) {
          setStatus(exitCode == 0 ? FINISHED : FAILED);
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING,
            "Warning, Thread interrupted while waiting for process to finish.", e);
      } finally {
        completed = true;
        cleanUp(process);
      }
    }

    return stateMachine.getState();
  }

  public String getTaskId() {
    return task.getTaskId();
  }

  @Override
  public boolean isRunning() {
    return Tasks.isActive(stateMachine.getState().getStatus());
  }

  @Override
  public boolean isCompleted() {
    return completed;
  }

  public AuditedStatus getStatus() {
    return stateMachine.getState();
  }

  public int getExitCode() {
    return exitCode;
  }

  private void setStatus(AuditedStatus status) {
    stateMachine.transition(status);
    try {
      recordStatus(stateMachine.getState().getStatus());
    } catch (TaskStorageException e) {
      LOG.log(Level.WARNING, "Failed to store task status.", e);
    }
  }

  @Override
  public synchronized void terminate(AuditedStatus terminalState) {
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

    AuditedStatus currentStatus = stateMachine.getState();
    if (Tasks.isTerminated(currentStatus.getStatus())) {
      LOG.info("Task " + this + " is already terminated, not changing state to " + terminalState);
      return;
    }

    LOG.info("Terminating task " + this);
    setStatus(terminalState);

    if (process == null) {
      return;
    }

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

  private boolean isHealthy() {
    LOG.info("Checking task health.");
    if (!supportsHttpSignals()) {
      return true;
    }
    try {
      return healthChecker.apply(healthCheckPort);
    } catch (HealthCheckException e) {
      LOG.log(Level.INFO, String.format("Health check for %s on port %d failed.",
          this, healthCheckPort), e);
      return false;
    }
  }

  @Override
  public String toString() {
    return String.format("%s/%s", Tasks.jobKey(task), task.getTaskId());
  }
}
