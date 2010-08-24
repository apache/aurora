package com.twitter.nexus.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.Pair;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.StateMachine;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.TaskState;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  private static final Amount<Long, Time> URL_TIMEOUT = Amount.of(1L, Time.SECONDS);
  private static final String PROCESS_TERM_ENDPOINT = "quitquitquit";
  private static final String PROCESS_KILL_ENDPOINT = "abortabortabort";

  private static final String HEALTH_CHECK_ENDPOINT = "healthz";
  private static final String HEALTH_CHECK_OK_VALUE = "ok";

  private final StateMachine<TaskState> stateMachine;

  private final SocketManager socketManager;
  private final int taskId;

  private final TwitterTaskInfo task;

  private String signalRootUrl = null;
  private ExecutorService httpSignalExecutor = null;

  @VisibleForTesting
  final File taskRoot;

  @VisibleForTesting
  protected final Map<String, Integer> leasedPorts = Maps.newHashMap();
  private Process process;

  private int exitCode = 0;
  private final ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier;

  public RunningTask(SocketManager socketManager, File executorRoot, int taskId,
      TwitterTaskInfo task,
      ExceptionalFunction<FileCopyRequest, File, IOException> fileCopier) {
    Preconditions.checkNotNull(socketManager);
    Preconditions.checkNotNull(executorRoot);
    Preconditions.checkState(executorRoot.exists() && executorRoot.isDirectory());
    Preconditions.checkNotNull(task);
    Preconditions.checkNotNull(fileCopier);

    this.socketManager = socketManager;
    this.taskId = taskId;
    this.task = task;
    taskRoot = new File(executorRoot, String.format(
        "%s/%s/%d-%d", task.getOwner(), task.getJobName(), taskId, System.nanoTime()));
    this.fileCopier = fileCopier;

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
      throws SocketManager.SocketLeaseException, ProcessException {
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
    } catch (SocketManager.SocketLeaseException e) {
      LOG.info("Failed to get sockets!");
      throw new ProcessException("Failed to obtain requested sockets.", e);
    }

    String startCommand = expansion.getFirst();

    LOG.info("Obtained leases on ports: " + expansion.getSecond());
    leasedPorts.putAll(expansion.getSecond());

    if (leasedPorts.containsKey(HEALTH_CHECK_PORT_NAME)) {
      signalRootUrl = "http://localhost:" + leasedPorts.get(HEALTH_CHECK_PORT_NAME);
      httpSignalExecutor = Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat(toString() + "-%d").build());
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

      stateMachine.transition(TaskState.TASK_RUNNING);
    } catch (IOException e) {
      stateMachine.transition(TaskState.TASK_FAILED);
      throw new ProcessException("Failed to launch process.", e);
    }
  }

  private boolean supportsHttpSignals() {
    return signalRootUrl != null;
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
      socketManager.returnPort(port);
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
    // TODO(wfarner): Fix this - the state transition makes waitFor() a no-op.
    stateMachine.transition(terminalState);

    signalTaskKill();

    process.destroy();
    waitFor();
  }

  private boolean isHealthy() {
    if (!supportsHttpSignals()) return true;
    List<String> response = touchUrl(HEALTH_CHECK_ENDPOINT);
    return response != null && Joiner.on("").join(response).equalsIgnoreCase(HEALTH_CHECK_OK_VALUE);
  }

  private void signalTaskKill() {
    if (supportsHttpSignals()) {
      touchUrl(PROCESS_TERM_ENDPOINT);

      try {
        // TODO(wfarner): Make this configurable.
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        LOG.warning("Interrupted while waiting beween TERM and KILL.");
      }

      touchUrl(PROCESS_KILL_ENDPOINT);
    }
  }

  private List<String> touchUrl(final String endpoint) {
    Preconditions.checkState(supportsHttpSignals());
    LOG.info("Touching signal endpoint: " + endpoint);

    final URL signalUrl;
    try {
      signalUrl = new URL(signalRootUrl + "/" + endpoint);
    } catch (MalformedURLException e) {
      LOG.log(Level.SEVERE, "Malformed signal url.", e);
      return null;
    }

    LOG.info("Signaling URL: " + signalUrl);
    Future<List<String>> task = httpSignalExecutor.submit(new Callable<List<String>>() {
      @Override public List<String> call() throws Exception {
        return Resources.readLines(signalUrl, Charsets.UTF_8);
      }
    });

    try {
      List<String> reply = task.get(URL_TIMEOUT.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS);
      LOG.info(String.format("Signal to %s replied with %s", endpoint, reply));
      return reply;
    } catch (InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted while requesting signal URL.", e);
      task.cancel(true);
    } catch (ExecutionException e) {
      LOG.log(Level.WARNING, "Failed while requesting signal URL.", e);
    } catch (TimeoutException e) {
      LOG.info("Signal URL request timed out.");
    }

    return null;
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
