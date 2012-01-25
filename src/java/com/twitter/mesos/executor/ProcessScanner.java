package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.twitter.common.base.MorePreconditions;
import com.twitter.common.stats.Stats;

/**
 * A function that invokes an external script to gather information about active processes.
 */
public class ProcessScanner {
  private static final Logger LOG = Logger.getLogger(ProcessScanner.class.getName());

  private static final AtomicLong sciptRunFailures =
      Stats.exportLong("process_scraper_run_failures");

  private final File processScraperScript;

  /**
   * Creates a process scanner that will invoke and parse data from the provided scraper script.
   *
   * When invoked, the script should return information about running mesos tasks in the following
   * format:
   *
   * <pre>
   * pid1 task_id1 port1,port2
   * pid2 task_id2
   * </pre>
   *
   * Where pids are the OS process IDs, task_ids are mesos task IDs, and ports are IP ports that
   * the process is currently listening on.
   *
   * @param processScraperScript Process scraping script file.
   */
  ProcessScanner(File processScraperScript) {
    this.processScraperScript = processScraperScript;
  }

  private static String runShellCommand(String commandline) throws CommandFailedException {
    Process process = null;
    String stdOutput;
    String stderrOutput;
    try {
      process = new ProcessBuilder(commandline).start();
      process.waitFor();
      stdOutput = CharStreams.toString(new InputStreamReader(process.getInputStream()));
      stderrOutput = CharStreams.toString(new InputStreamReader(process.getErrorStream()));
    } catch (IOException e) {
      throw new CommandFailedException("Failed to launch process " + commandline, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CommandFailedException("Failed to launch process " + commandline, e);
    } finally {
      if (process != null) {
        // Java seems to leak these resources if you don't close them explicitly.
        Closeables.closeQuietly(process.getOutputStream());
        Closeables.closeQuietly(process.getInputStream());
        Closeables.closeQuietly(process.getErrorStream());
        process.destroy();
      }
    }

    if (process.exitValue() != 0) {
      throw new CommandFailedException("Failed to launch process " + commandline);
    }
    if (stderrOutput.length() > 0) {
      LOG.warning(String.format(
          "Standard error output for running %s: %s", commandline, stderrOutput));
    }

    return stdOutput;
  }

  @VisibleForTesting
  static Set<ProcessInfo> parseOutput(String output) {
    ImmutableSet.Builder<ProcessInfo> builder = ImmutableSet.builder();
    Iterable<String> lines = Splitter.on('\n').omitEmptyStrings().split(output);
    for (String line : lines) {
      Optional<ProcessInfo> processInfo = ProcessInfo.parseLine(line);
      if (processInfo.isPresent()) {
        builder.add(processInfo.get());
      } else {
        LOG.warning("Failed to parse line " + line);
      }
    }
    return builder.build();
  }

  /**
   * Gathers information about all running mesos task processes.
   *
   * @return information about all processes discovered during the scan.
   */
  public Set<ProcessInfo> getRunningProcesses() {
    LOG.fine("Executing script from " + processScraperScript);
    String output;
    try {
      output = runShellCommand(processScraperScript.getAbsolutePath());
    } catch (CommandFailedException e) {
      LOG.log(Level.SEVERE, "Failed to fetch running processes.", e);
      sciptRunFailures.incrementAndGet();
      return ImmutableSet.of();
    }

    return parseOutput(output);
  }

  private static class CommandFailedException extends Exception {
    CommandFailedException(String s) {
      super(s);
    }

    CommandFailedException(String s, Exception e) {
      super(s, e);
    }
  }

  /**
   * Information about a single running process.
   */
  static class ProcessInfo {
    private static final Splitter SPLIT_SPACE = Splitter.on(" ").omitEmptyStrings();
    private static final Splitter SPLIT_COMMA = Splitter.on(",").omitEmptyStrings();

    private final int pid;
    private final String taskId;
    private final Set<Integer> listenPorts;

    private static final Function<String, Integer> A_TO_I = new Function<String, Integer>() {
      @Override public Integer apply(String input) {
        return Integer.parseInt(input);
      }
    };

    private static Optional<ProcessInfo> parseLine(String line) {
      ImmutableList<String> parts = ImmutableList.copyOf(SPLIT_SPACE.split(line));
      if (parts.size() != 2 && parts.size() != 3) {
        return Optional.absent();
      }

      try {
        Set<Integer> listenPorts = (parts.size() == 2)
            ? ImmutableSet.<Integer>of()
            : ImmutableSet.copyOf(Iterables.transform(SPLIT_COMMA.split(parts.get(2)), A_TO_I));

        return Optional.of(
            new ProcessInfo(Integer.parseInt(parts.get(0)), parts.get(1), listenPorts));
      } catch (NumberFormatException e) {
        LOG.warning(String.format("Failed to parse process info line: %s.", line));
        return Optional.absent();
      }
    }

    @VisibleForTesting ProcessInfo(int pid, String taskId, Set<Integer> listenPorts) {
      Preconditions.checkArgument(pid > 0, "pid must be greater than zero: " + pid);
      this.listenPorts = Preconditions.checkNotNull(listenPorts);
      this.taskId = MorePreconditions.checkNotBlank(taskId);
      this.pid = pid;
    }

    /**
     * Gets the ID of the process.
     *
     * @return The OS process ID (pid).
     */
    public int getPid() {
      return pid;
    }

    /**
     * Gets the task ID of the mesos task that spawned the process.
     *
     * @return Mesos task ID.
     */
    public String getTaskId() {
      return taskId;
    }

    /**
     * Gets any ports that the process is listening on.
     *
     * @return A possibly-empty list of ports that the process is listening on.
     */
    public Set<Integer> getListenPorts() {
      return listenPorts;
    }

    @Override public String toString() {
      return "pid=" + pid + ", task=" + taskId + ", ports=" + listenPorts;
    }

    @Override public int hashCode() {
      return new HashCodeBuilder().append(pid).append(taskId).append(listenPorts).toHashCode();
    }

    @Override public boolean equals(Object other) {
      if (other == this) {
        return true;
      }

      if (!(other instanceof ProcessInfo)) {
        return false;
      }

      ProcessInfo that = (ProcessInfo) other;
      return new EqualsBuilder()
          .append(pid, that.pid)
          .append(taskId, that.taskId)
          .append(listenPorts, that.listenPorts)
          .isEquals();
    }
  }
}

