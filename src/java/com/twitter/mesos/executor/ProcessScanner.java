package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.twitter.common.base.MorePreconditions;
import com.twitter.common.inject.TimedInterceptor.Timed;

/**
 * Process scanner to collect mesos task ids running on the machine.
 *
 * It will expect the output from given script in this format:
 * PID Mesos_TaskID, for example:
 * 20141 13196-abc.
 */
public class ProcessScanner {
  private static final Logger LOG = Logger.getLogger(ProcessScanner.class.getName());
  private static final int INVALID_PORT = -1;
  private final File processScraperScript;

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
      }
    }
    return builder.build();
  }

  /**
   * Gather all the valid mesos process information running on the machine.
   *
   * The reason of using pid as key is that a mesos task might have multiple processes, so
   * pid is the only unique identifier when handling the output.
   *
   * @return a map of pid to mesos task
   */
  public Set<ProcessInfo> getRunningProcesses() {
    LOG.fine("Executing script from " + processScraperScript);
    String output;
    try {
      output = runShellCommand(processScraperScript.getAbsolutePath());
    } catch (CommandFailedException e) {
      LOG.log(Level.SEVERE, "Failed to fetch running processes.", e);
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

  public static class ProcessInfo {
    private static final int NUM_COMPONENTS = 3;
    private final int pid;
    private final String taskID;
    private final List<Integer> listenPorts;

    private static final Predicate<Integer> REJECT_INVALID_PORT =
        Predicates.not(Predicates.equalTo(INVALID_PORT));

    private static final Function<String, Integer> STRING_TO_INT = new Function<String, Integer>() {
      @Override public Integer apply(String input) {
        return Integer.parseInt(input);
      }
    };

    private static Optional<ProcessInfo> parseLine(String line) {
      ImmutableList<String> components =
          ImmutableList.copyOf(Splitter.on(' ').omitEmptyStrings().split(line));
      if (components.size() != NUM_COMPONENTS) {
        return Optional.absent();
      }
      ImmutableList<String> portStrings =
          ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().split(components.get(2)));
      try {
        return Optional.of(new ProcessInfo(Integer.parseInt(components.get(0)), components.get(1),
            ImmutableList.copyOf(Iterables.filter(
                Iterables.transform(portStrings, STRING_TO_INT), REJECT_INVALID_PORT))));
      } catch (NumberFormatException e) {
        LOG.warning(String.format("Failed to parse process info line: %s.", line));
        return Optional.absent();
      }
    }

    @VisibleForTesting ProcessInfo(int pid, String taskID, List<Integer> listenPorts) {
      Preconditions.checkState(pid > 0);
      this.listenPorts = Preconditions.checkNotNull(listenPorts);
      this.taskID = MorePreconditions.checkNotBlank(taskID);
      this.pid = pid;
    }

    public int getPid() {
      return pid;
    }

    public String getTaskID() {
      return taskID;
    }

    public List<Integer> getListenPorts() {
      return listenPorts;
    }

    public boolean isSetListenPorts() {
      return !listenPorts.isEmpty();
    }

    @Override public int hashCode() {
      return new HashCodeBuilder().append(pid).append(taskID).append(listenPorts).toHashCode();
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
          .append(taskID, that.taskID)
          .append(listenPorts, that.listenPorts)
          .isEquals();
    }
  }
}

