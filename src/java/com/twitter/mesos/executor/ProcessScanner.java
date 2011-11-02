package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;

/**
 * Process scanner to collect mesos task ids running on the machine.
 *
 * It will expect the output from given script in this format:
 * PID Mesos_TaskID, for example:
 * 20141 13196-abc.
 */
public class ProcessScanner {
  private static final Logger LOG = Logger.getLogger(ProcessScanner.class.getName());
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
  static Map<String, Integer> parseOutput(String output) {
    ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
    Iterable<String> lines = Splitter.on('\n').omitEmptyStrings().split(output);
    for (String line : lines) {
      ImmutableList<String> components =
          ImmutableList.copyOf(Splitter.on(' ').omitEmptyStrings().split(line));
      if (components.size() != 2) {
        LOG.warning(String.format("Failed to parse script line: %s.", line));
        continue;
      }
      int pid;
      try {
        pid = Integer.parseInt(components.get(0));
      } catch (NumberFormatException e) {
        LOG.warning(String.format("Failed to parse script line: %s.", line));
        continue;
      }
      builder.put(components.get(1), pid);
    }
    return builder.build();
  }

  /**
   * Gather all the valid mesos process information running on the machine.
   *
   * @return a map of mesos task id to pid
   */
  public Map<String, Integer> getRunningProcesses() {
    LOG.fine("Executing script from " + processScraperScript);
    String output;
    try {
      output = runShellCommand(processScraperScript.getAbsolutePath());
    } catch (CommandFailedException e) {
      LOG.log(Level.SEVERE, "Failed to fetch running processes.", e);
      return ImmutableMap.of();
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
}

