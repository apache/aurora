package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.executor.HttpSignaler.SignalException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.gen.ScheduleStatus;

/**
 * Handles escalated killing of a process.
 *
 * @author William Farner
 */
public class ProcessKiller implements ExceptionalClosure<KillCommand, KillException> {
  private static final Logger LOG = Logger.getLogger(ProcessKiller.class.getName());

  private final ExceptionalFunction<String, List<String>, SignalException> httpSignaler;

  private static final String URL_FORMAT = "http://localhost:%d/%s";

  private static final String PROCESS_TERM_ENDPOINT = "quitquitquit";
  private static final String PROCESS_KILL_ENDPOINT = "abortabortabort";
  private final File killTreeScript;
  private final Amount<Long, Time> escalationDelay;

  public ProcessKiller(ExceptionalFunction<String, List<String>, SignalException> httpSignaler,
      File killTreeScript, Amount<Long, Time> escalationDelay) {
    Preconditions.checkNotNull(killTreeScript);
    Preconditions.checkArgument(killTreeScript.canRead());

    this.httpSignaler = Preconditions.checkNotNull(httpSignaler);
    this.killTreeScript = killTreeScript;
    this.escalationDelay = Preconditions.checkNotNull(escalationDelay);
  }

  @Override
  public void execute(KillCommand command) throws KillException {
    Preconditions.checkNotNull(command);

    // Start by requesting clean shutdown.
    if (command.httpSignalPort != -1) {
      signal(command.httpSignalPort, PROCESS_TERM_ENDPOINT);
      wait(escalationDelay);
      if (Tasks.isTerminated(command.task.getScheduleStatus())) {
        return;
      }
    }

    // Attempt to force shutdown by signaling.
    if (command.httpSignalPort != -1) {
      signal(command.httpSignalPort, PROCESS_KILL_ENDPOINT);
      wait(escalationDelay);
      if (Tasks.isTerminated(command.task.getScheduleStatus())) {
        return;
      }
    }

    killTree(command.pid);
  }

  private void signal(int port, String endpoint) {
    try {
      httpSignaler.apply(String.format(URL_FORMAT, port, endpoint));
    } catch (HttpSignaler.SignalException e) {
      LOG.log(Level.INFO, "HTTP signal failed: " + endpoint, e);
    }
  }

  private void wait(Amount<Long, Time> period) {
    try {
      Thread.sleep(period.as(Time.MILLISECONDS));
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "Interrupted while signaling.", e);
    }
  }

  private void killTree(int pid) throws KillException {
    LOG.info("Mercilessly killing process " + pid);

    ProcessBuilder builder = new ProcessBuilder("sh", killTreeScript.getAbsolutePath(),
        "-p", String.valueOf(pid), "-s", "9");
    LOG.info("Executing kill command " + builder.command());
    Process proc = null;
    try {
      proc = builder.start();
      String stderr = CharStreams.toString(new InputStreamReader(proc.getErrorStream()));
      String stdout = CharStreams.toString(new InputStreamReader(proc.getInputStream()));

      LOG.info("Killtree script gave stderr: " + stderr + " and stdout " + stdout);

      if (builder.start().waitFor() != 0) {
        LOG.severe("Killtree script failed!");
      }
    } catch (IOException e) {
      throw new KillException("Failed to kill tree on " + pid, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new KillException("Interrupted while waiting for kill tree on " + pid, e);
    } finally {
      if (proc != null) {
        // Java seems to leak these resources if you don't close them explicitly.
        Closeables.closeQuietly(proc.getOutputStream());
        Closeables.closeQuietly(proc.getInputStream());
        Closeables.closeQuietly(proc.getErrorStream());
        proc.destroy();
      }
    }
  }

  public static class KillCommand {
    private final int pid;
    private final Task task;
    private final int httpSignalPort;

    public KillCommand(int pid, Task task, int httpSignalPort) {
      this.pid = pid;
      this.task = task;
      this.httpSignalPort = httpSignalPort;
    }

    @Override
    public String toString() {
      return pid + "@" + httpSignalPort;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof KillCommand)) return false;

      KillCommand other = (KillCommand) o;

      return new EqualsBuilder()
          .append(pid, other.pid)
          .append(task, other.task)
          .append(httpSignalPort, other.httpSignalPort)
          .isEquals();
    }
  }

  public static class KillException extends Exception {
    public KillException(String msg) {
      super(msg);
    }

    public KillException(String msg, Throwable t) {
      super(msg, t);
    }
  }
}
