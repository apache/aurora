package com.twitter.nexus.executor;

import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.nexus.executor.HttpSignaler.SignalException;
import com.twitter.nexus.executor.ProcessKiller.KillCommand;
import com.twitter.nexus.executor.ProcessKiller.KillException;
import org.apache.commons.lang.builder.EqualsBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles escalated killing of a process.
 *
 * @author wfarner
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
    }

    // Attempt to force shutdown by signaling.
    if (command.httpSignalPort != -1) {
      signal(command.httpSignalPort, PROCESS_KILL_ENDPOINT);
      wait(escalationDelay);
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
        "-p", String.valueOf(pid));
    try {
      Process proc = builder.start();

      if (builder.start().waitFor() != 0) {
        String stderr = CharStreams.toString(new InputStreamReader(proc.getErrorStream()));
        String stdout = CharStreams.toString(new InputStreamReader(proc.getInputStream()));

        LOG.info("Killtree script failed, stderr:\n" + stderr + "\nstdout:\n" + stdout);
      }
    } catch (IOException e) {
      throw new KillException("Failed to kill tree on " + pid, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new KillException("Interrupted while waiting for kill tree on " + pid, e);
    }
  }

  public static class KillCommand {
    private final int pid;
    private final int httpSignalPort;

    public KillCommand(int pid) {
      this(pid, -1);
    }

    public KillCommand(int pid, int httpSignalPort) {
      this.pid = pid;
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
