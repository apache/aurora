package com.twitter.nexus.executor;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.nexus.executor.ProcessKiller.KillCommand;
import com.twitter.nexus.executor.ProcessKiller.KillException;

/**
 * Handles escalated killing of a process.
 *
 * @author wfarner
 */
public class ProcessKiller implements ExceptionalClosure<KillCommand, KillException> {
  private final HttpSignaler signaler;

  @Inject
  public ProcessKiller(HttpSignaler signaler) {
    this.signaler = Preconditions.checkNotNull(signaler);
  }

  @Override
  public void execute(KillCommand command) throws KillException {
    // TODO(wfarner): Implement.
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
