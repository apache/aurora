package com.twitter.mesos.executor;

import java.io.File;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;

/**
 * A task stored in the executor.
 */
public interface Task {

  public String getId();

  public void stage() throws TaskRunException;

  public void run() throws TaskRunException;

  public AuditedStatus blockUntilTerminated();

  public boolean isRunning();

  /**
   * Whether underlying process is terminated after completion.
   *
   * @return {@code true} iff the underlying process is completed
   */
  public boolean isCompleted();

  public void terminate(AuditedStatus terminalState);

  public File getSandboxDir();

  public AssignedTask getAssignedTask();

  public AuditedStatus getAuditedStatus();

  public static class TaskRunException extends Exception {

    // TODO(William Farner): Attach a GUID here (probably via UUID) to allow for automatic stack
    //    trace linking between disparate parts of the system.

    public TaskRunException(String msg, Throwable t) {
      super(msg, t);
    }

    public TaskRunException(String msg) {
      super(msg);
    }
  }

  /**
   * A status that carries along an optional audit message.
   */
  public static class AuditedStatus {
    private final ScheduleStatus status;
    private final Optional<String> message;

    private AuditedStatus(ScheduleStatus status, Optional<String> message) {
      this.status = Preconditions.checkNotNull(status);
      this.message = Preconditions.checkNotNull(message);
    }

    public AuditedStatus(ScheduleStatus status) {
      this(status, Optional.<String>absent());
    }

    public AuditedStatus(ScheduleStatus status, String message) {
      this(status, Optional.of(message));
    }

    public ScheduleStatus getStatus() {
      return status;
    }

    public Optional<String> getMessage() {
      return message;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof AuditedStatus)) {
        return false;
      }

      AuditedStatus other = (AuditedStatus) o;
      return status == other.status;
    }

    @Override
    public int hashCode() {
      return status.hashCode();
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("status", status)
          .add("message", message)
          .toString();
    }
  }
}
