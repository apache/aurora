package com.twitter.mesos.scheduler;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.ExecutorStatus;

/**
 * Receives executor status messages, and may trigger a callback to suggest that an executor
 * should be restarted.
 *
 * @author William Farner
 */
public interface ExecutorTracker {

  public void start(final Closure<String> restartCallback);

  public void addStatus(ExecutorStatus status);
}
