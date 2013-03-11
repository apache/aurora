package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

/**
 * A listener that will be notified when the framework has been registered.
 * TODO(William Farner): Use pubsub notifications in place of this.
 */
interface RegisteredListener {

  void registered(String frameworkId) throws RegisterHandlingException;

  class RegisterHandlingException extends Exception {
    RegisterHandlingException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * A listener that will notify a group of listeners.
   */
  class FanoutRegisteredListener implements RegisteredListener {

    private static final Logger LOG = Logger.getLogger(FanoutRegisteredListener.class.getName());

    private final Set<RegisteredListener> listeners;

    @Inject
    FanoutRegisteredListener(Set<RegisteredListener> listeners) {
      this.listeners = Preconditions.checkNotNull(listeners);
    }

    @Override
    public void registered(String frameworkId) throws RegisterHandlingException {
      LOG.info("Notifying " + listeners.size() + " listeners of registration.");
      for (RegisteredListener listener : listeners) {
        listener.registered(frameworkId);
      }
    }
  }
}
