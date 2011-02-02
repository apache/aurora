package com.twitter.mesos.scheduler;

import javax.annotation.Nullable;

/**
 * Stores data specific to the scheduler itself.
 *
 * @author jsirois
 */
public interface SchedulerStore {

  /**
   * Stores the given framework id overwriting any previously saved id.
   *
   * @param frameworkId The framework id to store.
   */
  void saveFrameworkId(String frameworkId);

  /**
   * Fetches the last saved framework id.  If none is saved, null can be returned.
   *
   * @return the last saved framework id
   */
  @Nullable String fetchFrameworkId();
}
