package com.twitter.mesos.updater;

import com.google.inject.Inject;
import com.twitter.mesos.gen.MesosSchedulerManager.Iface;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Coordinates between the scheduler and the update logic to perform an update.
 *
 * @author wfarner
 */
public class Coordinator {
  private final Iface scheduler;

  /**
   * {@literal @Named} binding key for the opaque update token.
   */
  static final String UPDATE_TOKEN = "com.twitter.mesos.updater.Coordinator.UPDATE_TOKEN";

  @Inject
  public Coordinator(Iface scheduler, Integer updateToken) {
    this.scheduler = checkNotNull(scheduler);

    // TODO(wfarner): Finish defining and implementing this class.
  }
}
