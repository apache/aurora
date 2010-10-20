package com.twitter.mesos.scheduler;

import com.twitter.mesos.Message;

/**
 * Defines an interface that is compatible with the mesos SchedulerDriver.
 *
 * @author wfarner
 */
public interface Driver {

  public int sendMessage(Message message);

  public int killTask(int taskId);
}
