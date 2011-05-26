package com.twitter.mesos.scheduler.migrations;

import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.thrift.TException;

import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.MesosSchedulerManager;
import com.twitter.mesos.gen.RestartResponse;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.TaskQuery;

/**
 * An implementation of the mesos scheduler manager thrift interface that forwards all calls to
 * a delegate.  Intended to be subclassed by code needing to re-route service calls or otherwise
 * modify requests or responses.
 *
 * @author John Sirois
 */
public abstract class ForwardingMesosSchedulerManager implements MesosSchedulerManager.Iface {

  private final MesosSchedulerManager.Iface delegate;

  /**
   * @param delegate a delegate that all calls will be forwarded to.
   */
  protected ForwardingMesosSchedulerManager(MesosSchedulerManager.Iface delegate) {
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration description, SessionKey session)
      throws TException {
    return delegate.createJob(description, session);
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) throws TException {
    return delegate.getTasksStatus(query);
  }

  @Override
  public KillResponse killTasks(TaskQuery query, SessionKey session) throws TException {
    return delegate.killTasks(query, session);
  }

  @Override
  public RestartResponse restartTasks(Set<String> taskIds, SessionKey session) throws TException {
    return delegate.restartTasks(taskIds, session);
  }
}
