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
import com.twitter.mesos.gen.ShardUpdateRequest;
import com.twitter.mesos.gen.ShardUpdateResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.UpdateCompleteResponse;
import com.twitter.mesos.gen.UpdateConfigResponse;
import com.twitter.mesos.gen.UpdateRequest;
import com.twitter.mesos.gen.UpdateResponse;

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
  public CreateJobResponse createJob(JobConfiguration description, SessionKey session) throws TException {
    return delegate.createJob(description, session);
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) throws TException {
    return delegate.getTasksStatus(query);
  }

  @Override
  public UpdateResponse updateTasks(UpdateRequest request) throws TException {
    return delegate.updateTasks(request);
  }

  @Override
  public KillResponse killTasks(TaskQuery query, SessionKey session) throws TException {
    return delegate.killTasks(query, session);
  }

  @Override
  public RestartResponse restartTasks(Set<String> taskIds, SessionKey session) throws TException {
    return delegate.restartTasks(taskIds, session);
  }

  @Override
  public UpdateConfigResponse getUpdateConfig(String updateToken) throws TException {
    return delegate.getUpdateConfig(updateToken);
  }

  @Override
  public ShardUpdateResponse updateShards(ShardUpdateRequest request) throws TException {
    return delegate.updateShards(request);
  }

  @Override
  public UpdateCompleteResponse finishUpdate(String updateToken) throws TException {
    return delegate.finishUpdate(updateToken);
  }

  @Override
  public UpdateCompleteResponse cancelUpdate(String role, String jobName, SessionKey session) throws TException {
    return delegate.cancelUpdate(role, jobName, session);
  }
}
