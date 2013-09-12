package com.twitter.aurora.scheduler.thrift.aop;

import java.util.Set;

import org.apache.thrift.TException;

import com.twitter.aurora.gen.APIVersion;
import com.twitter.aurora.gen.AuroraAdmin;
import com.twitter.aurora.gen.DrainHostsResponse;
import com.twitter.aurora.gen.EndMaintenanceResponse;
import com.twitter.aurora.gen.GetJobUpdatesResponse;
import com.twitter.aurora.gen.GetJobsResponse;
import com.twitter.aurora.gen.GetQuotaResponse;
import com.twitter.aurora.gen.Hosts;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.ListBackupsResponse;
import com.twitter.aurora.gen.MaintenanceStatusResponse;
import com.twitter.aurora.gen.PopulateJobResponse;
import com.twitter.aurora.gen.QueryRecoveryResponse;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.Response;
import com.twitter.aurora.gen.RewriteConfigsRequest;
import com.twitter.aurora.gen.RollbackShardsResponse;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduleStatusResponse;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.gen.StartMaintenanceResponse;
import com.twitter.aurora.gen.StartUpdateResponse;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.gen.UpdateResult;
import com.twitter.aurora.gen.UpdateShardsResponse;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A forwarding scheduler controller to make it easy to override specific behavior in an
 * implementation class.
 */
abstract class ForwardingThrift implements AuroraAdmin.Iface {

  private final AuroraAdmin.Iface delegate;

  ForwardingThrift(AuroraAdmin.Iface delegate) {
    this.delegate = checkNotNull(delegate);
  }

  @Override
  public Response setQuota(String ownerRole, Quota quota, SessionKey session)
      throws TException {

    return delegate.setQuota(ownerRole, quota, session);
  }

  @Override
  public Response forceTaskState(
      String taskId,
      ScheduleStatus status,
      SessionKey session) throws TException {

    return delegate.forceTaskState(taskId, status, session);
  }

  @Override
  public Response performBackup(SessionKey session) throws TException {
    return delegate.performBackup(session);
  }

  @Override
  public ListBackupsResponse listBackups(SessionKey session) throws TException {
    return delegate.listBackups(session);
  }

  @Override
  public Response stageRecovery(String backupId, SessionKey session)
      throws TException {

    return delegate.stageRecovery(backupId, session);
  }

  @Override
  public QueryRecoveryResponse queryRecovery(TaskQuery query, SessionKey session)
      throws TException {

    return delegate.queryRecovery(query, session);
  }

  @Override
  public Response deleteRecoveryTasks(TaskQuery query, SessionKey session)
      throws TException {

    return delegate.deleteRecoveryTasks(query, session);
  }

  @Override
  public Response commitRecovery(SessionKey session) throws TException {
    return delegate.commitRecovery(session);
  }

  @Override
  public Response unloadRecovery(SessionKey session) throws TException {
    return delegate.unloadRecovery(session);
  }

  @Override
  public Response createJob(JobConfiguration description, SessionKey session)
      throws TException {

    return delegate.createJob(description, session);
  }

  @Override
  public PopulateJobResponse populateJobConfig(JobConfiguration description) throws TException {
    return delegate.populateJobConfig(description);
  }

  @Override
  public Response startCronJob(JobKey job, SessionKey session) throws TException {
    return delegate.startCronJob(job, session);
  }

  @Override
  public StartUpdateResponse startUpdate(JobConfiguration updatedConfig, SessionKey session)
      throws TException {

    return delegate.startUpdate(updatedConfig, session);
  }

  @Override
  public UpdateShardsResponse updateShards(
      JobKey job,
      Set<Integer> shardIds,
      String updateToken,
      SessionKey session) throws TException {

    return delegate.updateShards(job, shardIds, updateToken, session);
  }

  @Override
  public RollbackShardsResponse rollbackShards(
      JobKey job,
      Set<Integer> shardIds,
      String updateToken,
      SessionKey session) throws TException {

    return delegate.rollbackShards(job, shardIds, updateToken, session);
  }

  @Override
  public Response finishUpdate(
      JobKey job,
      UpdateResult updateResult,
      String updateToken,
      SessionKey session) throws TException {

    return delegate.finishUpdate(job, updateResult, updateToken, session);
  }

  @Override
  public Response restartShards(
      JobKey job,
      Set<Integer> shardIds,
      SessionKey session) throws TException {

    return delegate.restartShards(job, shardIds, session);
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) throws TException {
    return delegate.getTasksStatus(query);
  }

  @Override
  public GetJobsResponse getJobs(String ownerRole) throws TException {
    return delegate.getJobs(ownerRole);
  }

  @Override
  public Response killTasks(TaskQuery query, SessionKey session) throws TException {
    return delegate.killTasks(query, session);
  }

  @Override
  public GetQuotaResponse getQuota(String ownerRole) throws TException {
    return delegate.getQuota(ownerRole);
  }

  @Override
  public StartMaintenanceResponse startMaintenance(Hosts hosts, SessionKey session)
      throws TException {

    return delegate.startMaintenance(hosts, session);
  }

  @Override
  public DrainHostsResponse drainHosts(Hosts hosts, SessionKey session) throws TException {
    return delegate.drainHosts(hosts, session);
  }

  @Override
  public MaintenanceStatusResponse maintenanceStatus(Hosts hosts, SessionKey session)
      throws TException {

    return delegate.maintenanceStatus(hosts, session);
  }

  @Override
  public EndMaintenanceResponse endMaintenance(Hosts hosts, SessionKey session) throws TException {
    return delegate.endMaintenance(hosts, session);
  }

  @Override
  public GetJobUpdatesResponse getJobUpdates(SessionKey session) throws TException {
    return delegate.getJobUpdates(session);
  }

  @Override
  public Response snapshot(SessionKey session) throws TException {
    return delegate.snapshot(session);
  }

  @Override
  public Response rewriteConfigs(RewriteConfigsRequest request, SessionKey session)
      throws TException {

    return delegate.rewriteConfigs(request, session);
  }

  @Override
  public APIVersion getVersion() throws TException {
    return delegate.getVersion();
  }
}
