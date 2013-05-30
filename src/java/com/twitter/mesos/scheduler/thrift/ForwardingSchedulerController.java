package com.twitter.mesos.scheduler.thrift;

import java.util.Set;

import com.google.common.base.Preconditions;

import com.twitter.mesos.gen.CommitRecoveryResponse;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.DeleteRecoveryTasksResponse;
import com.twitter.mesos.gen.DrainHostsResponse;
import com.twitter.mesos.gen.EndMaintenanceResponse;
import com.twitter.mesos.gen.FinishUpdateResponse;
import com.twitter.mesos.gen.ForceTaskStateResponse;
import com.twitter.mesos.gen.GetJobUpdatesResponse;
import com.twitter.mesos.gen.GetJobsResponse;
import com.twitter.mesos.gen.GetQuotaResponse;
import com.twitter.mesos.gen.Hosts;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.JobKey;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.ListBackupsResponse;
import com.twitter.mesos.gen.MaintenanceStatusResponse;
import com.twitter.mesos.gen.PerformBackupResponse;
import com.twitter.mesos.gen.PopulateJobResponse;
import com.twitter.mesos.gen.QueryRecoveryResponse;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.RestartShardsResponse;
import com.twitter.mesos.gen.RollbackShardsResponse;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.SetQuotaResponse;
import com.twitter.mesos.gen.StageRecoveryResponse;
import com.twitter.mesos.gen.StartCronResponse;
import com.twitter.mesos.gen.StartMaintenanceResponse;
import com.twitter.mesos.gen.StartUpdateResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.UnloadRecoveryResponse;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.UpdateShardsResponse;

/**
 * A forwarding scheduler controller to make it easy to override specific behavior in an
 * implementation class.
 */
abstract class ForwardingSchedulerController implements SchedulerController {

  private final SchedulerController delegate;

  ForwardingSchedulerController(SchedulerController delegate) {
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  @Override
  public SetQuotaResponse setQuota(String ownerRole, Quota quota, SessionKey session) {
    return delegate.setQuota(ownerRole, quota, session);
  }

  @Override
  public ForceTaskStateResponse forceTaskState(
      String taskId,
      ScheduleStatus status,
      SessionKey session) {

    return delegate.forceTaskState(taskId, status, session);
  }

  @Override
  public PerformBackupResponse performBackup(SessionKey session) {
    return delegate.performBackup(session);
  }

  @Override
  public ListBackupsResponse listBackups(SessionKey session) {
    return delegate.listBackups(session);
  }

  @Override
  public StageRecoveryResponse stageRecovery(String backupId, SessionKey session) {
    return delegate.stageRecovery(backupId, session);
  }

  @Override
  public QueryRecoveryResponse queryRecovery(TaskQuery query, SessionKey session) {
    return delegate.queryRecovery(query, session);
  }

  @Override
  public DeleteRecoveryTasksResponse deleteRecoveryTasks(TaskQuery query, SessionKey session) {
    return delegate.deleteRecoveryTasks(query, session);
  }

  @Override
  public CommitRecoveryResponse commitRecovery(SessionKey session) {
    return delegate.commitRecovery(session);
  }

  @Override
  public UnloadRecoveryResponse unloadRecovery(SessionKey session) {
    return delegate.unloadRecovery(session);
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration description, SessionKey session) {
    return delegate.createJob(description, session);
  }

  @Override
  public PopulateJobResponse populateJobConfig(JobConfiguration description) {
    return delegate.populateJobConfig(description);
  }

  @Override
  public StartCronResponse startCronJob(JobKey job, SessionKey session) {
    return delegate.startCronJob(job, session);
  }

  @Override
  public StartUpdateResponse startUpdate(JobConfiguration updatedConfig, SessionKey session) {
    return delegate.startUpdate(updatedConfig, session);
  }

  @Override
  public UpdateShardsResponse updateShards(
      JobKey job,
      Set<Integer> shardIds,
      String updateToken,
      SessionKey session) {

    return delegate.updateShards(job, shardIds, updateToken, session);
  }

  @Override
  public RollbackShardsResponse rollbackShards(
      JobKey job,
      Set<Integer> shardIds,
      String updateToken,
      SessionKey session) {

    return delegate.rollbackShards(job, shardIds, updateToken, session);
  }

  @Override
  public FinishUpdateResponse finishUpdate(
      JobKey job,
      UpdateResult updateResult,
      String updateToken,
      SessionKey session) {

    return delegate.finishUpdate(job, updateResult, updateToken, session);
  }

  @Override
  public RestartShardsResponse restartShards(
      JobKey job,
      Set<Integer> shardIds,
      SessionKey session) {

    return delegate.restartShards(job, shardIds, session);
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) {
    return delegate.getTasksStatus(query);
  }

  @Override
  public GetJobsResponse getJobs(String ownerRole) {
    return delegate.getJobs(ownerRole);
  }

  @Override
  public KillResponse killTasks(TaskQuery query, SessionKey session) {
    return delegate.killTasks(query, session);
  }

  @Override
  public GetQuotaResponse getQuota(String ownerRole) {
    return delegate.getQuota(ownerRole);
  }

  @Override
  public StartMaintenanceResponse startMaintenance(Hosts hosts, SessionKey session) {
    return delegate.startMaintenance(hosts, session);
  }

  @Override
  public DrainHostsResponse drainHosts(Hosts hosts, SessionKey session) {
    return delegate.drainHosts(hosts, session);
  }

  @Override
  public MaintenanceStatusResponse maintenanceStatus(Hosts hosts, SessionKey session) {
    return delegate.maintenanceStatus(hosts, session);
  }

  @Override
  public EndMaintenanceResponse endMaintenance(Hosts hosts, SessionKey session) {
    return delegate.endMaintenance(hosts, session);
  }

  @Override
  public GetJobUpdatesResponse getJobUpdates(SessionKey session) {
    return delegate.getJobUpdates(session);
  }
}
