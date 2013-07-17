package com.twitter.aurora.scheduler.thrift;

import java.util.Set;

import com.twitter.aurora.gen.CommitRecoveryResponse;
import com.twitter.aurora.gen.CreateJobResponse;
import com.twitter.aurora.gen.DeleteRecoveryTasksResponse;
import com.twitter.aurora.gen.DrainHostsResponse;
import com.twitter.aurora.gen.EndMaintenanceResponse;
import com.twitter.aurora.gen.FinishUpdateResponse;
import com.twitter.aurora.gen.ForceTaskStateResponse;
import com.twitter.aurora.gen.GetJobUpdatesResponse;
import com.twitter.aurora.gen.GetJobsResponse;
import com.twitter.aurora.gen.GetQuotaResponse;
import com.twitter.aurora.gen.Hosts;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.KillResponse;
import com.twitter.aurora.gen.ListBackupsResponse;
import com.twitter.aurora.gen.MaintenanceStatusResponse;
import com.twitter.aurora.gen.PerformBackupResponse;
import com.twitter.aurora.gen.PopulateJobResponse;
import com.twitter.aurora.gen.QueryRecoveryResponse;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.RestartShardsResponse;
import com.twitter.aurora.gen.RewriteConfigsRequest;
import com.twitter.aurora.gen.RewriteConfigsResponse;
import com.twitter.aurora.gen.RollbackShardsResponse;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduleStatusResponse;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.gen.SetQuotaResponse;
import com.twitter.aurora.gen.SnapshotResponse;
import com.twitter.aurora.gen.StageRecoveryResponse;
import com.twitter.aurora.gen.StartCronResponse;
import com.twitter.aurora.gen.StartMaintenanceResponse;
import com.twitter.aurora.gen.StartUpdateResponse;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.gen.UnloadRecoveryResponse;
import com.twitter.aurora.gen.UpdateResult;
import com.twitter.aurora.gen.UpdateShardsResponse;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A forwarding scheduler controller to make it easy to override specific behavior in an
 * implementation class.
 */
abstract class ForwardingSchedulerController implements SchedulerController {

  private final SchedulerController delegate;

  ForwardingSchedulerController(SchedulerController delegate) {
    this.delegate = checkNotNull(delegate);
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

  @Override
  public SnapshotResponse snapshot(SessionKey session) {
    return delegate.snapshot(session);
  }

  @Override
  public RewriteConfigsResponse rewriteConfigs(RewriteConfigsRequest request, SessionKey session) {
    return delegate.rewriteConfigs(request, session);
  }
}
