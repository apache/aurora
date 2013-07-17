package com.twitter.aurora.scheduler.thrift;

import java.util.Set;

import com.google.inject.Inject;

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
import com.twitter.aurora.gen.MesosAdmin;
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
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator.Capability;
import com.twitter.aurora.scheduler.thrift.auth.Requires;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Aurora scheduler thrift implementation. Performs routing between Thrift calls and
 * controllers. For now this is a thin shim to avoid huge refactors for simple Thrift API
 * changes and input validation is performed by the controllers.
 */
public class SchedulerThriftRouter implements MesosAdmin.Iface {

  private SchedulerController schedulerController;

  @Inject
  SchedulerThriftRouter(SchedulerController schedulerController) {
    this.schedulerController = checkNotNull(schedulerController);
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration job, SessionKey session) {
    return schedulerController.createJob(job, session);
  }

  @Override
  public PopulateJobResponse populateJobConfig(JobConfiguration description) {
    return schedulerController.populateJobConfig(description);
  }

  @Override
  public StartCronResponse startCronJob(JobKey jobKey, SessionKey session) {
    return schedulerController.startCronJob(jobKey, session);
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) {
    return schedulerController.getTasksStatus(query);
  }

  @Override
  public GetJobsResponse getJobs(String ownerRole) {
    return schedulerController.getJobs(ownerRole);
  }

  @Override
  public KillResponse killTasks(final TaskQuery query, SessionKey session) {
    return schedulerController.killTasks(query, session);
  }

  @Override
  public StartUpdateResponse startUpdate(JobConfiguration job, SessionKey session) {
    return schedulerController.startUpdate(job, session);
  }

  @Override
  public UpdateShardsResponse updateShards(
      JobKey jobKey,
      Set<Integer> shards,
      String updateToken,
      SessionKey session) {

    return schedulerController.updateShards(jobKey, shards, updateToken, session);
  }

  @Override
  public RollbackShardsResponse rollbackShards(
      JobKey jobKey,
      Set<Integer> shards,
      String updateToken,
      SessionKey session) {

    return schedulerController.rollbackShards(jobKey, shards, updateToken, session);
  }

  @Override
  public FinishUpdateResponse finishUpdate(
      JobKey jobKey,
      UpdateResult updateResult,
      String updateToken,
      SessionKey session) {

    return schedulerController.finishUpdate(jobKey, updateResult, updateToken, session);
  }

  @Override
  public RestartShardsResponse restartShards(
      JobKey jobKey,
      Set<Integer> shardIds,
      SessionKey session) {

    return schedulerController.restartShards(jobKey, shardIds, session);
  }

  @Override
  public GetQuotaResponse getQuota(String ownerRole) {
    return schedulerController.getQuota(ownerRole);
  }

  @Override
  public StartMaintenanceResponse startMaintenance(Hosts hosts, SessionKey session) {
    return schedulerController.startMaintenance(hosts, session);
  }

  @Override
  public DrainHostsResponse drainHosts(Hosts hosts, SessionKey session) {
    return schedulerController.drainHosts(hosts, session);
  }

  @Override
  public MaintenanceStatusResponse maintenanceStatus(Hosts hosts, SessionKey session) {
    return schedulerController.maintenanceStatus(hosts, session);
  }

  @Override
  public EndMaintenanceResponse endMaintenance(Hosts hosts, SessionKey session) {
    return schedulerController.endMaintenance(hosts, session);
  }

  @Requires(whitelist = Capability.PROVISIONER)
  @Override
  public SetQuotaResponse setQuota(String ownerRole, Quota quota, SessionKey session) {
    return schedulerController.setQuota(ownerRole, quota, session);
  }

  @Override
  public ForceTaskStateResponse forceTaskState(
      String taskId,
      ScheduleStatus status,
      SessionKey session) {

    return schedulerController.forceTaskState(taskId, status, session);
  }

  @Override
  public PerformBackupResponse performBackup(SessionKey session) {
    return schedulerController.performBackup(session);
  }

  @Override
  public ListBackupsResponse listBackups(SessionKey session) {
    return schedulerController.listBackups(session);
  }

  @Override
  public StageRecoveryResponse stageRecovery(String backupId, SessionKey session) {
    return schedulerController.stageRecovery(backupId, session);
  }

  @Override
  public QueryRecoveryResponse queryRecovery(TaskQuery query, SessionKey session) {
    return schedulerController.queryRecovery(query, session);
  }

  @Override
  public DeleteRecoveryTasksResponse deleteRecoveryTasks(TaskQuery query, SessionKey session) {
    return schedulerController.deleteRecoveryTasks(query, session);
  }

  @Override
  public CommitRecoveryResponse commitRecovery(SessionKey session) {
    return schedulerController.commitRecovery(session);
  }

  @Override
  public UnloadRecoveryResponse unloadRecovery(SessionKey session) {
    return schedulerController.unloadRecovery(session);
  }

  @Override
  public GetJobUpdatesResponse getJobUpdates(SessionKey session) {
    return schedulerController.getJobUpdates(session);
  }

  @Override
  public SnapshotResponse snapshot(SessionKey session) {
    return schedulerController.snapshot(session);
  }

  @Override
  public RewriteConfigsResponse rewriteConfigs(RewriteConfigsRequest request, SessionKey session) {
    return schedulerController.rewriteConfigs(request, session);
  }
}
