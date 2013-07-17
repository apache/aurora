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

/**
 * The Scheduler interface.
 *
 * TODO(ksweeney): Make this less monolithic by factoring out different concerns.
 * TODO(ksweeney): Extract an input validation layer.
 */
interface SchedulerController {

  SetQuotaResponse setQuota(String ownerRole, Quota quota, SessionKey session);

  ForceTaskStateResponse forceTaskState(
      String taskId,
      ScheduleStatus status,
      SessionKey session);

  PerformBackupResponse performBackup(SessionKey session);

  ListBackupsResponse listBackups(SessionKey session);

  StageRecoveryResponse stageRecovery(String backupId, SessionKey session);

  QueryRecoveryResponse queryRecovery(TaskQuery query, SessionKey session);

  DeleteRecoveryTasksResponse deleteRecoveryTasks(TaskQuery query, SessionKey session);

  CommitRecoveryResponse commitRecovery(SessionKey session);

  UnloadRecoveryResponse unloadRecovery(SessionKey session);

  CreateJobResponse createJob(JobConfiguration description, SessionKey session);

  PopulateJobResponse populateJobConfig(JobConfiguration description);

  StartCronResponse startCronJob(JobKey job, SessionKey session);

  StartUpdateResponse startUpdate(JobConfiguration updatedConfig, SessionKey session);

  UpdateShardsResponse updateShards(
      JobKey job,
      Set<Integer> shardIds,
      String updateToken,
      SessionKey session);

  RollbackShardsResponse rollbackShards(
      JobKey job,
      Set<Integer> shardIds,
      String updateToken,
      SessionKey session);

  FinishUpdateResponse finishUpdate(
      JobKey job,
      UpdateResult updateResult,
      String updateToken,
      SessionKey session);

  RestartShardsResponse restartShards(JobKey job, Set<Integer> shardIds, SessionKey session);

  ScheduleStatusResponse getTasksStatus(TaskQuery query);

  GetJobsResponse getJobs(String ownerRole);

  KillResponse killTasks(TaskQuery query, SessionKey session);

  GetQuotaResponse getQuota(String ownerRole);

  StartMaintenanceResponse startMaintenance(Hosts hosts, SessionKey session);

  DrainHostsResponse drainHosts(Hosts hosts, SessionKey session);

  MaintenanceStatusResponse maintenanceStatus(Hosts hosts, SessionKey session);

  EndMaintenanceResponse endMaintenance(Hosts hosts, SessionKey session);

  GetJobUpdatesResponse getJobUpdates(SessionKey session);

  SnapshotResponse snapshot(SessionKey session);

  RewriteConfigsResponse rewriteConfigs(RewriteConfigsRequest request, SessionKey session);
}
