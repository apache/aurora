package com.twitter.aurora.scheduler.thrift;

import java.util.Set;

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
import com.twitter.mesos.gen.SnapshotResponse;
import com.twitter.mesos.gen.StageRecoveryResponse;
import com.twitter.mesos.gen.StartCronResponse;
import com.twitter.mesos.gen.StartMaintenanceResponse;
import com.twitter.mesos.gen.StartUpdateResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.UnloadRecoveryResponse;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.UpdateShardsResponse;

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
}
