package com.twitter.mesos.scheduler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.thrift.TException;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CommitRecoveryResponse;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.DeleteRecoveryTasksResponse;
import com.twitter.mesos.gen.DrainHostsResponse;
import com.twitter.mesos.gen.EndMaintenanceResponse;
import com.twitter.mesos.gen.FinishUpdateResponse;
import com.twitter.mesos.gen.ForceTaskStateResponse;
import com.twitter.mesos.gen.GetQuotaResponse;
import com.twitter.mesos.gen.Hosts;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.ListBackupsResponse;
import com.twitter.mesos.gen.MaintenanceStatusResponse;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.gen.MesosAdmin.Iface;
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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A decorating scheduler thrift interface that logs requests.
 */
class LoggingThriftInterface implements MesosAdmin.Iface {

  private static final Logger LOG = Logger.getLogger(LoggingThriftInterface.class.getName());

  private static final MethodInterceptor UNHANDLED_EXCEPTION_INTERCEPTOR = new MethodInterceptor() {
    @Override public Object invoke(MethodInvocation invocation) throws Throwable {
      try {
        return invocation.proceed();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "Uncaught exception while handling "
            + invocation.getMethod().getName()
            + "(" + ImmutableList.copyOf(invocation.getArguments()) + ")", e);
        throw e;
      }
    }
  };

  /**
   * A {@literal @BindingAnnotation} that the delegate thrift interface should be bound against.
   */
  @BindingAnnotation
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @interface ThriftDelegate { }

  private final Iface delegate;

  @Inject
  LoggingThriftInterface(@ThriftDelegate Iface delegate) {
    this.delegate = checkNotNull(delegate);
  }

  private void logUserAction(SessionKey session, String message) {
    LOG.info("Request by " + session.getUser() + " to " + message);
  }

  public static void bind(Binder binder, Class<? extends Iface> delegate) {
    checkNotNull(binder);
    checkNotNull(delegate);
    binder.bind(Iface.class).to(LoggingThriftInterface.class);
    binder.bind(LoggingThriftInterface.class).in(Singleton.class);
    binder.bind(Iface.class).annotatedWith(ThriftDelegate.class).to(delegate);
    binder.bindInterceptor(
        Matchers.only(LoggingThriftInterface.class),
        Matchers.any(),
        UNHANDLED_EXCEPTION_INTERCEPTOR);
  }

  @Override
  public SetQuotaResponse setQuota(String ownerRole, Quota quota, SessionKey session)
      throws TException {

    logUserAction(session, "adjust " + ownerRole + " quota to " + quota);
    return delegate.setQuota(ownerRole, quota, session);
  }

  @Override
  public ForceTaskStateResponse forceTaskState(
      String taskId,
      ScheduleStatus status,
      SessionKey session) throws TException {

    logUserAction(session, "force " + taskId + " state to " + status);
    return delegate.forceTaskState(taskId, status, session);
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration description, SessionKey session)
      throws TException {

    logUserAction(session, "create job " + Tasks.jobKey(description));
    return delegate.createJob(description, session);
  }

  @Override
  public PopulateJobResponse populateJobConfig(JobConfiguration description) throws TException {
    LOG.info("Request to populate job config " + Tasks.jobKey(description));
    return delegate.populateJobConfig(description);
  }

  @Override
  public StartCronResponse startCronJob(String role, String jobName, SessionKey session)
      throws TException {

    logUserAction(session, "start cron job " + Tasks.jobKey(role, jobName));
    return delegate.startCronJob(role, jobName, session);
  }

  @Override
  public StartUpdateResponse startUpdate(JobConfiguration updatedConfig, SessionKey session)
      throws TException {

    logUserAction(session, "start updating job " + Tasks.jobKey(updatedConfig));
    return delegate.startUpdate(updatedConfig, session);
  }

  @Override
  public UpdateShardsResponse updateShards(
      String ownerRole,
      String jobName,
      Set<Integer> shardIds,
      String updateToken,
      SessionKey session) throws TException {

    logUserAction(session, "update job " + jobName + " shards " + shardIds);
    return delegate.updateShards(ownerRole, jobName, shardIds, updateToken, session);
  }

  @Override
  public RollbackShardsResponse rollbackShards(
      String ownerRole,
      String jobName,
      Set<Integer> shardIds,
      String updateToken,
      SessionKey session) throws TException {

    logUserAction(session, "rollback job " + jobName + " shards " + shardIds);
    return delegate.rollbackShards(ownerRole, jobName, shardIds, updateToken, session);
  }

  @Override
  public FinishUpdateResponse finishUpdate(
      String ownerRole,
      String jobName,
      UpdateResult updateResult,
      String updateToken,
      SessionKey session) throws TException {

    logUserAction(session, "finish updating job " + jobName + " with result " + updateResult);
    return delegate.finishUpdate(ownerRole, jobName, updateResult, updateToken, session);
  }

  @Override
  public RestartShardsResponse restartShards(
      String role,
      String jobName,
      Set<Integer> shardIds,
      SessionKey session) throws TException {

    logUserAction(session, "restart shards " + role + "/" + jobName + " " + shardIds);
    return delegate.restartShards(role, jobName, shardIds, session);
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) throws TException {
    LOG.info("Request to fetch status for tasks matching " + query);
    return delegate.getTasksStatus(query);
  }

  @Override
  public KillResponse killTasks(TaskQuery query, SessionKey session) throws TException {
    logUserAction(session, "kill tasks matching " + query);
    return delegate.killTasks(query, session);
  }

  @Override
  public GetQuotaResponse getQuota(String ownerRole) throws TException {
    LOG.info("Request to fetch quota for " + ownerRole);
    return delegate.getQuota(ownerRole);
  }

  @Override
  public PerformBackupResponse performBackup(SessionKey session) throws TException {
    logUserAction(session, "perform backup immediately");
    return delegate.performBackup(session);
  }

  @Override
  public ListBackupsResponse listBackups(SessionKey session) throws TException {
    logUserAction(session, "list backups");
    return delegate.listBackups(session);
  }

  @Override
  public StageRecoveryResponse stageRecovery(String backupId, SessionKey session)
      throws TException {

    logUserAction(session, "stage backup " + backupId);
    return delegate.stageRecovery(backupId, session);
  }

  @Override
  public QueryRecoveryResponse queryRecovery(TaskQuery query, SessionKey session)
      throws TException {

    logUserAction(session, "query recovery for " + query);
    return delegate.queryRecovery(query, session);
  }

  @Override
  public DeleteRecoveryTasksResponse deleteRecoveryTasks(TaskQuery query, SessionKey session)
      throws TException {

    logUserAction(session, "delete recovery tasks matching " + query);
    return delegate.deleteRecoveryTasks(query, session);
  }

  @Override
  public CommitRecoveryResponse commitRecovery(SessionKey session) throws TException {
    logUserAction(session, "commit staged recovery");
    return delegate.commitRecovery(session);
  }

  @Override
  public UnloadRecoveryResponse unloadRecovery(SessionKey session) throws TException {
    logUserAction(session, "unload staged recovery");
    return delegate.unloadRecovery(session);
  }

  @Override
  public StartMaintenanceResponse startMaintenance(
      Hosts hosts, SessionKey session) throws TException {

    logUserAction(session, "Starting cluster maintenance on hosts " + hosts);
    return delegate.startMaintenance(hosts, session);
  }

  @Override
  public DrainHostsResponse drainHosts(Hosts hostNames, SessionKey session) throws TException {
    logUserAction(session, "Draining tasks off maintenance hosts " + hostNames);
    return delegate.drainHosts(hostNames, session);
  }

  @Override
  public MaintenanceStatusResponse maintenanceStatus(
      Hosts hosts,
      SessionKey session) throws TException {

    logUserAction(session, "Gathering maintenance status of hosts " + hosts);
    return delegate.maintenanceStatus(hosts, session);
  }

  @Override
  public EndMaintenanceResponse endMaintenance(
      Hosts hosts,
      SessionKey session) throws TException {

    logUserAction(session, "Ending cluster maintenance for " + hosts);
    return delegate.endMaintenance(hosts, session);  }

}
