/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.thrift.aop;

import java.util.Set;

import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.LockValidation;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.RewriteConfigsRequest;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.SessionKey;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.gen.UpdateQuery;
import org.apache.aurora.gen.UpdateRequest;
import org.apache.thrift.TException;

import static java.util.Objects.requireNonNull;

/**
 * A forwarding scheduler controller to make it easy to override specific behavior in an
 * implementation class.
 */
abstract class ForwardingThrift implements AuroraAdmin.Iface {

  private final AuroraAdmin.Iface delegate;

  ForwardingThrift(AuroraAdmin.Iface delegate) {
    this.delegate = requireNonNull(delegate);
  }

  @Override
  public Response setQuota(
      String ownerRole,
      ResourceAggregate resourceAggregate,
      SessionKey session) throws TException {

    return delegate.setQuota(ownerRole, resourceAggregate, session);
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
  public Response listBackups(SessionKey session) throws TException {
    return delegate.listBackups(session);
  }

  @Override
  public Response stageRecovery(String backupId, SessionKey session)
      throws TException {

    return delegate.stageRecovery(backupId, session);
  }

  @Override
  public Response queryRecovery(TaskQuery query, SessionKey session)
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
  public Response getRoleSummary() throws TException {
    return delegate.getRoleSummary();
  }

  @Override
  public Response getJobSummary(String role) throws TException {
    return delegate.getJobSummary(role);
  }

  @Override
  public Response getConfigSummary(JobKey key) throws TException {
    return delegate.getConfigSummary(key);
  }

  @Override
  public Response createJob(JobConfiguration description, Lock lock, SessionKey session)
      throws TException {

    return delegate.createJob(description, lock, session);
  }

    @Override
    public Response scheduleCronJob(JobConfiguration description, Lock lock, SessionKey session)
        throws TException {

      return delegate.scheduleCronJob(description, lock, session);
    }

    @Override
    public Response descheduleCronJob(JobKey job, Lock lock, SessionKey session) throws TException {

      return delegate.descheduleCronJob(job, lock, session);
    }

    @Override
  public Response replaceCronTemplate(JobConfiguration config, Lock lock, SessionKey session)
      throws TException {

    return delegate.replaceCronTemplate(config, lock, session);
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) throws TException {
    return delegate.populateJobConfig(description);
  }

  @Override
  public Response startCronJob(JobKey job, SessionKey session) throws TException {
    return delegate.startCronJob(job, session);
  }

  @Override
  public Response restartShards(
      JobKey job,
      Set<Integer> shardIds,
      Lock lock,
      SessionKey session) throws TException {

    return delegate.restartShards(job, shardIds, lock, session);
  }

  @Override
  public Response getTasksStatus(TaskQuery query) throws TException {
    return delegate.getTasksStatus(query);
  }

  @Override
  public Response getTasksWithoutConfigs(TaskQuery query) throws TException {
    return delegate.getTasksStatus(query);
  }

  @Override
  public Response getJobs(String ownerRole) throws TException {
    return delegate.getJobs(ownerRole);
  }

  @Override
  public Response killTasks(TaskQuery query, Lock lock, SessionKey session) throws TException {
    return delegate.killTasks(query, lock, session);
  }

  @Override
  public Response getQuota(String ownerRole) throws TException {
    return delegate.getQuota(ownerRole);
  }

  @Override
  public Response startMaintenance(Hosts hosts, SessionKey session)
      throws TException {

    return delegate.startMaintenance(hosts, session);
  }

  @Override
  public Response drainHosts(Hosts hosts, SessionKey session) throws TException {
    return delegate.drainHosts(hosts, session);
  }

  @Override
  public Response maintenanceStatus(Hosts hosts, SessionKey session)
      throws TException {

    return delegate.maintenanceStatus(hosts, session);
  }

  @Override
  public Response endMaintenance(Hosts hosts, SessionKey session) throws TException {
    return delegate.endMaintenance(hosts, session);
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
  public Response getVersion() throws TException {
    return delegate.getVersion();
  }

  @Override
  public Response acquireLock(LockKey lockKey, SessionKey session) throws TException {
    return delegate.acquireLock(lockKey, session);
  }

  @Override
  public Response releaseLock(Lock lock, LockValidation validation, SessionKey session)
      throws TException {

    return delegate.releaseLock(lock, validation, session);
  }

  @Override
  public Response getLocks() throws TException {
    return delegate.getLocks();
  }

  @Override
  public Response addInstances(
      AddInstancesConfig config,
      Lock lock,
      SessionKey session) throws TException {

    return delegate.addInstances(config, lock, session);
  }

  @Override
  public Response getPendingReason(TaskQuery query) throws TException {
    return delegate.getPendingReason(query);
  }

  @Override
  public Response startUpdate(UpdateRequest request, Lock lock, SessionKey session)
      throws TException {

    return delegate.startUpdate(request, lock, session);
  }

  @Override
  public Response pauseUpdate(String updateId, Lock lock, SessionKey session) throws TException {
    return delegate.pauseUpdate(updateId, lock, session);
  }

  @Override
  public Response resumeUpdate(String updateId, Lock lock, SessionKey session) throws TException {
    return delegate.resumeUpdate(updateId, lock, session);
  }

  @Override
  public Response abortUpdate(String updateId, Lock lock, SessionKey session) throws TException {
    return delegate.abortUpdate(updateId, lock, session);
  }

  @Override
  public Response getUpdates(UpdateQuery updateQuery) throws TException {
    return delegate.getUpdates(updateQuery);
  }

  @Override
  public Response getUpdateDetails(String updateId) throws TException {
    return delegate.getUpdateDetails(updateId);
  }
}
