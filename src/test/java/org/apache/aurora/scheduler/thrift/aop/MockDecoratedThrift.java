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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.inject.Binder;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ExplicitReconciliationSettings;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.SlaPolicy;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.http.api.security.AuthorizingParam;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;
import org.apache.thrift.TException;

/**
 * An injected forwarding thrift implementation that delegates to a bound mock interface.
 * <p>
 * This is required to allow AOP to take place. For more details, see
 * https://code.google.com/p/google-guice/wiki/AOP#Limitations
 */
@DecoratedThrift
public class MockDecoratedThrift implements AnnotatedAuroraAdmin {

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @Qualifier
  private @interface MockThrift { }

  private final AnnotatedAuroraAdmin annotatedAuroraAdmin;

  @Inject
  MockDecoratedThrift(@MockThrift AnnotatedAuroraAdmin delegate) {
    this.annotatedAuroraAdmin = delegate;
  }

  public static void bindForwardedMock(Binder binder, AnnotatedAuroraAdmin mockThrift) {
    binder.bind(AnnotatedAuroraAdmin.class).annotatedWith(MockThrift.class).toInstance(mockThrift);

    binder.bind(AnnotatedAuroraAdmin.class).to(MockDecoratedThrift.class);
    binder.bind(AuroraAdmin.Iface.class).to(MockDecoratedThrift.class);
  }

  @Override
  public Response getRoleSummary() throws TException {
    return this.annotatedAuroraAdmin.getRoleSummary();
  }

  @Override
  public Response getJobSummary(String role) throws TException {
    return this.annotatedAuroraAdmin.getJobSummary(role);
  }

  @Override
  public Response getTasksStatus(TaskQuery query) throws TException {
    return this.annotatedAuroraAdmin.getTasksStatus(query);
  }

  @Override
  public Response getTasksWithoutConfigs(TaskQuery query) throws TException {
    return this.annotatedAuroraAdmin.getTasksWithoutConfigs(query);
  }

  @Override
  public Response getPendingReason(TaskQuery query) throws TException {
    return this.annotatedAuroraAdmin.getPendingReason(query);
  }

  @Override
  public Response getConfigSummary(JobKey job) throws TException {
    return this.annotatedAuroraAdmin.getConfigSummary(job);
  }

  @Override
  public Response getJobs(String ownerRole) throws TException {
    return this.annotatedAuroraAdmin.getJobs(ownerRole);
  }

  @Override
  public Response getQuota(String ownerRole) throws TException {
    return this.annotatedAuroraAdmin.getQuota(ownerRole);
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) throws TException {
    return this.annotatedAuroraAdmin.populateJobConfig(description);
  }

  @Override
  public Response getJobUpdateSummaries(JobUpdateQuery jobUpdateQuery) throws TException {
    return this.annotatedAuroraAdmin.getJobUpdateSummaries(jobUpdateQuery);
  }

  @Override
  public Response getJobUpdateDetails(JobUpdateKey key, JobUpdateQuery query) throws TException {
    return this.annotatedAuroraAdmin.getJobUpdateDetails(key, query);
  }

  @Override
  public Response getJobUpdateDiff(JobUpdateRequest request) throws TException {
    return this.annotatedAuroraAdmin.getJobUpdateDiff(request);
  }

  @Override
  public Response getTierConfigs() throws TException {
    return this.annotatedAuroraAdmin.getTierConfigs();
  }

  @Override
  public Response setQuota(String ownerRole, ResourceAggregate quota) throws TException {
    return this.annotatedAuroraAdmin.setQuota(ownerRole, quota);
  }

  @Override
  public Response forceTaskState(String taskId, ScheduleStatus status) throws TException {
    return this.annotatedAuroraAdmin.forceTaskState(taskId, status);
  }

  @Override
  public Response performBackup() throws TException {
    return this.annotatedAuroraAdmin.performBackup();
  }

  @Override
  public Response listBackups() throws TException {
    return this.annotatedAuroraAdmin.listBackups();
  }

  @Override
  public Response stageRecovery(String backupId) throws TException {
    return this.annotatedAuroraAdmin.stageRecovery(backupId);
  }

  @Override
  public Response queryRecovery(TaskQuery query) throws TException {
    return this.annotatedAuroraAdmin.queryRecovery(query);
  }

  @Override
  public Response deleteRecoveryTasks(TaskQuery query) throws TException {
    return this.annotatedAuroraAdmin.deleteRecoveryTasks(query);
  }

  @Override
  public Response commitRecovery() throws TException {
    return this.annotatedAuroraAdmin.commitRecovery();
  }

  @Override
  public Response unloadRecovery() throws TException {
    return this.annotatedAuroraAdmin.unloadRecovery();
  }

  @Override
  public Response startMaintenance(Hosts hosts) throws TException {
    return this.annotatedAuroraAdmin.startMaintenance(hosts);
  }

  @Override
  public Response drainHosts(Hosts hosts) throws TException {
    return this.annotatedAuroraAdmin.drainHosts(hosts);
  }

  @Override
  public Response maintenanceStatus(Hosts hosts) throws TException {
    return this.annotatedAuroraAdmin.maintenanceStatus(hosts);
  }

  @Override
  public Response endMaintenance(Hosts hosts) throws TException {
    return this.annotatedAuroraAdmin.endMaintenance(hosts);
  }

  @Override
  public Response slaDrainHosts(Hosts hosts, SlaPolicy defaultSlaPolicy, long timeoutSecs)
      throws TException {
    return this.annotatedAuroraAdmin.slaDrainHosts(hosts, defaultSlaPolicy, timeoutSecs);
  }

  @Override
  public Response snapshot() throws TException {
    return this.annotatedAuroraAdmin.snapshot();
  }

  @Override
  public Response triggerExplicitTaskReconciliation(ExplicitReconciliationSettings settings)
      throws TException {

    return this.annotatedAuroraAdmin.triggerExplicitTaskReconciliation(settings);
  }

  @Override
  public Response triggerImplicitTaskReconciliation() throws TException {
    return this.annotatedAuroraAdmin.triggerImplicitTaskReconciliation();
  }

  @Override
  public Response pruneTasks(TaskQuery query) throws TException {
    return this.annotatedAuroraAdmin.pruneTasks(query);
  }

  @Override
  public Response createJob(@AuthorizingParam JobConfiguration arg0) throws TException {
    return this.annotatedAuroraAdmin.createJob(arg0);
  }

  @Override
  public Response scheduleCronJob(@AuthorizingParam JobConfiguration arg0) throws TException {
    return this.annotatedAuroraAdmin.scheduleCronJob(arg0);
  }

  @Override
  public Response descheduleCronJob(@AuthorizingParam JobKey arg0) throws TException {
    return this.annotatedAuroraAdmin.descheduleCronJob(arg0);
  }

  @Override
  public Response startCronJob(@AuthorizingParam JobKey arg0) throws TException {
    return this.annotatedAuroraAdmin.startCronJob(arg0);
  }

  @Override
  public Response restartShards(@AuthorizingParam JobKey arg0, Set<Integer> arg1)
      throws TException {

    return this.annotatedAuroraAdmin.restartShards(arg0, arg1);
  }

  @Override
  public Response killTasks(@AuthorizingParam JobKey arg0, Set<Integer> arg1, String arg2)
      throws TException {

    return this.annotatedAuroraAdmin.killTasks(arg0, arg1, arg2);
  }

  @Override
  public Response addInstances(@AuthorizingParam InstanceKey arg0, int arg1) throws TException {
    return this.annotatedAuroraAdmin.addInstances(arg0, arg1);
  }

  @Override
  public Response replaceCronTemplate(@AuthorizingParam JobConfiguration arg0) throws TException {
    return this.annotatedAuroraAdmin.replaceCronTemplate(arg0);
  }

  @Override
  public Response startJobUpdate(@AuthorizingParam JobUpdateRequest arg0, String arg1)
      throws TException {

    return this.annotatedAuroraAdmin.startJobUpdate(arg0, arg1);
  }

  @Override
  public Response pauseJobUpdate(@AuthorizingParam JobUpdateKey arg0, String arg1)
      throws TException {

    return this.annotatedAuroraAdmin.pauseJobUpdate(arg0, arg1);
  }

  @Override
  public Response resumeJobUpdate(@AuthorizingParam JobUpdateKey arg0, String arg1)
      throws TException {

    return this.annotatedAuroraAdmin.resumeJobUpdate(arg0, arg1);
  }

  @Override
  public Response abortJobUpdate(@AuthorizingParam JobUpdateKey arg0, String arg1)
      throws TException {

    return this.annotatedAuroraAdmin.abortJobUpdate(arg0, arg1);
  }

  @Override
  public Response pulseJobUpdate(@AuthorizingParam JobUpdateKey arg0) throws TException {
    return this.annotatedAuroraAdmin.pulseJobUpdate(arg0);
  }

  @Override
  public Response rollbackJobUpdate(@AuthorizingParam JobUpdateKey arg0, String arg1)
      throws TException {

    return this.annotatedAuroraAdmin.rollbackJobUpdate(arg0, arg1);
  }
}
