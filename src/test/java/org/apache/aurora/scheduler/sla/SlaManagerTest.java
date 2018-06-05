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
package org.apache.aurora.scheduler.sla;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.CoordinatorSlaPolicy;
import org.apache.aurora.gen.CountSlaPolicy;
import org.apache.aurora.gen.PercentageSlaPolicy;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.SlaPolicy;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ISlaPolicy;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.base.TaskTestUtil.TIER_MANAGER;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class SlaManagerTest extends EasyMockTest {

  private static final Logger LOG = LoggerFactory.getLogger(SlaManagerTest.class);
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String STATS_URL_PREFIX = "fake_url";
  private static final String HOST_A = "a";

  private static final ISlaPolicy PERCENTAGE_SLA_POLICY = ISlaPolicy.build(
      SlaPolicy.percentageSlaPolicy(
          new PercentageSlaPolicy()
              .setPercentage(66)
              .setDurationSecs(1800)));

  private static final ISlaPolicy COUNT_SLA_POLICY = ISlaPolicy.build(
      SlaPolicy.countSlaPolicy(
          new CountSlaPolicy()
              .setCount(2)
              .setDurationSecs(1800)));

  private AsyncHttpClient httpClient;
  private SlaManager slaManager;
  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private IServerInfo serverInfo;
  private Server jettyServer;
  private CountDownLatch coordinatorResponded;

  @Before
  public void setUp() {
    jettyServer = new Server(0); // Start Jetty server with ephemeral port
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);
    httpClient = new DefaultAsyncHttpClient();
    coordinatorResponded = new CountDownLatch(1);

    serverInfo = IServerInfo.build(
        new ServerInfo()
            .setClusterName(CLUSTER_NAME)
            .setStatsUrlPrefix(STATS_URL_PREFIX));

    Injector injector = Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Storage.class).toInstance(storageUtil.storage);
            bind(StateManager.class).toInstance(stateManager);
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(TierManager.class).toInstance(TIER_MANAGER);
            bind(AsyncHttpClient.class)
                .annotatedWith(SlaManager.HttpClient.class)
                .toInstance(httpClient);

            bind(new TypeLiteral<Integer>() { })
                .annotatedWith(SlaManager.MinRequiredInstances.class)
                .toInstance(2);

            bind(new TypeLiteral<Integer>() { })
                .annotatedWith(SlaManager.MaxParallelCoordinators.class)
                .toInstance(10);

            bind(ScheduledExecutorService.class)
                .annotatedWith(SlaManager.SlaManagerExecutor.class)
                .toInstance(AsyncUtil.loggingScheduledExecutor(
                    10, "SlaManagerTest-%d", LOG));

            bind(IServerInfo.class).toInstance(serverInfo);
          }
        }
    );
    slaManager = injector.getInstance(SlaManager.class);

    addTearDown(() -> jettyServer.stop());
  }

  private static IScheduledTask makeTask(
      String taskId,
      int instanceId,
      ScheduleStatus status) {
    return makeTask(taskId, instanceId, status, 1000, true);
  }

  private static IScheduledTask makeTask(
      String taskId,
      int instanceId,
      long runningSince) {
    return makeTask(taskId, instanceId, RUNNING, runningSince, true);
  }

  private static IScheduledTask makeTask(
      String taskId,
      int instanceId,
      ScheduleStatus status,
      long runningSince,
      boolean prod) {
    ScheduledTask builder = TaskTestUtil.addStateTransition(
        TaskTestUtil.makeTask(taskId, TaskTestUtil.JOB, instanceId, prod),
        status,
        runningSince).newBuilder();
    builder.getAssignedTask().setSlaveHost(SlaManagerTest.HOST_A);
    return IScheduledTask.build(builder);
  }

  /**
   * Verifies that SLA check passes and the supplied {@link Storage.MutateWork} gets executed
   * for a job that has {@link CountSlaPolicy#count} + 1 tasks that have been RUNNING for
   * the required {@link CountSlaPolicy#durationSecs} with {@link CountSlaPolicy#count} set to 2.
   */
  @Test
  public void testCheckCountSlaPassesThenActs() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, RUNNING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        COUNT_SLA_POLICY,
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check gets overridden, passes and the supplied {@link Storage.MutateWork} is
   * executed for a job that has and aggressive {@link CountSlaPolicy#count} > total number of
   * instances.
   */
  @Test
  public void testCheckCountSlaOverridesAggressiveCountPassesThenActs() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, RUNNING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        ISlaPolicy.build(
            SlaPolicy.countSlaPolicy(
                new CountSlaPolicy()
                    .setCount(3)
                    .setDurationSecs(1800))),
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check gets overridden, fails and the supplied {@link Storage.MutateWork} is
   * not executed for a job that has and aggressive {@link CountSlaPolicy#count} > total number of
   * instances.
   */
  @Test
  public void testCheckCountSlaOverridesAggressiveCountFailsAct() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, PENDING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        ISlaPolicy.build(
            SlaPolicy.countSlaPolicy(
                new CountSlaPolicy()
                    .setCount(3)
                    .setDurationSecs(1800))),
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check fails and the supplied {@link Storage.MutateWork} does not get
   * executed for a job that has {@link CountSlaPolicy#count} tasks that have been RUNNING for
   * the required {@link CountSlaPolicy#durationSecs} and 1 task in PENDING with
   * {@link CountSlaPolicy#count} set to 2.
   */
  @Test
  public void testCheckCountSlaFailsDoesNotAct() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, PENDING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        COUNT_SLA_POLICY,
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check is skipped and the supplied {@link Storage.MutateWork} gets
   * executed for a job that does not meet {@link SlaManager#minRequiredInstances}.
   */
  @Test
  public void testCheckCountSlaFailsDueToMinRequiredInstancesActs() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1));

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        ISlaPolicy.build(
            SlaPolicy.countSlaPolicy(
                new CountSlaPolicy()
                    .setCount(0)
                    .setDurationSecs(0))),
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check is skipped and the supplied {@link Storage.MutateWork} gets
   * executed for a job is not the correct tier.
   */
  @Test
  public void testCheckCountSlaFailsDueToTierActs() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING, 1000, false);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1));

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        ISlaPolicy.build(
            SlaPolicy.countSlaPolicy(
                new CountSlaPolicy()
                    .setCount(0)
                    .setDurationSecs(0))),
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check fails and the supplied {@link Storage.MutateWork} does not get
   * executed for a job that has {@link CountSlaPolicy#count} tasks that have been RUNNING for
   * the required {@link CountSlaPolicy#durationSecs} and 1 task in RUNNING for less than
   * {@link CountSlaPolicy#durationSecs} with {@link CountSlaPolicy#count} set to 2.
   */
  @Test
  public void testCheckCountSlaFailsDueToRunningTimeDoesNotAct() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, System.currentTimeMillis());

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        COUNT_SLA_POLICY,
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that when {@code force} is {@code True} supplied {@link Storage.MutateWork} gets
   * executed for a job that has 1 task in RUNNING for less than {@link CountSlaPolicy#durationSecs}
   * with {@link CountSlaPolicy#count} set to 2, without checking SLA.
   */
  @Test
  public void testCheckCountSlaCheckForceAct() {
    IScheduledTask task1 = makeTask("taskA", 1, System.currentTimeMillis());

    // expect that the fetchTask is the work is called after force
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        COUNT_SLA_POLICY,
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        true);
  }

  /**
   * Verifies that SLA check passes and the supplied {@link Storage.MutateWork} gets executed
   * for a job that has more {@link PercentageSlaPolicy#percentage} tasks that have been RUNNING for
   * the required {@link PercentageSlaPolicy#durationSecs} with
   * {@link PercentageSlaPolicy#percentage} set to 66.
   */
  @Test
  public void testCheckPercentageSlaPassesThenActs() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, RUNNING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        PERCENTAGE_SLA_POLICY,
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check overridden and the supplied {@link Storage.MutateWork} gets executed
   * for a job that requires aggressive {@link PercentageSlaPolicy#percentage} tasks to be RUNNING
   * for the required {@link PercentageSlaPolicy#durationSecs} with
   * {@link PercentageSlaPolicy#percentage} set to 67.
   */
  @Test
  public void testCheckPercentageSlaWithAggressivePercentagePassesThenActs() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, RUNNING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    // We set the required running percentage to 67 which will allow 0 instances to be in
    // maintenance is overridden to allow exactly 1 instance to be in maintenance.
    slaManager.checkSlaThenAct(
        task1,
        ISlaPolicy.build(
            SlaPolicy.percentageSlaPolicy(
                new PercentageSlaPolicy()
                    .setPercentage(67)
                    .setDurationSecs(1800))),
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check is overridden and the supplied {@link Storage.MutateWork} gets
   * executed for a job that requires aggressive {@link PercentageSlaPolicy#percentage} tasks to
   * be RUNNING for the required {@link PercentageSlaPolicy#durationSecs} with
   * {@link PercentageSlaPolicy#percentage} set to 67.
   */
  @Test
  public void testCheckPercentageSlaWithAggressivePercentageFailsAct() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, PENDING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    control.replay();

    // We set the required running percentage to 67 which will allow 0 instances to be in
    // maintenance is not overridden to allow 1 instance to be in maintenance.
    slaManager.checkSlaThenAct(
        task1,
        ISlaPolicy.build(
            SlaPolicy.percentageSlaPolicy(
                new PercentageSlaPolicy()
                    .setPercentage(67)
                    .setDurationSecs(1800))),
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check fails and the supplied {@link Storage.MutateWork} does not get
   * executed for a job that has {@link PercentageSlaPolicy#percentage} tasks that have been
   * RUNNING for the required {@link PercentageSlaPolicy#durationSecs} and 1 task in PENDING with
   * {@link PercentageSlaPolicy#percentage} set to 66.
   */
  @Test
  public void testCheckPercentageSlaFailsDoesNotAct() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, PENDING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        PERCENTAGE_SLA_POLICY,
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check is skipped and the supplied {@link Storage.MutateWork} gets
   * executed for a job that does not meet {@link SlaManager#minRequiredInstances}.
   */
  @Test
  public void testCheckPercentageSlaFailsDueToMinRequiredInstancesActs() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1));

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        ISlaPolicy.build(
            SlaPolicy.percentageSlaPolicy(
                new PercentageSlaPolicy()
                    .setPercentage(0)
                    .setDurationSecs(0))),
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check is skipped and the supplied {@link Storage.MutateWork} gets
   * executed for a job is not correct tier.
   */
  @Test
  public void testCheckPercentageSlaFailsDueToTierActs() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING, 1000, false);

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1));

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        ISlaPolicy.build(
            SlaPolicy.percentageSlaPolicy(
                new PercentageSlaPolicy()
                    .setPercentage(0)
                    .setDurationSecs(0))),
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that SLA check fails and the supplied {@link Storage.MutateWork} does not get
   * executed for a job that has {@link PercentageSlaPolicy#percentage} tasks that have been
   * RUNNING for the required {@link PercentageSlaPolicy#durationSecs} and 1 task in RUNNING for
   * less than {@link PercentageSlaPolicy#durationSecs} with {@link PercentageSlaPolicy#percentage}
   * set to 66.
   */
  @Test
  public void testCheckPercentageSlaFailsDueToRunningTimeDoesNotAct() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);
    IScheduledTask task3 = makeTask("taskC", 3, System.currentTimeMillis());

    // mock calls to fetch all active tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).active()))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    // mock calls to fetch all RUNNING tasks for the job for sla calculation
    expect(storageUtil.taskStore.fetchTasks(Query.jobScoped(Tasks.getJob(task1)).byStatus(RUNNING)))
        .andReturn(ImmutableSet.of(task1, task2, task3));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        PERCENTAGE_SLA_POLICY,
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        false);
  }

  /**
   * Verifies that when {@code force} is {@code True} the supplied {@link Storage.MutateWork} gets
   * executed for a job that has 1 task in RUNNING for less than
   * {@link PercentageSlaPolicy#durationSecs}
   * with {@link PercentageSlaPolicy#percentage} set to 66, without checking SLA.
   */
  @Test
  public void testCheckPercentageSlaCheckForceAct() {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);

    // expect that the fetchTask is the work is called after force
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        PERCENTAGE_SLA_POLICY,
        storeProvider -> storeProvider
            .getUnsafeTaskStore()
            .fetchTask(task1.getAssignedTask().getTaskId()),
        true);
  }

  /**
   * Verifies that SLA check passes and the supplied {@link Storage.MutateWork} gets executed
   * for a job when {@link CoordinatorSlaPolicy#coordinatorUrl} responds
   * with {@code {"drain": true}}.
   */
  @Test
  public void testCheckCoordinatorSlaPassesThenActs() throws Exception {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    CountDownLatch workCalled = new CountDownLatch(1);

    jettyServer.setHandler(mockCoordinatorResponse(task1, "{\"drain\": true}"));
    jettyServer.start();

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    while (!coordinatorResponded.await(100, TimeUnit.MILLISECONDS)) {
      slaManager.checkSlaThenAct(
          task1,
          createCoordinatorSlaPolicy(),
          storeProvider -> {
            // set the marker to indicate that we performed the work
            workCalled.countDown();
            storeProvider
                .getUnsafeTaskStore()
                .fetchTask(task1.getAssignedTask().getTaskId());
            return null;
          },
          false);
    }

    // wait until we are sure that the server has responded
    coordinatorResponded.await();
    workCalled.await();

    assertEquals(0, coordinatorResponded.getCount());
    // check the work was called
    assertEquals(0, workCalled.getCount());
  }

  /**
   * Verifies that SLA check passes and the supplied {@link Storage.MutateWork} gets executed
   * for a job when {@link CoordinatorSlaPolicy#coordinatorUrl} responds
   * with {@code {"drain": true}}.
   */
  @Test
  public void testCheckCoordinatorSlaPassesThenActsCustomStatusKey() throws Exception {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    CountDownLatch workCalled = new CountDownLatch(1);

    jettyServer.setHandler(mockCoordinatorResponse(task1, "{\"custom-key\": true}"));
    jettyServer.start();

    // expect that the fetchTask in the work is called, after sla check passes
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    while (!coordinatorResponded.await(100, TimeUnit.MILLISECONDS)) {
      slaManager.checkSlaThenAct(
          task1,
          createCoordinatorSlaPolicy("custom-key"),
          storeProvider -> {
            // set the marker to indicate that we performed the work
            workCalled.countDown();
            storeProvider
                .getUnsafeTaskStore()
                .fetchTask(task1.getAssignedTask().getTaskId());
            return null;
          },
          false);
    }

    workCalled.await();

    assertEquals(0, coordinatorResponded.getCount());
    // check the work was called
    assertEquals(0, workCalled.getCount());
  }

  /**
   * Verifies that SLA check passes and the supplied {@link Storage.MutateWork} does not get
   * executed for a job when {@link CoordinatorSlaPolicy#coordinatorUrl} responds
   * with {@code {"drain": false}}.
   */
  @Test
  public void testCheckCoordinatorSlaFailsDoesNotAct() throws Exception {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    CountDownLatch workCalled = new CountDownLatch(1);

    jettyServer.setHandler(mockCoordinatorResponse(task1, "{\"drain\": false}"));
    jettyServer.start();

    control.replay();

    while (!coordinatorResponded.await(100, TimeUnit.MILLISECONDS)) {
      slaManager.checkSlaThenAct(
          task1,
          createCoordinatorSlaPolicy(),
          storeProvider -> {
            // set the marker to indicate that we performed the work
            workCalled.countDown();
            storeProvider
                .getUnsafeTaskStore()
                .fetchTask(task1.getAssignedTask().getTaskId());
            return null;
          },
          false);
    }

    try {
      workCalled.await(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // expected
    }

    assertEquals(0, coordinatorResponded.getCount());
    // check the work was not called
    assertEquals(1, workCalled.getCount());
  }

  /**
   * Verifies that when {@code force} is {@code True} the supplied {@link Storage.MutateWork} gets
   * executed without checking the SLA.
   */
  @Test
  public void testCheckCoordinatorSlaCheckForceAct() throws Exception {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    CountDownLatch workCalled = new CountDownLatch(1);

    jettyServer.setHandler(mockCoordinatorResponse(task1, "{\"drain\": false}"));
    jettyServer.start();

    // expect that the fetchTask in the work is called after force
    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        createCoordinatorSlaPolicy(),
        storeProvider -> {
          // set the marker to indicate that we performed the work
          workCalled.countDown();
          storeProvider
              .getUnsafeTaskStore()
              .fetchTask(task1.getAssignedTask().getTaskId());
          return null;
        },
        true);

    workCalled.await();

    // coordinator is not contacted
    assertEquals(1, coordinatorResponded.getCount());
    // check the work was called
    assertEquals(0, workCalled.getCount());
  }

  /**
   * Verifies that SLA check fails and the supplied {@link Storage.MutateWork} does not get
   * executed for a job when {@link CoordinatorSlaPolicy#coordinatorUrl} responds
   * with error.
   */
  @Test
  public void testCheckCoordinatorSlaErrorDoesNotAct() throws Exception {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    CountDownLatch workCalled = new CountDownLatch(1);

    jettyServer.setHandler(mockCoordinatorError());
    jettyServer.start();

    control.replay();

    while (!coordinatorResponded.await(100, TimeUnit.MILLISECONDS)) {
      slaManager.checkSlaThenAct(
          task1,
          createCoordinatorSlaPolicy(),
          storeProvider -> {
            // set the marker to indicate that we performed the work
            workCalled.countDown();
            storeProvider
                .getUnsafeTaskStore()
                .fetchTask(task1.getAssignedTask().getTaskId());
            return null;
          },
          false);
    }

    try {
      workCalled.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // expected
    }

    assertEquals(0, coordinatorResponded.getCount());
    // check the work was not called
    assertEquals(1, workCalled.getCount());
  }

  /**
   * Verifies that SLA check fails and the supplied {@link Storage.MutateWork} does not get
   * executed for a job when {@link CoordinatorSlaPolicy#coordinatorUrl} throws.
   */
  @Test
  public void testCheckCoordinatorSlaThrowsDoesNotAct() throws Exception {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    CountDownLatch workCalled = new CountDownLatch(1);

    jettyServer.setHandler(mockCoordinatorError());
    jettyServer.start();

    control.replay();

    slaManager.checkSlaThenAct(
        task1,
        ISlaPolicy.build(SlaPolicy.coordinatorSlaPolicy(
            new CoordinatorSlaPolicy()
                .setCoordinatorUrl("http://does.not.exist.com"))),
        storeProvider -> {
          // set the marker to indicate that we performed the work
          workCalled.countDown();
          storeProvider
              .getUnsafeTaskStore()
              .fetchTask(task1.getAssignedTask().getTaskId());
          return null;
        },
        false);

    try {
      workCalled.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // expected
    }

    // check the work was not called
    assertEquals(1, workCalled.getCount());
  }

  /**
   * Start 2 actions simultaneously and make the first one to enter the coordinator lock to block
   * on a semaphore. Only when the semaphore is released will the next action be able acquire the
   * lock. At the end both actions should have completed.
   */
  @Test
  public void testCheckCoordinatorSlaNoLockDoesNotAct() throws Exception {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);

    CountDownLatch blocked1 = new CountDownLatch(1);
    CountDownLatch blocked2 = new CountDownLatch(1);
    CountDownLatch action1 = new CountDownLatch(1);
    CountDownLatch action2 = new CountDownLatch(1);
    CountDownLatch finishAction1 = new CountDownLatch(1);
    CountDownLatch finishAction2 = new CountDownLatch(1);

    jettyServer.setHandler(mockCoordinatorResponses("{\"drain\": true}"));
    jettyServer.start();

    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    expect(storageUtil.taskStore.fetchTask(task2.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task2));

    control.replay();

    // kick off a new thread to attempt starting action1
    new Thread(() -> {
      try {
        // wait until action1 enters the lock
        while (!action1.await(100, TimeUnit.MILLISECONDS)) {
          // start action1
          slaManager.checkSlaThenAct(
              task1,
              createCoordinatorSlaPolicy(),
              storeProvider -> {
                LOG.info("Starting action1 for task:{}", slaManager.getTaskKey(task1));
                // set the marker to indicate that we started the work
                action1.countDown();
                storeProvider
                    .getUnsafeTaskStore()
                    .fetchTask(task1.getAssignedTask().getTaskId());
                // we will block here to make sure that the lock is held
                blocked1.await();
                finishAction1.countDown();
                LOG.info("Finished action1 for task:{}", slaManager.getTaskKey(task1));
                return null;
              },
              false);
        }
      } catch (InterruptedException e) {
        fail();
      }
    }).start();

    // wait for action to ge the lock
    action1.await();

    // kick off a new thread to attempt starting action2
    new Thread(() -> {
      try {
        // wait until action2 enters the lock
        while (!action2.await(100, TimeUnit.MILLISECONDS)) {
          // start action2
          slaManager.checkSlaThenAct(
              task2,
              createCoordinatorSlaPolicy(),
              storeProvider -> {
                LOG.info("Starting action2 for task:{}", slaManager.getTaskKey(task2));
                // set the marker to indicate that we started the work
                action2.countDown();
                storeProvider
                    .getUnsafeTaskStore()
                    .fetchTask(task2.getAssignedTask().getTaskId());
                // we will block here to make sure that the lock is held
                blocked2.await();
                finishAction2.countDown();
                LOG.info("Finished action2 for task:{}", slaManager.getTaskKey(task2));
                return null;
              },
              false);
        }
      } catch (InterruptedException e) {
        fail();
      }
    }).start();

    // both actions should not have entered critical section
    assertNotEquals(action1.getCount(), action2.getCount());
    // action1 should have entered the lock
    assertEquals(0, action1.getCount());
    // action2 should not have entered the lock
    assertEquals(1, action2.getCount());

    // unblock blocked action1
    blocked1.countDown();
    // wait for both of the actions to finish
    finishAction1.await();

    // wait for the second action to enter the lock
    action2.await();

    // action2 should have entered the lock
    assertEquals(0, action2.getCount());

    // unblock blocked action2
    blocked2.countDown();

    finishAction2.await();
  }

  /**
   * Start 2 actions simultaneously with different coordinator urls, so they can both enter the
   * appropriate coordinator locks and try to acquire the semaphore. Both the actions must be able
   * to enter the critical area and start the work simultaneously.
   * At the end only one action would have fully completed the work, since the semaphore is not
   * released.
   */
  @Test
  public void testCheckCoordinatorSlaParallelAct() throws Exception {
    IScheduledTask task1 = makeTask("taskA", 1, RUNNING);
    IScheduledTask task2 = makeTask("taskB", 2, RUNNING);

    Semaphore blocked = new Semaphore(1);
    CountDownLatch action1 = new CountDownLatch(1);
    CountDownLatch action2 = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(2);

    jettyServer.setHandler(mockCoordinatorResponses("{\"drain\": true}"));
    jettyServer.start();

    expect(storageUtil.taskStore.fetchTask(task1.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task1));

    expect(storageUtil.taskStore.fetchTask(task2.getAssignedTask().getTaskId()))
        .andReturn(Optional.of(task2));

    control.replay();

    // kick off a new thread to attempt starting action1
    new Thread(() -> {
      try {
        // wait until action1 enters the lock
        while (!action1.await(100, TimeUnit.MILLISECONDS)) {
          // start action1
          slaManager.checkSlaThenAct(
              task1,
              ISlaPolicy.build(SlaPolicy.coordinatorSlaPolicy(
                  new CoordinatorSlaPolicy()
                      .setCoordinatorUrl(
                          // Note that the url is different although referring to the same server
                          String.format("http://localhost:%d", jettyServer.getURI().getPort())))),
              storeProvider -> {
                LOG.info("Starting action for task:{}", slaManager.getTaskKey(task1));
                // set the marker to indicate that we started the work
                action1.countDown();
                storeProvider
                    .getUnsafeTaskStore()
                    .fetchTask(task1.getAssignedTask().getTaskId());
                // we will block here to make sure that the lock is held
                blocked.acquire();
                finished.countDown();
                LOG.info("Finished action for task:{}", slaManager.getTaskKey(task1));
                return null;
              },
              false);
        }
      } catch (InterruptedException e) {
        fail();
      }
    }).start();

    // kick off a new thread to attempt starting action2
    new Thread(() -> {
      try {
        // wait until action2 enters the lock
        while (!action2.await(100, TimeUnit.MILLISECONDS)) {
          // start action2
          slaManager.checkSlaThenAct(
              task2,
              ISlaPolicy.build(SlaPolicy.coordinatorSlaPolicy(
                  new CoordinatorSlaPolicy()
                      .setCoordinatorUrl(
                          // Note that the url is different although referring to the same server
                          String.format("http://0.0.0.0:%d", jettyServer.getURI().getPort())))),
              storeProvider -> {
                LOG.info("Starting action for task:{}", slaManager.getTaskKey(task2));
                // set the marker to indicate that we started the work
                action2.countDown();
                storeProvider
                    .getUnsafeTaskStore()
                    .fetchTask(task2.getAssignedTask().getTaskId());
                // we will block here to make sure that the lock is held
                blocked.acquire();
                finished.countDown();
                LOG.info("Finished action for task:{}", slaManager.getTaskKey(task2));
                return null;
              },
              false);
        }
      } catch (InterruptedException e) {
        fail();
      }
    }).start();

    // wait for both of the actions to get the lock
    action1.await();
    action2.await();

    assertEquals(0, action1.getCount());
    assertEquals(0, action2.getCount());

    // only 1 action has fully completed
    assertEquals(1, finished.getCount());

    // unblock the blocked action
    blocked.release();

    finished.await();

    // both actions have fully completed
    assertEquals(0, finished.getCount());
  }

  private ISlaPolicy createCoordinatorSlaPolicy() {
    return createCoordinatorSlaPolicy("drain");
  }

  private ISlaPolicy createCoordinatorSlaPolicy(String statusKey) {
    return ISlaPolicy.build(SlaPolicy.coordinatorSlaPolicy(
        new CoordinatorSlaPolicy()
            .setCoordinatorUrl(String.format("http://localhost:%d", jettyServer.getURI().getPort()))
            .setStatusKey(statusKey)
    ));
  }

  private AbstractHandler mockCoordinatorResponse(
      IScheduledTask task,
      String pollResponse) {

    return new AbstractHandler() {
      @Override
      public void handle(
          String target,
          Request baseRequest,
          HttpServletRequest request,
          HttpServletResponse response) throws IOException {
        try {
          String taskKey = slaManager.getTaskKey(task);
          String query = Joiner
              .on("=")
              .join(SlaManager.TASK_PARAM, URLEncoder.encode(taskKey, "UTF-8"));
          String body = new TSerializer(new TSimpleJSONProtocol.Factory())
              .toString(task.newBuilder());
          if (request.getQueryString().equals(query)
              && request.getReader().lines().collect(Collectors.joining()).equals(body)) {
            createResponse(baseRequest, response, pollResponse);
          }
          coordinatorResponded.countDown();
        } catch (TException e) {
          fail();
        }
      }
    };
  }

  private AbstractHandler mockCoordinatorResponses(String mockResponse) {
    return new AbstractHandler() {
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request,
                         HttpServletResponse response) throws IOException {
        createResponse(baseRequest, response, mockResponse);
      }
    };
  }

  private void createResponse(
      Request baseRequest,
      HttpServletResponse response,
      String mockResponse) throws IOException {
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().write(mockResponse);
    response.getWriter().flush();
    baseRequest.setHandled(true);
  }

  private AbstractHandler mockCoordinatorError() {
    return new AbstractHandler() {
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request,
                         HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        baseRequest.setHandled(true);
        coordinatorResponded.countDown();
      }
    };
  }
}
