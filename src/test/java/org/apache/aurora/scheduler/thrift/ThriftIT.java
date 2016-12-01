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
package org.apache.aurora.scheduler.thrift;

import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;

import org.apache.aurora.common.application.ShutdownStage;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.Container._Fields;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.app.AppModule;
import org.apache.aurora.scheduler.app.LifecycleModule;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.aurora.scheduler.app.local.FakeNonVolatileStorage;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.ConfigurationManagerSettings;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.cron.quartz.CronModule;
import org.apache.aurora.scheduler.mesos.DriverFactory;
import org.apache.aurora.scheduler.mesos.DriverSettings;
import org.apache.aurora.scheduler.mesos.TestExecutorSettings;
import org.apache.aurora.scheduler.quota.QuotaModule;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.stats.StatsModule;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.shiro.subject.Subject;
import org.junit.Test;

import static org.apache.aurora.gen.ResponseCode.OK;
import static org.junit.Assert.assertEquals;

public class ThriftIT extends EasyMockTest {

  private static final String USER = "someuser";
  private static final IResourceAggregate QUOTA = ResourceTestUtil.aggregate(1, 1, 1);
  private static final IServerInfo SERVER_INFO = IServerInfo.build(new ServerInfo());

  private AuroraAdmin.Iface thrift;

  private void createThrift(ConfigurationManagerSettings configurationManagerSettings) {
    Injector injector = Guice.createInjector(
        new ThriftModule(),
        new AbstractModule() {
          private <T> T bindMock(Class<T> clazz) {
            T mock = createMock(clazz);
            bind(clazz).toInstance(mock);
            return mock;
          }

          @Override
          protected void configure() {
            install(new LifecycleModule());
            install(new StatsModule());
            install(DbModule.testModule());
            install(new QuotaModule());
            install(new CronModule());
            install(new TierModule(TaskTestUtil.TIER_CONFIG));
            bind(ExecutorSettings.class).toInstance(TestExecutorSettings.THERMOS_EXECUTOR);

            install(new AppModule(configurationManagerSettings));

            bind(NonVolatileStorage.class).to(FakeNonVolatileStorage.class);

            ServiceGroupMonitor schedulers = createMock(ServiceGroupMonitor.class);
            bind(ServiceGroupMonitor.class).toInstance(schedulers);

            bindMock(DriverFactory.class);
            bind(DriverSettings.class).toInstance(new DriverSettings(
                "fakemaster",
                com.google.common.base.Optional.absent(),
                FrameworkInfo.newBuilder()
                    .setUser("framework user")
                    .setName("test framework")
                    .build()));
            bindMock(Recovery.class);
            bindMock(StorageBackup.class);
            bind(IServerInfo.class).toInstance(SERVER_INFO);
          }

          @Provides
          Optional<Subject> provideSubject() {
            return Optional.of(createMock(Subject.class));
          }
        }
    );

    Command shutdownCommand =
        injector.getInstance(Key.get(Command.class, ShutdownStage.class));
    addTearDown(shutdownCommand::execute);

    thrift = injector.getInstance(AnnotatedAuroraAdmin.class);
    Storage storage = injector.getInstance(Storage.class);
    storage.prepare();
  }

  @Test
  public void testSetQuota() throws Exception {
    createThrift(TaskTestUtil.CONFIGURATION_MANAGER_SETTINGS);

    control.replay();

    assertEquals(
        OK,
        thrift.setQuota(USER, QUOTA.newBuilder()).getResponseCode());

    assertEquals(
        QUOTA.newBuilder(),
        thrift.getQuota(USER).getResult().getGetQuotaResult().getQuota());
  }

  @Test
  public void testSubmitNoExecutorDockerTask() throws Exception {
    ConfigurationManagerSettings configurationManagerSettings = new ConfigurationManagerSettings(
        ImmutableSet.of(_Fields.DOCKER),
        true,
        ImmutableMultimap.of(),
        false,
        true,
        true,
        false);

    createThrift(configurationManagerSettings);

    control.replay();

    TaskConfig task = TaskTestUtil.makeConfig(TaskTestUtil.JOB).newBuilder();
    task.unsetExecutorConfig();
    task.setProduction(false)
        .setTier(TaskTestUtil.DEV_TIER_NAME)
        .setContainer(Container.docker(new DockerContainer()
            .setImage("image")
            .setParameters(
                ImmutableList.of(new DockerParameter("a", "b"), new DockerParameter("c", "d")))));
    JobConfiguration job = new JobConfiguration()
        .setKey(task.getJob())
        .setTaskConfig(task)
        .setInstanceCount(1);

    assertEquals(OK, thrift.createJob(job).getResponseCode());
    ScheduledTask scheduledTask = Iterables.getOnlyElement(
        thrift.getTasksStatus(new TaskQuery()).getResult().getScheduleStatusResult().getTasks());
    assertEquals(ScheduleStatus.PENDING, scheduledTask.getStatus());
    assertEquals(task, scheduledTask.getAssignedTask().getTask());
  }
}
