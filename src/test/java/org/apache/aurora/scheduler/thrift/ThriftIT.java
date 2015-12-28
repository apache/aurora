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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ResponseCode.OK;
import static org.junit.Assert.assertEquals;

public class ThriftIT extends EasyMockTest {

  private static final String USER = "someuser";
  private static final IResourceAggregate QUOTA =
      IResourceAggregate.build(new ResourceAggregate(1, 1, 1));

  private AuroraAdmin.Iface thrift;
  private StorageTestUtil storageTestUtil;
  private QuotaManager quotaManager;

  @Before
  public void setUp() {
    quotaManager = createMock(QuotaManager.class);
    createThrift();
  }

  private void createThrift() {
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
            bindMock(CronJobManager.class);
            bindMock(MaintenanceController.class);
            bindMock(Recovery.class);
            bindMock(LockManager.class);
            bindMock(ShutdownRegistry.class);
            bindMock(StateManager.class);
            bindMock(TaskIdGenerator.class);
            bindMock(UUIDGenerator.class);
            bindMock(JobUpdateController.class);
            bind(ConfigurationManager.class).toInstance(TaskTestUtil.CONFIGURATION_MANAGER);
            bind(Thresholds.class).toInstance(new Thresholds(1000, 2000));
            storageTestUtil = new StorageTestUtil(ThriftIT.this);
            bind(Storage.class).toInstance(storageTestUtil.storage);
            bind(NonVolatileStorage.class).toInstance(storageTestUtil.storage);
            bindMock(StorageBackup.class);
            bind(QuotaManager.class).toInstance(quotaManager);
            bind(IServerInfo.class).toInstance(IServerInfo.build(new ServerInfo()));
            bindMock(CronPredictor.class);
          }

          @Provides
          Optional<Subject> provideSubject() {
            return Optional.of(createMock(Subject.class));
          }
        }
    );
    thrift = injector.getInstance(AnnotatedAuroraAdmin.class);
  }

  @Test
  public void testSetQuota() throws Exception {
    storageTestUtil.expectOperations();
    quotaManager.saveQuota(
        USER,
        QUOTA,
        storageTestUtil.mutableStoreProvider);

    control.replay();

    assertEquals(
        OK,
        thrift.setQuota(USER, QUOTA.newBuilder()).getResponseCode());
  }
}
