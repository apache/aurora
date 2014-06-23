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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.auth.CapabilityValidator.Capability;
import org.apache.aurora.auth.SessionValidator;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.SessionKey;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.SchedulerCore;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.thrift.auth.ThriftAuthModule;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.auth.SessionValidator.SessionContext;
import static org.apache.aurora.gen.ResponseCode.AUTH_FAILED;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class ThriftIT extends EasyMockTest {

  private static final String USER = "someuser";
  private static final String ROOT_USER = "blue";
  private static final String PROVISIONER_USER = "red";
  private static final IResourceAggregate QUOTA =
      IResourceAggregate.build(new ResourceAggregate(1, 1, 1));

  private static final Map<Capability, String> CAPABILITIES =
      ImmutableMap.of(Capability.ROOT, ROOT_USER, Capability.PROVISIONER, PROVISIONER_USER);

  private AuroraAdmin.Iface thrift;
  private StorageTestUtil storageTestUtil;
  private SessionContext context;
  private QuotaManager quotaManager;

  private final SessionValidator validator = new SessionValidator() {
    @Override
    public SessionContext checkAuthenticated(
        SessionKey sessionKey,
        Set<String> targetRoles) throws AuthFailedException {

      for (String role : targetRoles) {
        if (!Arrays.equals(role.getBytes(), sessionKey.getData())) {
          throw new AuthFailedException("Injected");
        }
      }

      return context;
    }

    @Override
    public String toString(SessionKey sessionKey) {
      return "test";
    }
  };

  private static class CapabilityValidatorFake implements CapabilityValidator {
    private final SessionValidator validator;

    CapabilityValidatorFake(SessionValidator validator) {
      this.validator = validator;
    }

    @Override
    public SessionContext checkAuthorized(
        SessionKey sessionKey,
        Capability capability,
        AuditCheck check)
        throws AuthFailedException {

      return validator.checkAuthenticated(
          sessionKey,
          ImmutableSet.of(CAPABILITIES.get(capability)));
    }

    @Override
    public SessionContext checkAuthenticated(
        SessionKey sessionKey,
        Set<String> targetRoles)
        throws AuthFailedException {

      return validator.checkAuthenticated(sessionKey, targetRoles);
    }

    @Override
    public String toString(SessionKey sessionKey) {
      return validator.toString(sessionKey);
    }
  }

  @Before
  public void setUp() {
    context = createMock(SessionContext.class);
    quotaManager = createMock(QuotaManager.class);
    createThrift(CAPABILITIES);
  }

  private void createThrift(Map<Capability, String> capabilities) {
    Injector injector = Guice.createInjector(
        new ThriftModule(),
        new ThriftAuthModule(capabilities),
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
            bindMock(SchedulerCore.class);
            bindMock(LockManager.class);
            bindMock(ShutdownRegistry.class);
            bindMock(StateManager.class);
            storageTestUtil = new StorageTestUtil(ThriftIT.this);
            bind(Storage.class).toInstance(storageTestUtil.storage);
            bind(NonVolatileStorage.class).toInstance(storageTestUtil.storage);
            bindMock(StorageBackup.class);
            bind(QuotaManager.class).toInstance(quotaManager);
            bind(SessionValidator.class).toInstance(validator);
            bind(CapabilityValidator.class).toInstance(new CapabilityValidatorFake(validator));
            bind(IServerInfo.class).toInstance(IServerInfo.build(new ServerInfo()));
            bindMock(CronPredictor.class);
          }
        }
    );
    thrift = injector.getInstance(AuroraAdmin.Iface.class);
  }

  private void setQuota(String user, boolean allowed) throws Exception {
    assertEquals(
        allowed ? OK : AUTH_FAILED,
        thrift.setQuota(USER, QUOTA.newBuilder(), new SessionKey().setData(user.getBytes()))
            .getResponseCode());
  }

  @Test
  public void testProvisionAccess() throws Exception {
    quotaManager.saveQuota(USER, QUOTA);
    expectLastCall().times(2);

    control.replay();

    setQuota(ROOT_USER, true);
    setQuota(PROVISIONER_USER, true);
    setQuota("cletus", false);
  }
}
