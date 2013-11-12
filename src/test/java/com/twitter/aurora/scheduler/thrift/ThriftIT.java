/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler.thrift;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.auth.CapabilityValidator;
import com.twitter.aurora.auth.CapabilityValidator.Capability;
import com.twitter.aurora.auth.SessionValidator;
import com.twitter.aurora.gen.AuroraAdmin;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.scheduler.cron.CronScheduler;
import com.twitter.aurora.scheduler.state.LockManager;
import com.twitter.aurora.scheduler.state.MaintenanceController;
import com.twitter.aurora.scheduler.state.SchedulerCore;
import com.twitter.aurora.scheduler.state.StateManager;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.backup.Recovery;
import com.twitter.aurora.scheduler.storage.backup.StorageBackup;
import com.twitter.aurora.scheduler.storage.entities.IQuota;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.aurora.scheduler.thrift.auth.ThriftAuthModule;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

import static com.twitter.aurora.auth.SessionValidator.SessionContext;
import static com.twitter.aurora.gen.ResponseCode.AUTH_FAILED;
import static com.twitter.aurora.gen.ResponseCode.OK;

public class ThriftIT extends EasyMockTest {

  private static final String USER = "someuser";
  private static final String ROOT_USER = "blue";
  private static final String PROVISIONER_USER = "red";
  private static final IQuota QUOTA = IQuota.build(new Quota(1, 1, 1));

  private static final Map<Capability, String> CAPABILITIES =
      ImmutableMap.of(Capability.ROOT, ROOT_USER, Capability.PROVISIONER, PROVISIONER_USER);

  private AuroraAdmin.Iface thrift;
  private StorageTestUtil storageTestUtil;
  private SessionContext context;

  private final SessionValidator validator = new SessionValidator() {
    @Override public SessionContext checkAuthenticated(
        SessionKey sessionKey,
        Set<String> targetRoles) throws AuthFailedException {

      for (String role : targetRoles) {
        if (!Arrays.equals(role.getBytes(), sessionKey.getData())) {
          throw new AuthFailedException("Injected");
        }
      }

      return context;
    }

    @Override public String toString(SessionKey sessionKey) {
      return "test";
    }
  };

  private class CapabilityValidatorFake implements CapabilityValidator {
    private final SessionValidator validator;

    CapabilityValidatorFake(SessionValidator validator) {
      this.validator = validator;
    }

    @Override public SessionContext checkAuthorized(
        SessionKey sessionKey,
        Capability capability,
        AuditCheck check)
        throws AuthFailedException {

      return validator.checkAuthenticated(
          sessionKey,
          ImmutableSet.of(CAPABILITIES.get(capability)));
    }

    @Override public SessionContext checkAuthenticated(
        SessionKey sessionKey,
        Set<String> targetRoles)
        throws AuthFailedException {

      return validator.checkAuthenticated(sessionKey, targetRoles);
    }

    @Override public String toString(SessionKey sessionKey) {
      return validator.toString(sessionKey);
    }
  }

  @Before
  public void setUp() {
    context = createMock(SessionContext.class);
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

          @Override protected void configure() {
            bindMock(CronScheduler.class);
            bindMock(MaintenanceController.class);
            bindMock(Recovery.class);
            bindMock(SchedulerCore.class);
            bindMock(LockManager.class);
            bindMock(ShutdownRegistry.class);
            bindMock(StateManager.class);
            storageTestUtil = new StorageTestUtil(ThriftIT.this);
            bind(Storage.class).toInstance(storageTestUtil.storage);
            bindMock(StorageBackup.class);
            bindMock(ThriftConfiguration.class);
            bind(SessionValidator.class).toInstance(validator);
            bind(CapabilityValidator.class).toInstance(new CapabilityValidatorFake(validator));
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
    storageTestUtil.expectOperations();
    storageTestUtil.quotaStore.saveQuota(USER, QUOTA);
    expectLastCall().times(2);

    control.replay();

    setQuota(ROOT_USER, true);
    setQuota(PROVISIONER_USER, true);
    setQuota("cletus", false);
  }
}
