package com.twitter.aurora.scheduler.thrift;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.auth.SessionValidator;
import com.twitter.aurora.gen.AuroraAdmin;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.scheduler.cron.CronScheduler;
import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.state.MaintenanceController;
import com.twitter.aurora.scheduler.state.SchedulerCore;
import com.twitter.aurora.scheduler.state.StateManager;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.backup.Recovery;
import com.twitter.aurora.scheduler.storage.backup.StorageBackup;
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator.Capability;
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
  private static final Quota QUOTA = new Quota().setNumCpus(1).setRamMb(1).setDiskMb(1);

  private static final Map<Capability, String> CAPABILITIES =
      ImmutableMap.of(Capability.ROOT, ROOT_USER, Capability.PROVISIONER, PROVISIONER_USER);

  private AuroraAdmin.Iface thrift;
  private QuotaManager quotaManager;
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
            bindMock(ShutdownRegistry.class);
            bindMock(StateManager.class);
            bindMock(Storage.class);
            bindMock(StorageBackup.class);
            bindMock(ThriftConfiguration.class);
            quotaManager = bindMock(QuotaManager.class);
            bind(SessionValidator.class).toInstance(validator);
          }
        }
    );
    thrift = injector.getInstance(AuroraAdmin.Iface.class);
  }

  private void setQuota(String user, boolean allowed) throws Exception {
    assertEquals(
        allowed ? OK : AUTH_FAILED,
        thrift.setQuota(USER, QUOTA, new SessionKey().setData(user.getBytes())).getResponseCode());
  }

  @Test
  public void testProvisionAccess() throws Exception {
    quotaManager.setQuota(USER, QUOTA);
    expectLastCall().times(2);

    control.replay();

    setQuota(ROOT_USER, true);
    setQuota(PROVISIONER_USER, true);
    setQuota("cletus", false);
  }
}
