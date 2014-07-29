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
package org.apache.aurora.scheduler.http;

import java.net.InetSocketAddress;

import javax.ws.rs.core.MediaType;

import com.google.common.base.Throwables;
import com.google.common.testing.TearDown;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import com.twitter.common.application.StartupRegistry;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.application.modules.LocalServiceRegistry;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.base.Command;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.thrift.ServiceInstance;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.scheduler.async.OfferQueue;
import org.apache.aurora.scheduler.async.RescheduleCalculator;
import org.apache.aurora.scheduler.async.TaskGroups.TaskGroupsSettings;
import org.apache.aurora.scheduler.async.TaskScheduler;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.http.api.GsonMessageBodyHandler;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.SchedulerCore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.junit.Before;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertNotNull;

/**
 * TODO(wfarner): Break apart ServletModule so test setup isn't so involved.
 * TODO(wfarner): Come up with an approach for these tests that doesn't require starting an actual
 * HTTP server for each test case.
 *
 */
public abstract class ServletModuleTest extends EasyMockTest {

  private Injector injector;
  protected StorageTestUtil storage;
  protected InetSocketAddress httpServer;
  protected Capture<HostChangeMonitor<ServiceInstance>> schedulerWatcher;
  protected AuroraAdmin.Iface thrift;

  @Before
  public final void setUpServletModuleTest() throws Exception {
    storage = new StorageTestUtil(this);
    final DynamicHostSet<ServiceInstance> schedulers =
        createMock(new Clazz<DynamicHostSet<ServiceInstance>>() { });

    injector = Guice.createInjector(
        new ServletModule(),
        new LogModule(),
        new StatsModule(),
        new HttpModule(),
        new LifecycleModule(),
        new AbstractModule() {
          <T> T bindMock(Class<T> clazz) {
            T mock = createMock(clazz);
            bind(clazz).toInstance(mock);
            return mock;
          }

          @Override
          protected void configure() {
            bind(Storage.class).toInstance(storage.storage);
            bind(IServerInfo.class).toInstance(IServerInfo.build(new ServerInfo()
                .setClusterName("unittest")
                .setThriftAPIVersion(100)
                .setStatsUrlPrefix("none")));
            bind(TaskGroupsSettings.class).toInstance(
                new TaskGroupsSettings(bindMock(BackoffStrategy.class), RateLimiter.create(1000)));

            bind(new TypeLiteral<DynamicHostSet<ServiceInstance>>() { }).toInstance(schedulers);
            thrift = bindMock(AuroraAdmin.Iface.class);
            bindMock(CronJobManager.class);
            bindMock(LockManager.class);
            bindMock(OfferQueue.class);
            bindMock(QuotaManager.class);
            bindMock(RescheduleCalculator.class);
            bindMock(SchedulerCore.class);
            bindMock(TaskScheduler.class);
            bindMock(Thread.UncaughtExceptionHandler.class);
          }
        });

    schedulerWatcher = createCapture();
    expect(schedulers.watch(capture(schedulerWatcher))).andReturn(createMock(Command.class));
  }

  protected void replayAndStart() {
    control.replay();

    final ShutdownRegistryImpl shutdownRegistry = injector.getInstance(ShutdownRegistryImpl.class);
    addTearDown(new TearDown() {
      @Override
      public void tearDown() {
        shutdownRegistry.execute();
      }
    });
    try {
      injector.getInstance(StartupRegistry.class).execute();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    LocalServiceRegistry serviceRegistry = injector.getInstance(LocalServiceRegistry.class);
    httpServer = serviceRegistry.getAuxiliarySockets().get("http");
  }

  protected String makeUrl(String path) {
    return String.format("http://%s:%s%s", httpServer.getHostName(), httpServer.getPort(), path);
  }

  protected WebResource.Builder getRequestBuilder(String path) {
    assertNotNull("HTTP server must be started first", httpServer);
    ClientConfig config = new DefaultClientConfig();
    config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    config.getClasses().add(GsonMessageBodyHandler.class);
    Client client = Client.create(config);
    // Disable redirects so we can unit test them.
    client.setFollowRedirects(false);
    return client.resource(makeUrl(path)).getRequestBuilder().accept(MediaType.APPLICATION_JSON);
  }
}
