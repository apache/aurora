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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletContextListener;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.scheduler.AppStartup;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.app.LifecycleModule;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.aurora.scheduler.async.AsyncModule;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.http.api.GsonMessageBodyHandler;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator;
import org.apache.aurora.scheduler.scheduling.TaskGroups;
import org.apache.aurora.scheduler.scheduling.TaskGroups.TaskGroupsSettings;
import org.apache.aurora.scheduler.scheduling.TaskScheduler;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.stats.StatsModule;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;

import static org.apache.aurora.scheduler.http.JettyServerModule.makeServletContextListener;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertNotNull;

/**
 * TODO(wfarner): Break apart ServletModule so test setup isn't so involved.
 * TODO(wfarner): Come up with an approach for these tests that doesn't require starting an actual
 * HTTP server for each test case.
 *
 */
public abstract class AbstractJettyTest extends EasyMockTest {
  private Injector injector;
  protected StorageTestUtil storage;
  protected HostAndPort httpServer;
  private AtomicReference<ImmutableSet<ServiceInstance>> schedulers;

  /**
   * Subclasses should override with a module that configures the servlets they are testing.
   *
   * @return A module used in the creation of the servlet container's child injector.
   */
  protected Module getChildServletModule() {
    return Modules.EMPTY_MODULE;
  }

  @Before
  public void setUpBase() throws Exception {
    storage = new StorageTestUtil(this);

    ServiceGroupMonitor serviceGroupMonitor = createMock(ServiceGroupMonitor.class);

    injector = Guice.createInjector(
        new StatsModule(),
        new LifecycleModule(),
        new SchedulerServicesModule(),
        new AsyncModule(),
        new AbstractModule() {
          <T> T bindMock(Class<T> clazz) {
            T mock = createMock(clazz);
            bind(clazz).toInstance(mock);
            return mock;
          }

          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(Storage.class).toInstance(storage.storage);
            bind(IServerInfo.class).toInstance(IServerInfo.build(new ServerInfo()
                .setClusterName("unittest")
                .setStatsUrlPrefix("none")));
            bind(TaskGroupsSettings.class).toInstance(
                new TaskGroupsSettings(
                    Amount.of(1L, Time.MILLISECONDS),
                    bindMock(BackoffStrategy.class),
                    RateLimiter.create(1000),
                    5));
            bind(ServiceGroupMonitor.class).toInstance(serviceGroupMonitor);
            bindMock(CronJobManager.class);
            bindMock(LockManager.class);
            bindMock(OfferManager.class);
            bindMock(RescheduleCalculator.class);
            bindMock(TaskScheduler.class);
            bindMock(TierManager.class);
            bindMock(Thread.UncaughtExceptionHandler.class);
            bindMock(TaskGroups.TaskGroupBatchWorker.class);

            bind(ServletContextListener.class).toProvider(() -> {
              return makeServletContextListener(injector, getChildServletModule());
            });
          }
        },
        new JettyServerModule(false));

    schedulers = new AtomicReference<>(ImmutableSet.of());

    serviceGroupMonitor.start();
    expectLastCall();

    expect(serviceGroupMonitor.get()).andAnswer(schedulers::get).anyTimes();
  }

  protected void setLeadingScheduler(String host, int port) {
    schedulers.set(
        ImmutableSet.of(new ServiceInstance().setServiceEndpoint(new Endpoint(host, port))));
  }

  protected void unsetLeadingSchduler() {
    schedulers.set(ImmutableSet.of());
  }

  protected void replayAndStart() {
    control.replay();
    try {
      ServiceManagerIface service =
          injector.getInstance(Key.get(ServiceManagerIface.class, AppStartup.class));
      service.startAsync().awaitHealthy();
      addTearDown(() -> {
        service.stopAsync().awaitStopped(5L, TimeUnit.SECONDS);
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    httpServer = injector.getInstance(HttpService.class).getAddress();

    // By default we'll set this instance to be the leader.
    setLeadingScheduler(httpServer.getHostText(), httpServer.getPort());
  }

  protected String makeUrl(String path) {
    return String.format("http://%s:%s%s", httpServer.getHostText(), httpServer.getPort(), path);
  }

  protected WebResource.Builder getPlainRequestBuilder(String path) {
    assertNotNull("HTTP server must be started first", httpServer);
    Client client = Client.create(new DefaultClientConfig());
    return client.resource(makeUrl(path)).getRequestBuilder();
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
