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

import javax.ws.rs.core.HttpHeaders;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.TearDown;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.twitter.common.application.StartupRegistry;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.application.modules.LocalServiceRegistry;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.base.Command;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.scheduler.async.OfferQueue;
import org.apache.aurora.scheduler.async.RescheduleCalculator;
import org.apache.aurora.scheduler.async.TaskScheduler;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.SchedulerCore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static com.sun.jersey.api.client.ClientResponse.Status;
import static com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import static com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor;

import static org.apache.aurora.scheduler.async.TaskGroups.TaskGroupsSettings;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

/**
 * TODO(wfarner): Break apart ServletModule so test setup isn't so involved.
 */
public class ServletModuleTest extends EasyMockTest {

  private StorageTestUtil storage;
  private InetSocketAddress httpServer;
  private Capture<HostChangeMonitor<ServiceInstance>> schedulerWatcher;

  @Before
  public void setUp() throws Exception {
    storage = new StorageTestUtil(this);
    final DynamicHostSet<ServiceInstance> schedulers =
        createMock(new Clazz<DynamicHostSet<ServiceInstance>>() {
        });

    Injector injector = Guice.createInjector(
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
            bindMock(AuroraAdmin.Iface.class);
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

    control.replay();

    final ShutdownRegistryImpl shutdownRegistry = injector.getInstance(ShutdownRegistryImpl.class);
    addTearDown(new TearDown() {
      @Override
      public void tearDown() {
        shutdownRegistry.execute();
      }
    });
    injector.getInstance(StartupRegistry.class).execute();
    LocalServiceRegistry serviceRegistry = injector.getInstance(LocalServiceRegistry.class);
    httpServer = serviceRegistry.getAuxiliarySockets().get("http");
  }

  private ClientResponse get(String path) {
    Client client = Client.create();
    // Disable redirects so we can unit test them.
    client.setFollowRedirects(false);
    WebResource resource = client.resource(
        String.format("http://%s:%d%s", httpServer.getHostName(), httpServer.getPort(), path));
    return resource.getRequestBuilder()
        .header(HttpHeaders.ACCEPT_ENCODING, "gzip")
        .get(ClientResponse.class);
  }

  private void assertContentEncoding(String path, Optional<String> encoding) {
    assertEquals(encoding.orNull(), get(path).getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING));
  }

  private void assertGzipEncoded(String path) {
    assertContentEncoding(path, Optional.of("gzip"));
  }

  @Test
  public void testGzipEncoding() throws Exception {
    assertContentEncoding("/", Optional.<String>absent());
    assertGzipEncoded("/scheduler");
    assertGzipEncoded("/scheduler/");
    assertGzipEncoded("/scheduler/role");
    assertGzipEncoded("/scheduler/role/");
    assertGzipEncoded("/scheduler/role/env/");
    assertGzipEncoded("/scheduler/role/env/job");
    assertGzipEncoded("/scheduler/role/env/job/");
  }

  private void assertResponseStatus(String path, Status expectedStatus) {
    ClientResponse response = get(path);
    assertEquals(expectedStatus.getStatusCode(), response.getStatus());
  }

  private void setLeadingScheduler(String host, int port) {
    ServiceInstance instance = new ServiceInstance()
        .setAdditionalEndpoints(ImmutableMap.of("http", new Endpoint(host, port)));
    schedulerWatcher.getValue().onChange(ImmutableSet.of(instance));
  }

  private void leaderRedirectSmokeTest(Status expectedStatus) {
    assertResponseStatus("/scheduler", expectedStatus);
    assertResponseStatus("/scheduler/", expectedStatus);
    assertResponseStatus("/scheduler/role", expectedStatus);
    assertResponseStatus("/scheduler/role/env", expectedStatus);
    assertResponseStatus("/scheduler/role/env/job", expectedStatus);
  }

  @Test
  public void testLeaderRedirect() throws Exception {
    assertResponseStatus("/", Status.OK);

    // Scheduler is assumed leader at this point, since no members are present in the service
    // (not even this process).
    leaderRedirectSmokeTest(Status.OK);

    // This process is leading
    setLeadingScheduler(httpServer.getHostName(), httpServer.getPort());
    leaderRedirectSmokeTest(Status.OK);

    setLeadingScheduler("otherHost", 1234);
    leaderRedirectSmokeTest(Status.FOUND);
    assertResponseStatus("/", Status.OK);
  }
}
