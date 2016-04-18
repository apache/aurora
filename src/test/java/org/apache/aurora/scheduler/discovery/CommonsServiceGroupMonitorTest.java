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
package org.apache.aurora.scheduler.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.net.pool.DynamicHostSet;
import org.apache.aurora.common.net.pool.DynamicHostSet.HostChangeMonitor;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CommonsServiceGroupMonitorTest extends EasyMockTest {

  private DynamicHostSet<ServiceInstance> serverSet;
  private Capture<HostChangeMonitor<ServiceInstance>> hostChangeMonitorCapture;
  private Command stopCommand;

  @Before
  public void setUp() throws Exception {
    serverSet = createMock(new Clazz<DynamicHostSet<ServiceInstance>>() { });
    hostChangeMonitorCapture = createCapture();
    stopCommand = createMock(Command.class);
  }

  private void expectSuccessfulWatch() throws Exception {
    expect(serverSet.watch(capture(hostChangeMonitorCapture))).andReturn(stopCommand);
  }

  private void expectFailedWatch() throws Exception {
    DynamicHostSet.MonitorException watchError =
        new DynamicHostSet.MonitorException(
            "Problem watching service group",
            new RuntimeException());
    expect(serverSet.watch(capture(hostChangeMonitorCapture))).andThrow(watchError);
  }

  @Test
  public void testNominalLifecycle() throws Exception {
    expectSuccessfulWatch();

    stopCommand.execute();
    expectLastCall();

    control.replay();

    CommonsServiceGroupMonitor groupMonitor = new CommonsServiceGroupMonitor(serverSet);
    groupMonitor.start();
    groupMonitor.close();
  }

  @Test
  public void testExceptionalLifecycle() throws Exception {
    expectFailedWatch();
    control.replay();

    CommonsServiceGroupMonitor groupMonitor = new CommonsServiceGroupMonitor(serverSet);
    try {
      groupMonitor.start();
      fail();
    } catch (ServiceGroupMonitor.MonitorException e) {
      // expected
    }

    // Close on a non-started monitor should be allowed.
    groupMonitor.close();
  }

  @Test
  public void testNoHosts() throws Exception {
    expectSuccessfulWatch();
    control.replay();

    CommonsServiceGroupMonitor groupMonitor = new CommonsServiceGroupMonitor(serverSet);
    assertEquals(ImmutableSet.of(), groupMonitor.get());

    groupMonitor.start();
    assertEquals(ImmutableSet.of(), groupMonitor.get());

    hostChangeMonitorCapture.getValue().onChange(ImmutableSet.of());
    assertEquals(ImmutableSet.of(), groupMonitor.get());
  }

  @Test
  public void testHostUpdates() throws Exception {
    expectSuccessfulWatch();
    control.replay();

    CommonsServiceGroupMonitor groupMonitor = new CommonsServiceGroupMonitor(serverSet);
    groupMonitor.start();

    ImmutableSet<ServiceInstance> twoHosts =
        ImmutableSet.of(serviceInstance("one"), serviceInstance("two"));
    hostChangeMonitorCapture.getValue().onChange(twoHosts);
    assertEquals(twoHosts, groupMonitor.get());

    ImmutableSet<ServiceInstance> oneHost = ImmutableSet.of(serviceInstance("one"));
    hostChangeMonitorCapture.getValue().onChange(oneHost);
    assertEquals(oneHost, groupMonitor.get());

    ImmutableSet<ServiceInstance> anotherHost = ImmutableSet.of(serviceInstance("three"));
    hostChangeMonitorCapture.getValue().onChange(anotherHost);
    assertEquals(anotherHost, groupMonitor.get());

    ImmutableSet<ServiceInstance> noHosts = ImmutableSet.of();
    hostChangeMonitorCapture.getValue().onChange(noHosts);
    assertEquals(noHosts, groupMonitor.get());
  }

  private ServiceInstance serviceInstance(String hostName) {
    return new ServiceInstance(new Endpoint(hostName, 42), ImmutableMap.of(), Status.ALIVE);
  }
}
