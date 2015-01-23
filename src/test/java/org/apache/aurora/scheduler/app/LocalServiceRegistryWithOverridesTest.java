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
package org.apache.aurora.scheduler.app;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.twitter.common.application.modules.LocalServiceRegistry;
import com.twitter.common.net.InetSocketAddressHelper;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class LocalServiceRegistryWithOverridesTest extends EasyMockTest {
  private InetSocketAddress localHost;
  public LocalServiceRegistryWithOverridesTest() throws UnknownHostException {
    this.localHost = InetSocketAddressHelper.getLocalAddress(8081);
  }

  private LocalServiceRegistryWithOverrides createMockServiceRegistry(
      Optional<String> dnsOverride) throws Exception {
    LocalServiceRegistry serviceRegistry = control.createMock(LocalServiceRegistry.class);
    expect(serviceRegistry.getAuxiliarySockets()).andReturn(
        ImmutableMap.of("http", InetSocketAddressHelper.getLocalAddress(8081)));

    control.replay();
    return new LocalServiceRegistryWithOverrides(
        serviceRegistry,
        new LocalServiceRegistryWithOverrides.Settings(dnsOverride));
  }

  @Test
  public void testNoOverride() throws Exception {
    LocalServiceRegistryWithOverrides registry = createMockServiceRegistry(
        Optional.<String>absent());
    InetSocketAddress addr = registry.getAuxiliarySockets().get("http");
    assertEquals(addr.getHostString(), localHost.getHostString());
  }

  @Test
  public void testOverride() throws Exception {
    LocalServiceRegistryWithOverrides registry = createMockServiceRegistry(
        Optional.of("www.google.com"));
    InetSocketAddress addr = registry.getAuxiliarySockets().get("http");
    assertEquals(addr.getHostString(), "www.google.com");
  }
}
