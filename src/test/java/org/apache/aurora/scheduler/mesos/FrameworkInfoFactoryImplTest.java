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
package org.apache.aurora.scheduler.mesos;

import com.google.common.net.HostAndPort;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.http.HttpService;
import org.apache.mesos.v1.Protos;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class FrameworkInfoFactoryImplTest extends EasyMockTest {
  private static final Protos.FrameworkInfo INFO = Protos.FrameworkInfo.newBuilder()
      .setId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
      .setName("name")
      .setFailoverTimeout(100)
      .setUser("user")
      .build();

  private HttpService service;
  private FrameworkInfoFactory factory;

  @Before
  public void setUp() {
    service = createMock(HttpService.class);
    factory = new FrameworkInfoFactory.FrameworkInfoFactoryImpl(INFO, service, "http");
  }

  @Test
  public void testHostnameAndURLAdded() {
    expect(service.getAddress()).andReturn(HostAndPort.fromParts("hostname", 80));

    control.replay();
    Protos.FrameworkInfo result = factory.getFrameworkInfo();

    assertEquals(result.getHostname(), "hostname");
    assertEquals(result.getWebuiUrl(), "http://hostname:80");
  }
}
