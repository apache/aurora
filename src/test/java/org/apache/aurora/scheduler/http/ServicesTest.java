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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class ServicesTest extends EasyMockTest {
  private static final String FAILURE_CAUSE_REASON = "Fake failure";

  private ServiceManagerIface startupServiceManager;
  private ServiceManagerIface activeServicesManager;

  private Services servicesServlet;

  @Before
  public void setUp() {
    activeServicesManager = createMock(ServiceManagerIface.class);
    startupServiceManager = createMock(ServiceManagerIface.class);

    servicesServlet = new Services(activeServicesManager, startupServiceManager);
  }

  @Test
  public void testGetServices() throws Exception {
    Service newService = createMock(Service.class);
    expect(newService.state()).andReturn(State.NEW);

    Service failedService = createMock(Service.class);
    expect(failedService.state()).andReturn(State.FAILED);
    Exception failureCause = new Exception(FAILURE_CAUSE_REASON);
    expect(failedService.failureCause()).andReturn(failureCause);

    Service runningService = createMock(Service.class);
    expect(runningService.state()).andReturn(State.RUNNING);

    expect(startupServiceManager.servicesByState()).andReturn(
        ImmutableMultimap.of(
            State.RUNNING, runningService,
            State.FAILED, failedService));
    expect(activeServicesManager.servicesByState())
        .andReturn(ImmutableMultimap.of(State.NEW, newService));

    control.replay();

    assertEquals(
        ImmutableList.of(
            ImmutableMap.of(
                "name", newService.getClass().getSimpleName(),
                "state", State.NEW),
            ImmutableMap.of(
                "name", failedService.getClass().getSimpleName(),
                "state", State.RUNNING),
            ImmutableMap.of(
                "name", failedService.getClass().getSimpleName(),
                "state", State.FAILED,
                "failureCause", failureCause.toString())),
        servicesServlet.getServices().getEntity());
  }
}
