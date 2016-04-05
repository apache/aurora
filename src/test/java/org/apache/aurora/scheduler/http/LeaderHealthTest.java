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

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import org.apache.aurora.common.net.pool.DynamicHostSet.MonitorException;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.http.LeaderRedirect.LeaderStatus;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class LeaderHealthTest extends EasyMockTest {

  private LeaderRedirect leaderRedirect;
  private LeaderHealth leaderHealth;

  @Before
  public void setUp() throws MonitorException {
    leaderRedirect = createMock(LeaderRedirect.class);
    leaderHealth = new LeaderHealth(leaderRedirect);
  }

  private void expectResponse(LeaderStatus status, int responseCode) {
    expect(leaderRedirect.getLeaderStatus()).andReturn(status);

    control.replay();

    Response response = leaderHealth.get();
    assertEquals(responseCode, response.getStatus());
    assertEquals(LeaderHealth.getHelpText(status), response.getEntity().toString());
  }

  @Test
  public void testLeader() throws Exception {
    expectResponse(LeaderStatus.LEADING, HttpServletResponse.SC_OK);
  }

  @Test
  public void testNotLeader() throws Exception {
    expectResponse(LeaderStatus.NOT_LEADING, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
  }

  @Test
  public void testNoLeaders() throws Exception {
    expectResponse(LeaderStatus.NO_LEADER, HttpServletResponse.SC_BAD_GATEWAY);
  }
}
