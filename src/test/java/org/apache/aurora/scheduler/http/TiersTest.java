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

import java.io.IOException;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import com.google.gson.Gson;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class TiersTest extends EasyMockTest {

  private TierManager tierManager;
  private Tiers tiers;

  @Before
  public void setUp() {
    tierManager = createMock(TierManager.class);
    tiers = new Tiers(tierManager);
  }

  @Test
  public void testGetTiers() throws IOException {
    expect(tierManager.getDefaultTierName()).andReturn("preemptible");
    expect(tierManager.getTiers()).andReturn(TaskTestUtil.tierInfos());

    control.replay();

    Response response = tiers.getTiers();
    assertEquals(HttpServletResponse.SC_OK, response.getStatus());
    String responseAsString = new ObjectMapper().writeValueAsString(response.getEntity());
    assertEquals(toMap(TaskTestUtil.tierConfigFile()), toMap(responseAsString));
  }

  private static Map toMap(String json) {
    return new Gson().fromJson(json, Map.class);
  }
}
