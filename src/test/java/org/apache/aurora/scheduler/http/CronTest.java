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

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class CronTest extends EasyMockTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private CronJobManager cronJobManager;

  private Cron cron;

  @Before
  public void setUp() {
    cronJobManager = createMock(CronJobManager.class);

    cron = new Cron(cronJobManager);
  }

  @Test
  public void testDumpContents() throws Exception {
    expect(cronJobManager.getScheduledJobs()).andReturn(ImmutableMap.of(
        JobKeys.from("test", "test", "test"), CrontabEntry.parse("* * * * *")));

    control.replay();

    Response response = cron.dumpContents();
    assertEquals(HttpServletResponse.SC_OK, response.getStatus());
    assertEquals(
        "{\"scheduled\":{\"test/test/test\":\"* * * * *\"}}",
        OBJECT_MAPPER.writeValueAsString(response.getEntity()));
  }
}
