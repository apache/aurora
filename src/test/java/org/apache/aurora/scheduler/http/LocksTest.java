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

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocksTest extends EasyMockTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final IJobKey JOB_KEY = JobKeys.from("role", "env", "job");
  private static final ILockKey LOCK_KEY = ILockKey.build(LockKey.job(JOB_KEY.newBuilder()));

  private Locks locks;
  private LockManager lockManager;

  @Before
  public void setUp() {
    lockManager = createMock(LockManager.class);
    locks = new Locks(lockManager);
  }

  @Test
  public void testDumpContents() throws Exception {
    ILock lock = ILock.build(new Lock()
        .setKey(LOCK_KEY.newBuilder())
        .setToken("test token")
        .setMessage("test msg")
        .setUser("test usr")
        .setTimestampMs(325));
    expect(lockManager.getLocks()).andReturn(ImmutableSet.of(lock));

    control.replay();

    Response response = locks.getLocks();
    assertEquals(HttpServletResponse.SC_OK, response.getStatus());

    String result = OBJECT_MAPPER.writeValueAsString(response.getEntity());
    assertTrue(result.contains("role/env/job"));
    assertTrue(result.contains("test token"));
    assertTrue(result.contains("test msg"));
    assertTrue(result.contains("test usr"));
    assertTrue(result.contains("325"));
  }

  @Test
  public void testDumpEmptyContents() throws Exception {
    expect(lockManager.getLocks()).andReturn(ImmutableSet.of());

    control.replay();

    Response response = locks.getLocks();
    assertEquals(HttpServletResponse.SC_OK, response.getStatus());
    assertEquals("{}", OBJECT_MAPPER.writeValueAsString(response.getEntity()));
  }
}
