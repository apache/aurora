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
package org.apache.aurora.scheduler.updater;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver.UpdateAgentReserverImpl;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UpdateAgentReserverImplTest extends EasyMockTest {
  private UpdateAgentReserver reserver;
  private BiCache<IInstanceKey, String> cache;

  private static final String AGENT_ID = "agent";
  private static final IInstanceKey INSTANCE_KEY =
      InstanceKeys.from(JobKeys.from("role", "env", "name"), 1);

  @Before
  public void setUp() {
    cache = createMock(new Clazz<BiCache<IInstanceKey, String>>() { });
    reserver = new UpdateAgentReserverImpl(cache);
  }

  @Test
  public void testReserve() {
    cache.put(INSTANCE_KEY, AGENT_ID);
    expectLastCall();
    control.replay();
    reserver.reserve(AGENT_ID, INSTANCE_KEY);
  }

  @Test
  public void testRelease() {
    cache.remove(INSTANCE_KEY, AGENT_ID);
    expectLastCall();
    control.replay();
    reserver.release(AGENT_ID, INSTANCE_KEY);
  }

  @Test
  public void testIsReserved() {
    expect(cache.getByValue(AGENT_ID)).andReturn(ImmutableSet.of(INSTANCE_KEY));
    expect(cache.getByValue(AGENT_ID)).andReturn(ImmutableSet.of());
    control.replay();
    assertTrue(reserver.isReserved(AGENT_ID));
    assertFalse(reserver.isReserved(AGENT_ID));
  }
}
