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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver.UpdateAgentReserverImpl;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UpdateAgentReserverImplTest extends EasyMockTest {
  private UpdateAgentReserver reserver;
  private BiCache<IInstanceKey, String> cache;

  private static final String AGENT_ID = "agent";
  private static final IInstanceKey INSTANCE_KEY =
      InstanceKeys.from(JobKeys.from("role", "env", "name"), 1);

  private TaskGroupKey getTaskGroup(IInstanceKey key) {
    return TaskGroupKey.from(ITaskConfig.build(
        new TaskConfig()
            .setJob(key.getJobKey().newBuilder())
            .setNumCpus(1.0)
            .setRamMb(1L)
            .setDiskMb(1L)));
  }

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
  public void testGetReservations() {
    expect(cache.getByValue(AGENT_ID)).andReturn(ImmutableSet.of(INSTANCE_KEY));
    control.replay();
    assertEquals(ImmutableSet.of(INSTANCE_KEY), reserver.getReservations(AGENT_ID));
  }

  @Test
  public void testHasReservations() {
    IInstanceKey instanceKey2 = InstanceKeys.from(JobKeys.from("role", "env", "name"), 2);
    IInstanceKey instanceKey3 = InstanceKeys.from(JobKeys.from("role2", "env2", "name2"), 1);
    expect(cache.asMap())
        .andReturn(ImmutableMap.of(
            INSTANCE_KEY,
            AGENT_ID,
            instanceKey2,
            AGENT_ID,
            instanceKey3,
            "different-agent")).anyTimes();
    control.replay();
    assertTrue(reserver.hasReservations(getTaskGroup(INSTANCE_KEY)));
    assertTrue(reserver.hasReservations(getTaskGroup(instanceKey2)));
    assertTrue(reserver.hasReservations(getTaskGroup(instanceKey3)));
    assertTrue(reserver.hasReservations(
        getTaskGroup(InstanceKeys.from(JobKeys.from("role", "env", "name"), 3))));
    assertFalse(reserver.hasReservations(
        getTaskGroup(InstanceKeys.from(JobKeys.from("not", "in", "map"), 1))));
  }

}
