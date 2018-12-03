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
package org.apache.aurora.scheduler.storage.durability;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.namedPort;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.junit.Assert.assertEquals;

public class ThriftBackfillTest extends EasyMockTest {

  private ThriftBackfill thriftBackfill;

  @Before
  public void setUp() {
    thriftBackfill = new ThriftBackfill();
  }

  @Test
  public void testFieldsToSetNoPorts() {
    TaskConfig config = new TaskConfig()
        .setResources(ImmutableSet.of(
            numCpus(1.0),
            ramMb(32),
            diskMb(64)))
        .setProduction(false)
        .setTier(TaskTestUtil.DEV_TIER_NAME);
    TaskConfig expected = config.deepCopy()
        .setResources(ImmutableSet.of(numCpus(1.0), ramMb(32), diskMb(64)));

    control.replay();

    assertEquals(
        expected,
        thriftBackfill.backfillTask(config));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testResourceAggregateTooManyResources() {
    control.replay();

    ResourceAggregate aggregate = new ResourceAggregate()
        .setResources(ImmutableSet.of(numCpus(1.0), ramMb(32), diskMb(64), numCpus(2.0)));
    ThriftBackfill.backfillResourceAggregate(aggregate);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testResourceAggregateInvalidResources() {
    control.replay();

    ResourceAggregate aggregate = new ResourceAggregate()
        .setResources(ImmutableSet.of(numCpus(1.0), ramMb(32), namedPort("http")));
    ThriftBackfill.backfillResourceAggregate(aggregate);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testResourceAggregateMissingResources() {
    control.replay();

    ResourceAggregate aggregate = new ResourceAggregate()
        .setResources(ImmutableSet.of(numCpus(1.0), ramMb(32)));
    ThriftBackfill.backfillResourceAggregate(aggregate);
  }
}
