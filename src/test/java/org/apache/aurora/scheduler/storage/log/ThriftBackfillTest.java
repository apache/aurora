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
package org.apache.aurora.scheduler.storage.log;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.TaskConfig;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.namedPort;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.junit.Assert.assertEquals;

public class ThriftBackfillTest {

  @Test
  public void testFieldsToSetNoPorts() {
    TaskConfig config = new TaskConfig()
        .setNumCpus(1.0)
        .setRamMb(32)
        .setDiskMb(64);

    TaskConfig expected = config.deepCopy();
    expected.setResources(ImmutableSet.of(numCpus(1.0), ramMb(32), diskMb(64)));

    assertEquals(
        expected,
        ThriftBackfill.backfillTask(config));
  }

  @Test
  public void testFieldsToSetWithPorts() {
    TaskConfig config = new TaskConfig()
        .setNumCpus(1.0)
        .setRamMb(32)
        .setDiskMb(64)
        .setRequestedPorts(ImmutableSet.of("http"));

    TaskConfig expected = config.deepCopy();
    expected.setResources(ImmutableSet.of(numCpus(1.0), ramMb(32), diskMb(64), namedPort("http")));

    assertEquals(
        expected,
        ThriftBackfill.backfillTask(config));
  }

  @Test
  public void testSetToFieldsNoPorts() {
    TaskConfig config = new TaskConfig()
        .setResources(ImmutableSet.of(numCpus(1.0), ramMb(32), diskMb(64)));

    TaskConfig expected = config.deepCopy()
        .setNumCpus(1.0)
        .setRamMb(32)
        .setDiskMb(64);

    assertEquals(
        expected,
        ThriftBackfill.backfillTask(config));
  }

  @Test
  public void testSetToFieldsWithPorts() {
    TaskConfig config = new TaskConfig()
        .setResources(ImmutableSet.of(numCpus(1.0), ramMb(32), diskMb(64), namedPort("http")));

    TaskConfig expected = config.deepCopy()
        .setNumCpus(1.0)
        .setRamMb(32)
        .setDiskMb(64)
        .setRequestedPorts(ImmutableSet.of("http"));

    assertEquals(
        expected,
        ThriftBackfill.backfillTask(config));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingResourceThrows() {
    TaskConfig config = new TaskConfig().setResources(ImmutableSet.of(numCpus(1.0), ramMb(32)));
    ThriftBackfill.backfillTask(config);
  }
}
