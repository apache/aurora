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
package org.apache.aurora.scheduler.resources;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static java.lang.Double.POSITIVE_INFINITY;

import static org.apache.aurora.scheduler.resources.ResourceBag.IS_NEGATIVE;
import static org.apache.aurora.scheduler.resources.ResourceBag.LARGE;
import static org.apache.aurora.scheduler.resources.ResourceBag.MEDIUM;
import static org.apache.aurora.scheduler.resources.ResourceBag.SMALL;
import static org.apache.aurora.scheduler.resources.ResourceBag.XLARGE;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.bag;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.junit.Assert.assertEquals;

public class ResourceBagTest {
  @Test
  public void testAdd() {
    assertEquals(LARGE, MEDIUM.add(MEDIUM));
  }

  @Test
  public void testSubtract() {
    assertEquals(MEDIUM, LARGE.subtract(MEDIUM));
  }

  @Test
  public void testDivide() {
    assertEquals(bag(16.0, 32, 16), XLARGE.divide(SMALL));
  }

  @Test
  public void testMax() {
    assertEquals(bag(2.0, 32, 256), bag(1.0, 32, 128).max(bag(2.0, 16, 256)));
  }

  @Test
  public void testScale() {
    assertEquals(bag(8.0, 128, 1024), bag(2.0, 32, 256).scale(4));
  }

  @Test
  public void testKeyMismatch() {
    assertEquals(
        new ResourceBag(ImmutableMap.of(CPUS, 9.0, RAM_MB, 16384.0, DISK_MB, 32768.0)),
        new ResourceBag(ImmutableMap.of(CPUS, 1.0)).add(LARGE));

    // Check add with mismatched keys is still commutative.
    assertEquals(
        new ResourceBag(ImmutableMap.of(CPUS, 9.0, RAM_MB, 16384.0, DISK_MB, 32768.0)),
        LARGE.add(new ResourceBag(ImmutableMap.of(CPUS, 1.0))));

    // Check divide with missing values on the right.
    assertEquals(
        new ResourceBag(ImmutableMap.of(
            CPUS, 1.0,
            RAM_MB, POSITIVE_INFINITY,
            DISK_MB, POSITIVE_INFINITY)),
        LARGE.divide(new ResourceBag(ImmutableMap.of(CPUS, 8.0))));
  }

  @Test
  public void testValueOf() {
    assertEquals(1.0, SMALL.valueOf(CPUS), 0.0);
    assertEquals(0.0, SMALL.valueOf(PORTS), 0.0);
  }

  @Test
  public void testFilter() {
    assertEquals(
        new ResourceBag(ImmutableMap.of(CPUS, -1.0)),
        bag(-1.0, 128, 1024).filter(IS_NEGATIVE));
  }
}
