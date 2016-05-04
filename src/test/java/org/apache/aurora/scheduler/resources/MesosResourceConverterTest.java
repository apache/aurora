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

import org.junit.Test;

import static org.apache.aurora.scheduler.resources.MesosResourceConverter.RANGES;
import static org.apache.aurora.scheduler.resources.MesosResourceConverter.SCALAR;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.junit.Assert.assertEquals;

public class MesosResourceConverterTest {
  @Test
  public void testQuantifyScalar() {
    assertEquals(2, SCALAR.quantify(mesosScalar(CPUS, 2.0)).doubleValue(), 0.0);
  }

  @Test
  public void testQuantifyRangeSinglePort() {
    assertEquals(1, RANGES.quantify(mesosRange(PORTS, 5000)).doubleValue(), 0.0);
  }

  @Test
  public void testQuantifyRangeMultiplePorts() {
    assertEquals(3, RANGES.quantify(mesosRange(PORTS, 1, 2, 3)).doubleValue(), 0.0);
  }

  @Test
  public void testQuantifyRangeDefaultValue() {
    assertEquals(0, RANGES.quantify(mesosRange(PORTS)).doubleValue(), 0.0);
  }
}
