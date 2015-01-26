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
package org.apache.aurora.scheduler;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.scheduler.configuration.Resources;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ResourceSlotTest {

  @Test
  public void testMaxElements() {
    Resources highRAM = new Resources(1, Amount.of(8L, Data.GB), Amount.of(10L, Data.MB), 0);
    Resources rest = new Resources(10, Amount.of(1L, Data.MB), Amount.of(10L, Data.GB), 1);

    Resources result = ResourceSlot.maxElements(highRAM, rest);
    assertEquals(result.getNumCpus(), 10, 0.001);
    assertEquals(result.getRam(), Amount.of(8L, Data.GB));
    assertEquals(result.getDisk(), Amount.of(10L, Data.GB));
    assertEquals(result.getNumPorts(), 1);
  }
}
