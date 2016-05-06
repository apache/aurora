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
package org.apache.aurora.scheduler.quota;

import org.junit.Test;

import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.greaterOrEqual;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.bag;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuotaCheckResultTest {

  @Test
  public void testGreaterOrEqualPass() {
    assertEquals(
        SUFFICIENT_QUOTA,
        greaterOrEqual(bag(1.0, 256, 512), bag(1.0, 256, 512)).getResult());
  }

  @Test
  public void testGreaterOrEqualFailsCpu() {
    QuotaCheckResult result = greaterOrEqual(bag(1.0, 256, 512), bag(2.0, 256, 512));
    assertEquals(INSUFFICIENT_QUOTA, result.getResult());
    assertTrue(result.getDetails().get().contains(CPUS.getAuroraName()));
  }

  @Test
  public void testGreaterOrEqualFailsRam() {
    QuotaCheckResult result = greaterOrEqual(bag(1.0, 256, 512), bag(1.0, 512, 512));
    assertEquals(INSUFFICIENT_QUOTA, result.getResult());
    assertTrue(result.getDetails().get().length() > 0);
    assertTrue(result.getDetails().get().contains(RAM_MB.getAuroraName()));
  }

  @Test
  public void testGreaterOrEqualFailsDisk() {
    QuotaCheckResult result = greaterOrEqual(bag(1.0, 256, 512), bag(1.0, 256, 1024));
    assertEquals(INSUFFICIENT_QUOTA, result.getResult());
    assertTrue(result.getDetails().get().length() > 0);
    assertTrue(result.getDetails().get().contains(DISK_MB.getAuroraName()));
  }
}
