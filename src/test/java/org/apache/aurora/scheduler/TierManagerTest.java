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

import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Test;

import static org.apache.aurora.scheduler.TierModule.parseTierConfig;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.junit.Assert.assertEquals;

public class TierManagerTest {
  private static final TierManager TIER_MANAGER = new TierManagerImpl(
      parseTierConfig("{\"tiers\":{"
          + "\"preferred\": {\"revocable\": false, \"preemptible\": false},"
          + "\"preemptible\": {\"revocable\": false, \"preemptible\": true},"
          + "\"revocable\": {\"revocable\": true, \"preemptible\": true}"
          + "}}"));

  @Test
  public void testRevocable() {
    assertEquals(
        REVOCABLE_TIER,
        TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig().setTier("revocable"))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNameMismatch() {
    TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig().setTier("Revocable")));
  }

  @Test
  public void testNoTierInTaskConfig() {
    assertEquals(TaskTestUtil.DEV_TIER, TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig())));
  }
}
