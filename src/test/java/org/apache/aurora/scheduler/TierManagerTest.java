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
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Test;

import static org.apache.aurora.scheduler.TierModule.parseTierConfig;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.junit.Assert.assertEquals;

public class TierManagerTest {
  @Test
  public void testRevocable() {
    TierManager manager = new TierManagerImpl(
        parseTierConfig("{\"tiers\":{"
            + "\"revocable\": {\"revocable\": true},"
            + "\"preferred\": {\"revocable\": false}"
            + "}}"));
    assertEquals(
        REVOCABLE_TIER,
        manager.getTier(ITaskConfig.build(new TaskConfig().setTier("revocable"))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNameMismatch() {
    TierManager manager = new TierManagerImpl(
        parseTierConfig("{\"tiers\":{\"revocable\": {\"revocable\": true}}}"));
    manager.getTier(ITaskConfig.build(new TaskConfig().setTier("Revocable")));
  }

  @Test
  public void testNoTierInTaskConfig() {
    TierManager manager = new TierManagerImpl(
        parseTierConfig("{\"tiers\":{\"revocable\": {\"revocable\": true}}}"));
    assertEquals(TierInfo.DEFAULT, manager.getTier(ITaskConfig.build(new TaskConfig())));
  }
}
