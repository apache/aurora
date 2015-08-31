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

import com.google.common.base.Optional;

import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl.TierConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Test;

import static org.apache.aurora.scheduler.SchedulerModule.parseTierConfig;
import static org.apache.aurora.scheduler.TierInfo.DEFAULT;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.junit.Assert.assertEquals;

public class TierManagerTest {

  @Test
  public void testRevocable() {
    TierManager manager = new TierManagerImpl(
        parseTierConfig(Optional.of("{\"tiers\":{\"revocable\": {\"revocable\": true}}}")));
    assertEquals(
        REVOCABLE_TIER,
        manager.getTier(ITaskConfig.build(new TaskConfig().setTier("revocable"))));
  }

  @Test
  public void testNameMismatch() {
    TierManager manager = new TierManagerImpl(
        parseTierConfig(Optional.of("{\"tiers\":{\"revocable\": {\"revocable\": true}}}")));
    assertEquals(
        DEFAULT,
        manager.getTier(ITaskConfig.build(new TaskConfig().setTier("Revocable"))));
  }

  @Test
  public void testDefaultTier() {
    TierManager manager = new TierManagerImpl(TierConfig.EMPTY);
    assertEquals(
        DEFAULT,
        manager.getTier(ITaskConfig.build(new TaskConfig().setTier("revocable"))));
  }

  @Test
  public void testNoTier() {
    TierManager manager = new TierManagerImpl(TierConfig.EMPTY);
    assertEquals(
        DEFAULT,
        manager.getTier(ITaskConfig.build(new TaskConfig())));
  }
}
