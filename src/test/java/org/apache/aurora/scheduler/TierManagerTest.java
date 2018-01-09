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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Test;

import static org.apache.aurora.scheduler.TierModule.parseTierConfig;
import static org.apache.aurora.scheduler.base.TaskTestUtil.DEV_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.PREFERRED_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.junit.Assert.assertEquals;

public class TierManagerTest {
  private static final String PREFERRED_TIER_NAME = "preferred";
  private static final String PREEMPTIBLE_TIER_NAME = "preemptible";
  private static final String REVOCABLE_TIER_NAME = "revocable";
  private static final Map<String, TierInfo> TIERS = ImmutableMap.of(
      PREFERRED_TIER_NAME, PREFERRED_TIER,
      PREEMPTIBLE_TIER_NAME, DEV_TIER,
      REVOCABLE_TIER_NAME, REVOCABLE_TIER);
  private static final TierManager TIER_MANAGER = new TierManagerImpl(
      parseTierConfig(TaskTestUtil.tierConfigFile()));

  @Test
  public void testGetTierRevocable() {
    assertEquals(
        REVOCABLE_TIER,
        TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig().setTier(REVOCABLE_TIER_NAME))));
  }

  @Test
  public void testDefaultTier() {
    assertEquals(
        DEV_TIER,
        TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig())));
  }

  @Test
  public void testGetTierRevocableAndProduction() {
    assertEquals(
        REVOCABLE_TIER,
        TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig()
            .setTier(REVOCABLE_TIER_NAME)
            .setProduction(true))));
  }

  @Test
  public void testGetTierPreemptibleAndProduction() {
    assertEquals(
        DEV_TIER,
        TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig()
            .setTier(PREEMPTIBLE_TIER_NAME)
            .setProduction(true))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTierNameMismatch() {
    TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig().setTier("Revocable")));
  }

  @Test
  public void testGetDefaultTierName() {
    assertEquals(PREEMPTIBLE_TIER_NAME, TIER_MANAGER.getDefaultTierName());
  }

  @Test
  public void testTiers() {
    assertEquals(TIERS, TIER_MANAGER.getTiers());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingDefaultInTierConfig() {
    new TierManagerImpl.TierConfig(null, TIERS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTiersInTierConfig() {
    new TierManagerImpl.TierConfig(PREEMPTIBLE_TIER_NAME, ImmutableMap.of());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMismatchingDefaultInTierConfig() {
    new TierManagerImpl.TierConfig("something", TIERS);
  }
}
