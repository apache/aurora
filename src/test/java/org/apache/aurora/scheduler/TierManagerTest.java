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
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Test;

import static org.apache.aurora.scheduler.TierModule.parseTierConfig;
import static org.apache.aurora.scheduler.base.TaskTestUtil.DEV_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.PREFERRED_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.junit.Assert.assertEquals;

public class TierManagerTest {
  private static final Map<String, TierInfo> TIERS = ImmutableMap.of(
      "preferred", PREFERRED_TIER,
      "preemptible", DEV_TIER,
      "revocable", REVOCABLE_TIER);
  private static final TierManager TIER_MANAGER = new TierManagerImpl(
      parseTierConfig("{\"default\": \"preemptible\","
          + "\"tiers\":{"
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

  @Test
  public void testRevocableAndProduction() {
    assertEquals(
        REVOCABLE_TIER,
        TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig()
            .setTier("revocable")
            .setProduction(true))));
  }

  @Test
  public void testPreemptibleAndProduction() {
    assertEquals(
        DEV_TIER,
        TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig()
            .setTier("preemptible")
            .setProduction(true))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNameMismatch() {
    TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig().setTier("Revocable")));
  }

  @Test
  public void testProduction() {
    assertEquals(
        PREFERRED_TIER,
        TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig().setProduction(true))));
  }

  @Test
  public void testNoTierInTaskConfig() {
    assertEquals(DEV_TIER, TIER_MANAGER.getTier(ITaskConfig.build(new TaskConfig())));
  }

  @Test(expected = IllegalStateException.class)
  public void testBadTierConfiguration() {
    TierManager tierManager = new TierManagerImpl(
        parseTierConfig("{\"default\": \"revocable\","
            + "\"tiers\":{"
            + "\"preferred\": {\"revocable\": false, \"preemptible\": false},"
            + "\"revocable\": {\"revocable\": true, \"preemptible\": true}"
            + "}}"));
    // preemptible: false, revocable: false
    ITaskConfig taskConfig = ITaskConfig.build(new TaskConfig());
    tierManager.getTier(taskConfig);
  }

  @Test
  public void testDefault() {
    assertEquals("preemptible", TIER_MANAGER.getDefaultTierName());
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
    new TierManagerImpl.TierConfig("preemptible", ImmutableMap.of());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMismatchingDefaultInTierConfig() {
    new TierManagerImpl.TierConfig("something", TIERS);
  }
}
