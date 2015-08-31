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
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static org.apache.aurora.scheduler.SchedulerModule.parseTierConfig;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.junit.Assert.assertEquals;

public class SchedulerModuleTest {
  @Test
  public void testNoTierConfigFile() {
    assertEquals(ImmutableMap.of(), parseTierConfig(Optional.absent()).getTiers());
  }

  @Test
  public void testTierConfigFile() {
    assertEquals(
        ImmutableMap.of("revocable", REVOCABLE_TIER),
        parseTierConfig(Optional.of("{\"tiers\":{\"revocable\": {\"revocable\": true}}}"))
            .getTiers());
  }

  @Test(expected = RuntimeException.class)
  public void testTierConfigExtraKeysNotAllowed() {
    parseTierConfig(
        Optional.of("{\"tiers\":{\"revocable\": {\"revocable\": true, \"foo\": false}}}"));
  }
}
