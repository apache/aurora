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

import java.io.File;
import java.nio.charset.StandardCharsets;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import org.junit.Test;

import static org.apache.aurora.scheduler.TierModule.TIER_CONFIG_PATH;
import static org.apache.aurora.scheduler.TierModule.parseTierConfig;
import static org.apache.aurora.scheduler.TierModule.readTierFile;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TierModuleTest {

  @Test
  public void testTierConfigArgumentNotSpecified() throws Exception {
    String tierFileContents = readTierFile();
    assertFalse(Strings.isNullOrEmpty(tierFileContents));
    assertEquals(
        tierFileContents,
        Files.toString(
            new File(TierModule.class.getClassLoader().getResource(TIER_CONFIG_PATH).getFile()),
            StandardCharsets.UTF_8));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTierConfigFile() {
    parseTierConfig("");
  }

  @Test
  public void testTierConfigFile() {
    assertEquals(
        ImmutableMap.of("revocable", REVOCABLE_TIER),
        parseTierConfig(
            "{\"tiers\":{"
            + "\"revocable\": {\"revocable\": true, \"preemptible\": true}"
            + "}}")
            .getTiers());
  }

  @Test(expected = RuntimeException.class)
  public void testTierConfigExtraKeysNotAllowed() {
    parseTierConfig("{\"tiers\":{\"revocable\": {\"revocable\": true, \"foo\": false}}}");
  }
}
