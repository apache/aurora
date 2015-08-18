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

import static org.junit.Assert.assertEquals;

public class TierManagerTest {

  @Test
  public void testIsRevocable() {
    TierInfo expected = new TierInfo(false);
    assertEquals(expected, new TierManagerImpl().getTier(ITaskConfig.build(new TaskConfig())));
  }
}
