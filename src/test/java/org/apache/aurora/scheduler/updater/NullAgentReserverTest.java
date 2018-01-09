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
package org.apache.aurora.scheduler.updater;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver.NullAgentReserver;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class NullAgentReserverTest extends EasyMockTest {
  private static final IInstanceKey INSTANCE_KEY =
      InstanceKeys.from(JobKeys.from("role", "env", "name"), 1);

  @Test
  public void testNullReserver() {
    control.replay();
    NullAgentReserver reserver = new NullAgentReserver();
    reserver.reserve("test", INSTANCE_KEY);
    assertFalse(reserver.getAgent(INSTANCE_KEY).isPresent());
    assertFalse(reserver.isReserved("test"));
  }
}
