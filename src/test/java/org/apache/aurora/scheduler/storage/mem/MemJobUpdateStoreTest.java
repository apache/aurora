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
package org.apache.aurora.scheduler.storage.mem;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.AbstractJobUpdateStoreTest;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Test;

import static org.apache.aurora.scheduler.storage.mem.MemJobUpdateStore.UPDATE_STORE_SIZE;
import static org.junit.Assert.assertEquals;

public class MemJobUpdateStoreTest extends AbstractJobUpdateStoreTest {

  private FakeStatsProvider statsProvider;

  @Override
  protected Module getStorageModule() {
    statsProvider = new FakeStatsProvider();
    return Modules.combine(
        new MemStorageModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(statsProvider);
          }
        });
  }

  @Test
  public void testStoreSize() {
    IJobUpdateKey updateId1 = makeKey(JobKeys.from("role", "env", "name1"), "u1");
    IJobUpdateKey updateId2 = makeKey(JobKeys.from("role", "env", "name2"), "u2");

    IJobUpdateDetails update1 = makeJobUpdate(updateId1);
    IJobUpdateDetails update2 = makeJobUpdate(updateId2);

    assertEquals(0L, statsProvider.getLongValue(UPDATE_STORE_SIZE));
    saveUpdate(update1);
    assertEquals(1L, statsProvider.getLongValue(UPDATE_STORE_SIZE));
    saveUpdate(update2);
    assertEquals(2L, statsProvider.getLongValue(UPDATE_STORE_SIZE));
    truncateUpdates();
    assertEquals(0L, statsProvider.getLongValue(UPDATE_STORE_SIZE));
  }
}
