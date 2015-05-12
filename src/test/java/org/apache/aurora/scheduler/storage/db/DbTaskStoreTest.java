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
package org.apache.aurora.scheduler.storage.db;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.AbstractTaskStoreTest;
import org.apache.aurora.scheduler.storage.db.views.TaskConfigRow;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DbTaskStoreTest extends AbstractTaskStoreTest {

  private TaskConfigManager configManager;

  @Before
  public void setUp() {
    configManager = injector.getInstance(TaskConfigManager.class);
  }

  @Override
  protected Module getStorageModule() {
    return Modules.combine(
        DbModule.testModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(Clock.class).toInstance(new FakeClock());
          }
        });
  }

  @Test
  public void testRelationsRemoved() {
    // When there are no remaining references to a task config, it should be removed.
    saveTasks(TASK_A);
    deleteTasks(Tasks.id(TASK_A));
    assertEquals(
        ImmutableList.<TaskConfigRow>of(),
        configManager.getConfigs(TASK_A.getAssignedTask().getTask().getJob()));

    // TODO(wfarner): Check that the job key was removed.
  }
}
