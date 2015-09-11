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
package org.apache.aurora.scheduler.storage;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * TODO(wfarner): Data store constraints (not null, valid relations) have made this test of little
 * value other than for coverage.  Rethink.
 */
public class StorageBackfillTest {
  // By default, only the mesos container is allowed by ConfigurationManager.
  private static final IScheduledTask TASK =
      setMesosContainer(TaskTestUtil.makeTask("task_id", TaskTestUtil.JOB));

  private Storage storage;

  @Before
  public void setUp() {
    storage = DbUtil.createStorage();
  }

  @Test
  public void testBackfillTask() {
    final Set<IScheduledTask> backfilledTasks = ImmutableSet.of(TASK);
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      public void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(backfilledTasks);
      }
    });

    backfill();

    assertEquals(
        ImmutableSet.of(TASK),
        Storage.Util.fetchTasks(storage, Query.unscoped()));
  }

  private void backfill() {
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      public void execute(Storage.MutableStoreProvider storeProvider) {
        StorageBackfill.backfill(storeProvider);
      }
    });
  }

  private static IScheduledTask setMesosContainer(IScheduledTask task) {
    ScheduledTask builder = task.newBuilder();
    builder.getAssignedTask().getTask().setContainer(Container.mesos(new MesosContainer()));
    return IScheduledTask.build(builder);
  }
}
