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
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.mem.MemStorage;
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
    storage = MemStorage.newEmptyStorage();
  }

  @Test
  public void testJobConfigurationBackfill() throws Exception {
    TaskConfig task = TASK.getAssignedTask().getTask().newBuilder();
    final JobConfiguration config = new JobConfiguration()
        .setOwner(task.getOwner())
        .setKey(task.getJob())
        .setInstanceCount(1)
        .setTaskConfig(task);

    SanitizedConfiguration expected =
        SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(config));

    // Unset task config job key.
    config.getTaskConfig().unsetJob();
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getCronJobStore().saveAcceptedJob(IJobConfiguration.build(config));
      }
    });

    backfill();

    IJobConfiguration actual = Iterables.getOnlyElement(
        storage.read(new Storage.Work.Quiet<Iterable<IJobConfiguration>>() {
          @Override
          public Iterable<IJobConfiguration> apply(Storage.StoreProvider storeProvider) {
            return storeProvider.getCronJobStore().fetchJobs();
          }
        }));

    assertEquals(expected.getJobConfig(), actual);
  }

  @Test
  public void testBackfillTask() throws Exception {
    ScheduledTask task = TASK.newBuilder();
    ConfigurationManager.applyDefaultsIfUnset(task.getAssignedTask().getTask());

    final Set<IScheduledTask> backfilledTasks = ImmutableSet.of(IScheduledTask.build(task));
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(backfilledTasks);
      }
    });

    backfill();

    assertEquals(
        ImmutableSet.of(IScheduledTask.build(task)),
        Storage.Util.fetchTasks(storage, Query.unscoped()));
  }

  private void backfill() {
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
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
