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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageEntityUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public abstract class AbstractCronJobStoreTest {
  private static final IJobConfiguration JOB_A = makeJob("a");
  private static final IJobConfiguration JOB_B = makeJob("b");

  private static final IJobKey KEY_A = JOB_A.getKey();
  private static final IJobKey KEY_B = JOB_B.getKey();

  protected Injector injector;
  protected Storage storage;

  protected abstract Module getStorageModule();

  @Before
  public void baseSetUp() {
    injector = Guice.createInjector(getStorageModule());
    storage = injector.getInstance(Storage.class);
    storage.prepare();
  }

  @Test
  public void testJobStore() {
    assertNull(fetchJob(JobKeys.from("nobody", "nowhere", "noname")).orNull());
    assertEquals(ImmutableSet.of(), fetchJobs());

    saveAcceptedJob(JOB_A);
    assertEquals(JOB_A, fetchJob(KEY_A).orNull());
    assertEquals(ImmutableSet.of(JOB_A), fetchJobs());

    saveAcceptedJob(JOB_B);
    assertEquals(JOB_B, fetchJob(KEY_B).orNull());
    assertEquals(ImmutableSet.of(JOB_A, JOB_B), fetchJobs());

    removeJob(KEY_B);
    assertEquals(ImmutableSet.of(JOB_A), fetchJobs());

    deleteJobs();
    assertEquals(ImmutableSet.of(), fetchJobs());
  }

  @Test
  public void testJobStoreSameEnvironment() {
    IJobConfiguration templateConfig = makeJob("labrat");
    JobConfiguration prodBuilder = templateConfig.newBuilder();
    prodBuilder.getKey().setEnvironment("prod");
    IJobConfiguration prod = IJobConfiguration.build(prodBuilder);
    JobConfiguration stagingBuilder = templateConfig.newBuilder();
    stagingBuilder.getKey().setEnvironment("staging");
    IJobConfiguration staging = IJobConfiguration.build(stagingBuilder);

    saveAcceptedJob(prod);
    saveAcceptedJob(staging);

    assertNull(fetchJob(
        IJobKey.build(templateConfig.getKey().newBuilder().setEnvironment("test"))).orNull());
    assertEquals(prod, fetchJob(prod.getKey()).orNull());
    assertEquals(staging, fetchJob(staging.getKey()).orNull());

    removeJob(prod.getKey());
    assertNull(fetchJob(prod.getKey()).orNull());
    assertEquals(staging, fetchJob(staging.getKey()).orNull());
  }

  @Test
  public void testTaskConfigDedupe() {
    // Test for regression of AURORA-1392.

    final IScheduledTask instance = TaskTestUtil.makeTask("a", JOB_A.getTaskConfig());
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(instance));
      }
    });

    saveAcceptedJob(JOB_A);

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().mutateTasks(Query.taskScoped(Tasks.id(instance)),
            new TaskStore.Mutable.TaskMutation() {
              @Override
              public IScheduledTask apply(IScheduledTask task) {
                return IScheduledTask.build(task.newBuilder().setStatus(ScheduleStatus.RUNNING));
              }
            });
      }
    });
  }

  @Test
  public void testUpdate() {
    // Test for regression of AURORA-1390.  Updates are not normal, but are used in cases such as
    // backfilling fields upon storage recovery.

    saveAcceptedJob(JOB_A);
    IJobConfiguration jobAUpdated =
        IJobConfiguration.build(JOB_A.newBuilder().setCronSchedule("changed"));
    saveAcceptedJob(jobAUpdated);
    assertEquals(jobAUpdated, fetchJob(KEY_A).orNull());
  }

  private static IJobConfiguration makeJob(String name) {
    IJobKey job = JobKeys.from("role-" + name, "env-" + name, name);
    ITaskConfig config = TaskTestUtil.makeConfig(job);

    return StorageEntityUtil.assertFullyPopulated(
        IJobConfiguration.build(
            new JobConfiguration()
                .setKey(job.newBuilder())
                .setOwner(new Identity(job.getRole(), "user"))
                .setCronSchedule("schedule")
                .setCronCollisionPolicy(CronCollisionPolicy.CANCEL_NEW)
                .setTaskConfig(config.newBuilder())
                .setInstanceCount(5)));
  }

  private Set<IJobConfiguration> fetchJobs() {
    return storage.read(new Work.Quiet<Set<IJobConfiguration>>() {
      @Override
      public Set<IJobConfiguration> apply(StoreProvider storeProvider) {
        return ImmutableSet.copyOf(storeProvider.getCronJobStore().fetchJobs());
      }
    });
  }

  private Optional<IJobConfiguration> fetchJob(final IJobKey jobKey) {
    return storage.read(new Work.Quiet<Optional<IJobConfiguration>>() {
      @Override
      public Optional<IJobConfiguration> apply(StoreProvider storeProvider) {
        return storeProvider.getCronJobStore().fetchJob(jobKey);
      }
    });
  }

  private void saveAcceptedJob(final IJobConfiguration jobConfig) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getCronJobStore().saveAcceptedJob(jobConfig);
      }
    });
  }

  private void removeJob(final IJobKey jobKey) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getCronJobStore().removeJob(jobKey);
      }
    });
  }

  private void deleteJobs() {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getCronJobStore().deleteJobs();
      }
    });
  }
}
