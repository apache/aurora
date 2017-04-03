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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.junit.Assert.assertEquals;

public class RowGarbageCollectorTest {

  private static final IJobKey JOB_A = IJobKey.build(new JobKey("roleA", "envA", "jobA"));
  private static final IJobKey JOB_B = IJobKey.build(new JobKey("roleB", "envB", "jobB"));
  private static final IScheduledTask TASK_A2 = TaskTestUtil.makeTask("task_a2", JOB_A);
  private static final ITaskConfig CONFIG_A =
      ITaskConfig.build(TASK_A2.getAssignedTask().getTask().newBuilder()
              .setResources(ImmutableSet.of(numCpus(1.0), ramMb(124246), diskMb(1024))));
  private static final ITaskConfig CONFIG_B = TaskTestUtil.makeConfig(JOB_B);

  private JobKeyMapper jobKeyMapper;
  private TaskMapper taskMapper;
  private TaskConfigMapper taskConfigMapper;
  private RowGarbageCollector rowGc;

  @Before
  public void setUp() {
    Injector injector = Guice.createInjector(
        DbModule.testModuleWithWorkQueue(),
        new DbModule.GarbageCollectorModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(Clock.class).toInstance(new FakeClock());
          }
        }
    );

    rowGc = injector.getInstance(RowGarbageCollector.class);
    injector.getInstance(Storage.class).prepare();
    taskMapper = injector.getInstance(TaskMapper.class);
    jobKeyMapper = injector.getInstance(JobKeyMapper.class);
    taskConfigMapper = injector.getInstance(TaskConfigMapper.class);
  }

  @Test
  public void testNoop() {
    rowGc.runOneIteration();
  }

  @Test
  public void testGarbageCollection() {
    rowGc.runOneIteration();
    assertEquals(ImmutableList.of(), jobKeyMapper.selectAll());
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_A));
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_B));

    jobKeyMapper.merge(JOB_A);
    rowGc.runOneIteration();
    assertEquals(ImmutableList.of(), jobKeyMapper.selectAll());
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_A));
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_B));

    jobKeyMapper.merge(JOB_A);
    taskConfigMapper.insert(CONFIG_A, new InsertResult());
    InsertResult a2Insert = new InsertResult();
    taskConfigMapper.insert(TASK_A2.getAssignedTask().getTask(), a2Insert);
    taskMapper.insertScheduledTask(TASK_A2, a2Insert.getId(), new InsertResult());
    jobKeyMapper.merge(JOB_B);
    taskConfigMapper.insert(CONFIG_B, new InsertResult());
    rowGc.runOneIteration();
    // Only job A and config A2 are still referenced, other rows are deleted.
    assertEquals(ImmutableList.of(JOB_A.newBuilder()), jobKeyMapper.selectAll());
    // Note: Using the ramMb as a sentinel value, since relations in the TaskConfig are not
    // populated, therefore full object equivalence cannot easily be used.
    assertEquals(
        TASK_A2.getAssignedTask().getTask().getRamMb(),
        Iterables.getOnlyElement(taskConfigMapper.selectConfigsByJob(JOB_A)).toImmutable()
            .getRamMb());
    assertEquals(ImmutableList.of(), taskConfigMapper.selectConfigsByJob(JOB_B));
  }
}
