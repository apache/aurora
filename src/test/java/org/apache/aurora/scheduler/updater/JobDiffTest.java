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

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JobDiffTest extends EasyMockTest {

  private static final IJobKey JOB = JobKeys.from("role", "env", "job");
  private static final JobDiff NO_DIFF =
      new JobDiff(ImmutableMap.of(), ImmutableSet.of(), ImmutableSet.of());
  private static final Set<IRange> NO_SCOPE = ImmutableSet.of();
  private static final Set<IRange> CANARY_SCOPE = ImmutableSet.of(IRange.build(new Range(0, 0)));

  private TaskStore store;

  @Before
  public void setUp() {
    store = createMock(TaskStore.class);
  }

  @Test
  public void testNoDiff() {
    ITaskConfig task = makeTask("job", "echo");

    expectFetch(instance(task, 0), instance(task, 1)).times(2);

    control.replay();

    assertEquals(
        new JobDiff(ImmutableMap.of(), ImmutableSet.of(), ImmutableSet.of(0, 1)),
        JobDiff.compute(store, JOB, JobDiff.asMap(task, 2), NO_SCOPE));
    assertEquals(
        new JobDiff(ImmutableMap.of(), ImmutableSet.of(), ImmutableSet.of(0, 1)),
        JobDiff.compute(store, JOB, JobDiff.asMap(task, 2), CANARY_SCOPE));
  }

  @Test
  public void testInstancesAdded() {
    ITaskConfig task = makeTask("job", "echo");

    expectFetch(instance(task, 0), instance(task, 1)).times(2);

    control.replay();

    assertEquals(
        new JobDiff(ImmutableMap.of(), ImmutableSet.of(2, 3, 4), ImmutableSet.of(0, 1)),
        JobDiff.compute(store, JOB, JobDiff.asMap(task, 5), NO_SCOPE));
    assertEquals(
        new JobDiff(ImmutableMap.of(), ImmutableSet.of(), ImmutableSet.of(0, 1)),
        JobDiff.compute(store, JOB, JobDiff.asMap(task, 5), CANARY_SCOPE));
  }

  @Test
  public void testInstancesRemoved() {
    ITaskConfig task = makeTask("job", "echo");

    expectFetch(instance(task, 0), instance(task, 1), instance(task, 2)).times(2);

    control.replay();

    assertEquals(
        new JobDiff(ImmutableMap.of(1, task, 2, task), ImmutableSet.of(), ImmutableSet.of(0)),
        JobDiff.compute(store, JOB, JobDiff.asMap(task, 1), NO_SCOPE));
    assertEquals(
        new JobDiff(ImmutableMap.of(), ImmutableSet.of(), ImmutableSet.of(0, 1, 2)),
        JobDiff.compute(store, JOB, JobDiff.asMap(task, 1), CANARY_SCOPE));
  }

  @Test
  public void testFullUpdate() {
    ITaskConfig oldTask = makeTask("job", "echo");
    ITaskConfig newTask = makeTask("job", "echo2");

    expectFetch(instance(oldTask, 0), instance(oldTask, 1), instance(oldTask, 2)).times(2);

    control.replay();

    assertEquals(
        new JobDiff(
            ImmutableMap.of(0, oldTask, 1, oldTask, 2, oldTask),
            ImmutableSet.of(0, 1, 2),
            ImmutableSet.of()),
        JobDiff.compute(store, JOB, JobDiff.asMap(newTask, 3), NO_SCOPE));
    assertEquals(
        new JobDiff(ImmutableMap.of(0, oldTask), ImmutableSet.of(0), ImmutableSet.of(1, 2)),
        JobDiff.compute(store, JOB, JobDiff.asMap(newTask, 3), CANARY_SCOPE));
  }

  @Test
  public void testMultipleConfigsAndHoles() {
    ITaskConfig oldTask = makeTask("job", "echo");
    ITaskConfig oldTask2 = makeTask("job", "echo1");
    ITaskConfig newTask = makeTask("job", "echo2");

    expectFetch(
        instance(oldTask, 0), instance(newTask, 1), instance(oldTask2, 3), instance(oldTask, 4))
        .times(2);

    control.replay();

    assertEquals(
        new JobDiff(
            ImmutableMap.of(0, oldTask, 3, oldTask2, 4, oldTask),
            ImmutableSet.of(0, 2, 3, 4),
            ImmutableSet.of(1)),
        JobDiff.compute(store, JOB, JobDiff.asMap(newTask, 5), NO_SCOPE));
    assertEquals(
        new JobDiff(ImmutableMap.of(0, oldTask), ImmutableSet.of(0), ImmutableSet.of(1, 3, 4)),
        JobDiff.compute(store, JOB, JobDiff.asMap(newTask, 5), CANARY_SCOPE));
  }

  @Test
  public void testUnchangedInstances() {
    ITaskConfig oldTask = makeTask("job", "echo");
    ITaskConfig newTask = makeTask("job", "echo2");

    expectFetch(instance(oldTask, 0), instance(newTask, 1), instance(oldTask, 2)).times(3);

    control.replay();

    assertEquals(
        new JobDiff(
            ImmutableMap.of(0, oldTask, 2, oldTask),
            ImmutableSet.of(0, 2),
            ImmutableSet.of(1)),
        JobDiff.compute(store, JOB, JobDiff.asMap(newTask, 3), NO_SCOPE));
    assertEquals(
        new JobDiff(ImmutableMap.of(0, oldTask), ImmutableSet.of(0), ImmutableSet.of(1, 2)),
        JobDiff.compute(store, JOB, JobDiff.asMap(newTask, 3), CANARY_SCOPE));
    assertEquals(
        new JobDiff(ImmutableMap.of(0, oldTask), ImmutableSet.of(0), ImmutableSet.of(1, 2)),
        JobDiff.compute(store, JOB, JobDiff.asMap(newTask, 4), CANARY_SCOPE));
  }

  @Test
  public void testObjectOverrides() {
    control.replay();

    JobDiff a = new JobDiff(
        ImmutableMap.of(0, makeTask("job", "echo")),
        ImmutableSet.of(0),
        ImmutableSet.of());
    JobDiff b = new JobDiff(
        ImmutableMap.of(0, makeTask("job", "echo")),
        ImmutableSet.of(0),
        ImmutableSet.of());
    JobDiff c = new JobDiff(
        ImmutableMap.of(0, makeTask("job", "echo1")),
        ImmutableSet.of(0),
        ImmutableSet.of());
    JobDiff d = new JobDiff(
        ImmutableMap.of(0, makeTask("job", "echo")),
        ImmutableSet.of(1),
        ImmutableSet.of());
    assertEquals(a, b);
    assertEquals(ImmutableSet.of(a), ImmutableSet.of(a, b));
    assertNotEquals(a, c);
    assertNotEquals(a, "a string");
    assertNotEquals(a, d);
    assertEquals(a.toString(), b.toString());
  }

  private IExpectationSetters<?> expectFetch(IAssignedTask... results) {
    ImmutableSet.Builder<IScheduledTask> tasks = ImmutableSet.builder();
    for (IAssignedTask result : results) {
      tasks.add(IScheduledTask.build(new ScheduledTask().setAssignedTask(result.newBuilder())));
    }

    return expect(store.fetchTasks(Query.jobScoped(JOB).active()))
        .andReturn(tasks.build());
  }

  private static IAssignedTask instance(ITaskConfig config, int instance) {
    return IAssignedTask.build(
        new AssignedTask().setTask(config.newBuilder()).setInstanceId(instance));
  }

  private static ITaskConfig makeTask(String job, String config) {
    return ITaskConfig.build(new TaskConfig()
        .setOwner(new Identity("owner", "owner"))
        .setEnvironment("test")
        .setJobName(job)
        .setExecutorConfig(new ExecutorConfig().setData(config)));
  }
}
