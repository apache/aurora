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
package org.apache.aurora.scheduler.storage.durability;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.durability.DurableStorage.TransactionManager;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WriteRecorderTest extends EasyMockTest {

  private TransactionManager transactionManager;
  private TaskStore.Mutable taskStore;
  private AttributeStore.Mutable attributeStore;
  private JobUpdateStore.Mutable jobUpdateStore;
  private EventSink eventSink;
  private WriteRecorder storage;

  @Before
  public void setUp() {
    transactionManager = createMock(TransactionManager.class);
    taskStore = createMock(TaskStore.Mutable.class);
    attributeStore = createMock(AttributeStore.Mutable.class);
    jobUpdateStore = createMock(JobUpdateStore.Mutable.class);
    eventSink = createMock(EventSink.class);

    storage = new WriteRecorder(
        transactionManager,
        createMock(SchedulerStore.Mutable.class),
        createMock(CronJobStore.Mutable.class),
        taskStore,
        createMock(QuotaStore.Mutable.class),
        attributeStore,
        jobUpdateStore,
        LoggerFactory.getLogger(WriteRecorderTest.class),
        eventSink);
  }

  private void expectOp(Op op) {
    expect(transactionManager.hasActiveTransaction()).andReturn(true);
    transactionManager.log(op);
  }

  @Test
  public void testRemoveUpdates() {
    Set<IJobUpdateKey> removed = ImmutableSet.of(
        IJobUpdateKey.build(new JobUpdateKey(TaskTestUtil.JOB.newBuilder(), "a")),
        IJobUpdateKey.build(new JobUpdateKey(TaskTestUtil.JOB.newBuilder(), "b")));
    jobUpdateStore.removeJobUpdates(removed);
    // No operation is written since this Op is in read-only compatibility mode.

    control.replay();

    storage.removeJobUpdates(removed);
  }

  @Test
  public void testMutate() {
    String taskId = "a";
    Function<IScheduledTask, IScheduledTask> mutator =
        createMock(new Clazz<Function<IScheduledTask, IScheduledTask>>() { });
    Optional<IScheduledTask> mutated = Optional.of(TaskTestUtil.makeTask(taskId, TaskTestUtil.JOB));

    expect(taskStore.mutateTask(taskId, mutator)).andReturn(mutated);
    expectOp(Op.saveTasks(new SaveTasks(ImmutableSet.of(mutated.get().newBuilder()))));

    control.replay();

    assertEquals(mutated, storage.mutateTask(taskId, mutator));
  }

  @Test
  public void testSaveHostAttributes() {
    IHostAttributes attributes = IHostAttributes.build(
        new HostAttributes()
            .setHost("a")
            .setMode(MaintenanceMode.DRAINING)
            .setAttributes(ImmutableSet.of(
                new Attribute().setName("b").setValues(ImmutableSet.of("1", "2")))));

    expect(attributeStore.saveHostAttributes(attributes)).andReturn(true);
    expectOp(Op.saveHostAttributes(
        new SaveHostAttributes().setHostAttributes(attributes.newBuilder())));
    eventSink.post(new PubsubEvent.HostAttributesChanged(attributes));

    expect(attributeStore.saveHostAttributes(attributes)).andReturn(false);

    control.replay();

    assertTrue(storage.saveHostAttributes(attributes));

    assertFalse(storage.saveHostAttributes(attributes));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteAllTasks() {
    control.replay();
    storage.deleteAllTasks();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteHostAttributes() {
    control.replay();
    storage.deleteHostAttributes();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteJobs() {
    control.replay();
    storage.deleteJobs();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteQuotas() {
    control.replay();
    storage.deleteQuotas();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteAllUpdatesAndEvents() {
    control.replay();
    storage.deleteAllUpdates();
  }
}
