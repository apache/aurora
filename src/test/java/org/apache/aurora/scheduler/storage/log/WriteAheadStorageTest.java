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
package org.apache.aurora.scheduler.storage.log;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.PruneJobUpdateHistory;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.JobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WriteAheadStorageTest extends EasyMockTest {

  private LogStorage.TransactionManager transactionManager;
  private SchedulerStore.Mutable schedulerStore;
  private JobStore.Mutable jobStore;
  private TaskStore.Mutable taskStore;
  private LockStore.Mutable lockStore;
  private QuotaStore.Mutable quotaStore;
  private AttributeStore.Mutable attributeStore;
  private JobUpdateStore.Mutable jobUpdateStore;
  private Logger log;
  private EventSink eventSink;
  private WriteAheadStorage storage;

  @Before
  public void setUp() {
    transactionManager = createMock(LogStorage.TransactionManager.class);
    schedulerStore = createMock(SchedulerStore.Mutable.class);
    jobStore = createMock(JobStore.Mutable.class);
    taskStore = createMock(TaskStore.Mutable.class);
    lockStore = createMock(LockStore.Mutable.class);
    quotaStore = createMock(QuotaStore.Mutable.class);
    attributeStore = createMock(AttributeStore.Mutable.class);
    jobUpdateStore = createMock(JobUpdateStore.Mutable.class);
    log = createMock(Logger.class);
    eventSink = createMock(EventSink.class);

    storage = new WriteAheadStorage(
        transactionManager,
        schedulerStore,
        jobStore,
        taskStore,
        lockStore,
        quotaStore,
        attributeStore,
        jobUpdateStore,
        log,
        eventSink);
  }

  private void expectOp(Op op) {
    expect(transactionManager.hasActiveTransaction()).andReturn(true);
    transactionManager.log(op);
  }

  @Test
  public void testPruneHistory() {
    Set<String> pruned = ImmutableSet.of("a", "b");
    expect(jobUpdateStore.pruneHistory(1, 1)).andReturn(pruned);
    expectOp(Op.pruneJobUpdateHistory(new PruneJobUpdateHistory(1, 1)));

    control.replay();

    storage.pruneHistory(1, 1);
  }

  @Test
  public void testNoopPruneHistory() {
    expect(jobUpdateStore.pruneHistory(1, 1)).andReturn(ImmutableSet.<String>of());

    control.replay();

    storage.pruneHistory(1, 1);
  }

  @Test
  public void testMutate() {
    Query.Builder query = Query.taskScoped("a");
    Function<IScheduledTask, IScheduledTask> mutator =
        createMock(new Clazz<Function<IScheduledTask, IScheduledTask>>() { });
    ImmutableSet<IScheduledTask> mutated = ImmutableSet.of(IScheduledTask.build(
            new ScheduledTask().setAssignedTask(new AssignedTask().setTaskId("a"))));

    expect(taskStore.mutateTasks(query, mutator)).andReturn(mutated);
    expect(log.isLoggable(Level.FINE)).andReturn(false);
    expectOp(Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(mutated))));

    // With increased logging.
    expect(taskStore.mutateTasks(query, mutator)).andReturn(mutated);
    expect(log.isLoggable(Level.FINE)).andReturn(true);
    expectOp(Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(mutated))));
    log.fine(EasyMock.anyString());

    control.replay();

    assertEquals(mutated, storage.mutateTasks(query, mutator));
    assertEquals(mutated, storage.mutateTasks(query, mutator));
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
  public void testDeleteLocks() {
    control.replay();
    storage.deleteLocks();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteAllUpdatesAndEvents() {
    control.replay();
    storage.deleteAllUpdatesAndEvents();
  }
}
