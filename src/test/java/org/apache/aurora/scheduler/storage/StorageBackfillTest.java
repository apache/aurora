/**
 * Copyright 2014 Apache Software Foundation
 *
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


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.mem.MemStorage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StorageBackfillTest {
  private Storage storage;
  private FakeClock clock;

  @Before
  public void setUp() {
    storage = MemStorage.newEmptyStorage();
    clock = new FakeClock();
  }

  @Test
  public void testRewriteUnknownState() {
    final String taskId = "id";
    final ScheduledTask task = makeTask(taskId).setStatus(ScheduleStatus.UNKNOWN);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.FINISHED));
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.UNKNOWN));

    final ScheduledTask expected = makeTask(taskId).setStatus(ScheduleStatus.FINISHED);
    expected.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.FINISHED));

    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(IScheduledTask.build(task)));
        StorageBackfill.backfill(storeProvider, clock);
      }
    });

    assertEquals(
        IScheduledTask.build(expected),
        Iterables.getOnlyElement(Storage.Util.consistentFetchTasks(
            storage,
            Query.taskScoped(taskId))));
  }

  @Test
  public void testRewriteUnknownToFailed() {
    final String taskId = "id";
    final ScheduledTask task = makeTask(taskId).setStatus(ScheduleStatus.UNKNOWN);
    task.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.UNKNOWN));

    final ScheduledTask expected = makeTask(taskId).setStatus(ScheduleStatus.FAILED);
    expected.addToTaskEvents(new TaskEvent(clock.nowMillis(), ScheduleStatus.FAILED));

    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(IScheduledTask.build(task)));
        StorageBackfill.backfill(storeProvider, clock);
      }
    });

    assertEquals(
        IScheduledTask.build(expected),
        Iterables.getOnlyElement(Storage.Util.consistentFetchTasks(
            storage,
            Query.taskScoped(taskId))));
  }

  private ScheduledTask makeTask(String id) {
    TaskConfig config = new TaskConfig()
        .setOwner(new Identity("user", "role"))
        .setEnvironment("test")
        .setJobName("jobName")
        .setProduction(false)
        .setRequestedPorts(ImmutableSet.<String>of())
        .setTaskLinks(ImmutableMap.<String, String>of())
        .setMaxTaskFailures(1)
        .setConstraints(ImmutableSet.of(
            new Constraint("host", TaskConstraint.limit(new LimitConstraint(1)))));

    ScheduledTask task = new ScheduledTask().setAssignedTask(new AssignedTask().setTask(config));
    task.getAssignedTask().setTaskId(id);

    return task;
  }
}
