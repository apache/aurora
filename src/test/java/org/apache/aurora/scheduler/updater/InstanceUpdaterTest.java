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

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.apache.aurora.scheduler.updater.InstanceUpdater.Result;
import static org.apache.aurora.scheduler.updater.InstanceUpdater.Result.SUCCESS;
import static org.easymock.EasyMock.expectLastCall;

public class InstanceUpdaterTest extends EasyMockTest {
  private static final Optional<ITaskConfig> NO_CONFIG = Optional.absent();
  private static final Optional<IScheduledTask> NO_TASK = Optional.absent();

  private static final ITaskConfig OLD = ITaskConfig.build(new TaskConfig().setNumCpus(1.0));
  private static final ITaskConfig NEW = ITaskConfig.build(new TaskConfig().setNumCpus(2.0));

  private static final Amount<Long, Time> MIN_RUNNING_TIME = Amount.of(1L, Time.MINUTES);
  private static final Amount<Long, Time> MAX_NON_RUNNING_TIME = Amount.of(5L, Time.MINUTES);

  private FakeClock clock;
  private TaskController controller;
  private InstanceUpdater updater;

  @Before
  public void setUp() {
    clock = new FakeClock();
    controller = createMock(TaskController.class);
  }

  private void newUpdater(Optional<ITaskConfig> newConfig, int maxToleratedFailures) {
    updater = new InstanceUpdater(
        newConfig,
        maxToleratedFailures,
        MIN_RUNNING_TIME,
        MAX_NON_RUNNING_TIME,
        clock,
        controller);
  }

  private void newUpdater(ITaskConfig newConfig, int maxToleratedFailures) {
    newUpdater(Optional.of(newConfig), maxToleratedFailures);
  }

  private IScheduledTask makeTask(ITaskConfig config, ScheduleStatus status) {
    List<TaskEvent> events = Lists.newArrayList();
    if (status != PENDING) {
      events.add(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(PENDING));
    }
    if (Tasks.isTerminated(status) || status == KILLING) {
      events.add(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(ASSIGNED));
      events.add(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(RUNNING));
    }

    events.add(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(status));

    return IScheduledTask.build(
        new ScheduledTask()
            .setStatus(status)
            .setTaskEvents(ImmutableList.copyOf(events))
            .setAssignedTask(
                new AssignedTask()
                    .setTask(config.newBuilder())));
  }

  private void evaluate(ITaskConfig config, ScheduleStatus status, ScheduleStatus... statuses) {
    IScheduledTask task = makeTask(config, status);

    updater.evaluate(Optional.of(task));
    for (ScheduleStatus s : statuses) {
      ScheduledTask builder = task.newBuilder();
      builder.setStatus(s);
      builder.addToTaskEvents(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(s));
      task = IScheduledTask.build(builder);
      updater.evaluate(Optional.of(task));
    }
  }

  @Test
  public void testSuccessfulUpdate() {
    controller.killTask();
    controller.addReplacement();
    controller.reevaluteAfterRunningLimit();
    expectLastCall().times(4);
    controller.updateCompleted(SUCCESS);

    control.replay();

    newUpdater(NEW, 1);
    evaluate(OLD, RUNNING, KILLING, FINISHED);
    evaluate(NEW, PENDING, ASSIGNED, STARTING);
    IScheduledTask task = makeTask(NEW, RUNNING);
    updater.evaluate(Optional.of(task));
    clock.advance(MIN_RUNNING_TIME);
    updater.evaluate(Optional.of(task));
  }

  @Test
  public void testUpdateRetryOnTaskExit() {
    controller.killTask();
    controller.addReplacement();
    controller.reevaluteAfterRunningLimit();
    expectLastCall().times(8);
    controller.updateCompleted(SUCCESS);

    control.replay();

    newUpdater(NEW, 1);
    evaluate(OLD, RUNNING, KILLING, FINISHED);
    evaluate(NEW, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);
    evaluate(NEW, PENDING, ASSIGNED, STARTING);
    IScheduledTask task = makeTask(NEW, RUNNING);
    updater.evaluate(Optional.of(task));
    clock.advance(MIN_RUNNING_TIME);
    updater.evaluate(Optional.of(task));
  }

  @Test
  public void testUpdateRetryFailure() {
    controller.killTask();
    controller.addReplacement();
    controller.reevaluteAfterRunningLimit();
    expectLastCall().times(4);
    controller.updateCompleted(Result.FAILED);

    control.replay();

    newUpdater(NEW, 0);
    evaluate(OLD, RUNNING, KILLING, FINISHED);
    evaluate(NEW, PENDING, ASSIGNED, STARTING, RUNNING, FAILED);
  }

  @Test
  public void testNoopUpdate() {
    controller.updateCompleted(SUCCESS);

    control.replay();

    newUpdater(NEW, 1);
    IScheduledTask task = makeTask(NEW, RUNNING);
    clock.advance(MIN_RUNNING_TIME);
    updater.evaluate(Optional.of(task));
  }

  @Test
  public void testPointlessUpdate() {
    controller.updateCompleted(SUCCESS);

    control.replay();

    newUpdater(NO_CONFIG, 1);
    updater.evaluate(NO_TASK);
  }

  @Test
  public void testNoOldConfig() {
    controller.addReplacement();
    controller.reevaluteAfterRunningLimit();
    expectLastCall().times(4);
    controller.updateCompleted(SUCCESS);

    control.replay();

    newUpdater(NEW, 1);
    updater.evaluate(NO_TASK);
    evaluate(NEW, PENDING, ASSIGNED, STARTING);
    IScheduledTask task = makeTask(NEW, RUNNING);
    updater.evaluate(Optional.of(task));
    clock.advance(MIN_RUNNING_TIME);
    updater.evaluate(Optional.of(task));
  }

  @Test
  public void testNoNewConfig() {
    controller.killTask();
    controller.updateCompleted(SUCCESS);

    control.replay();

    newUpdater(NO_CONFIG, 1);
    evaluate(OLD, RUNNING, KILLING, FINISHED);
    updater.evaluate(NO_TASK);
  }

  @Test
  public void testAvoidsMultipleResults() {
    controller.killTask();
    controller.addReplacement();
    controller.reevaluteAfterRunningLimit();
    expectLastCall().times(4);
    controller.updateCompleted(SUCCESS);

    control.replay();

    newUpdater(NEW, 1);
    evaluate(OLD, RUNNING, KILLING, FINISHED);
    evaluate(NEW, PENDING, ASSIGNED, STARTING);
    IScheduledTask task = makeTask(NEW, RUNNING);
    updater.evaluate(Optional.of(task));
    clock.advance(MIN_RUNNING_TIME);
    updater.evaluate(Optional.of(task));
    // The extra evaluation should not result in another updateCompleted().
    updater.evaluate(Optional.of(task));
  }

  @Test
  public void testStuckInPending() {
    controller.killTask();
    controller.addReplacement();
    controller.reevaluteAfterRunningLimit();
    expectLastCall().times(2);
    controller.killTask();
    controller.addReplacement();
    controller.updateCompleted(Result.FAILED);

    control.replay();

    newUpdater(NEW, 1);
    evaluate(OLD, RUNNING, KILLING, FINISHED);
    evaluate(NEW, PENDING);
    IScheduledTask task1 = makeTask(NEW, PENDING);
    clock.advance(MAX_NON_RUNNING_TIME);
    updater.evaluate(Optional.of(task1));
    updater.evaluate(NO_TASK);
    evaluate(NEW, PENDING);
    IScheduledTask task2 = makeTask(NEW, PENDING);
    clock.advance(MAX_NON_RUNNING_TIME);
    updater.evaluate(Optional.of(task2));
  }

  @Test
  public void testStuckInKilling() {
    controller.killTask();
    controller.addReplacement();
    controller.reevaluteAfterRunningLimit();
    expectLastCall().times(6);
    controller.killTask();
    controller.addReplacement();
    controller.updateCompleted(Result.FAILED);

    control.replay();

    newUpdater(NEW, 1);
    evaluate(OLD, RUNNING, KILLING, FINISHED);
    evaluate(NEW, PENDING);
    IScheduledTask task1 = makeTask(NEW, PENDING);
    clock.advance(MAX_NON_RUNNING_TIME);
    updater.evaluate(Optional.of(task1));
    updater.evaluate(NO_TASK);
    evaluate(NEW, PENDING, ASSIGNED, STARTING, RUNNING);
    IScheduledTask task2 = makeTask(NEW, KILLING);
    updater.evaluate(Optional.of(task2));
    clock.advance(MAX_NON_RUNNING_TIME);
    updater.evaluate(Optional.of(task2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidInput() {
    control.replay();

    newUpdater(NEW, 1);
    IScheduledTask noEvents = IScheduledTask.build(
        makeTask(OLD, RUNNING).newBuilder().setTaskEvents(ImmutableList.<TaskEvent>of()));
    updater.evaluate(Optional.of(noEvents));
  }

  @Test
  public void testOldTaskDies() {
    // If the original task dies, the updater should not add a replacement if the task will be
    // resuscitated.  Only a task that has passed through KILLING will not be resuscitated.

    control.replay();

    newUpdater(NEW, 1);

    // Task did not pass through KILLING, therefore will be rescheudled.
    evaluate(OLD, FINISHED);
  }

  @Test
  public void testOldTaskDiesAndRescheduled() {
    // Identical to testOldTaskDies, with the follow-through of rescheduling and updating.
    // If the original task dies, the updater should not add a replacement if the task will be
    // resuscitated.  Only a task that has passed through KILLING will not be resuscitated.

    controller.killTask();
    controller.addReplacement();
    controller.reevaluteAfterRunningLimit();
    expectLastCall().times(4);
    controller.updateCompleted(SUCCESS);

    control.replay();

    newUpdater(NEW, 1);

    // Task did not pass through KILLING, therefore will be rescheudled.
    evaluate(OLD, FINISHED);
    evaluate(OLD, PENDING);
    updater.evaluate(NO_TASK);
    evaluate(NEW, PENDING, ASSIGNED, STARTING);
    IScheduledTask task = makeTask(NEW, RUNNING);
    updater.evaluate(Optional.of(task));
    clock.advance(MIN_RUNNING_TIME);
    updater.evaluate(Optional.of(task));
  }
}
