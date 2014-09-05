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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.EVALUATE_AFTER_MIN_RUNNING_MS;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.EVALUATE_ON_STATE_CHANGE;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.SUCCEEDED;
import static org.junit.Assert.assertEquals;

public class InstanceUpdaterTest {
  private static final Optional<ITaskConfig> NO_CONFIG = Optional.absent();

  private static final ITaskConfig OLD = ITaskConfig.build(new TaskConfig().setNumCpus(1.0));
  private static final ITaskConfig NEW = ITaskConfig.build(new TaskConfig().setNumCpus(2.0));

  private static final Amount<Long, Time> MIN_RUNNING_TIME = Amount.of(1L, Time.MINUTES);
  private static final Amount<Long, Time> MAX_NON_RUNNING_TIME = Amount.of(5L, Time.MINUTES);

  private static class TestFixture {
    private final FakeClock clock;
    private final InstanceUpdater updater;
    private final TaskUtil taskUtil;
    private Optional<IScheduledTask> task = Optional.absent();

    TestFixture(Optional<ITaskConfig> newConfig, int maxToleratedFailures) {
      this.clock = new FakeClock();
      this.updater = new InstanceUpdater(
          newConfig,
          maxToleratedFailures,
          MIN_RUNNING_TIME,
          MAX_NON_RUNNING_TIME,
          clock);
      this.taskUtil = new TaskUtil(clock);
    }

    TestFixture(ITaskConfig newConfig, int maxToleratedFailures) {
      this(Optional.of(newConfig), maxToleratedFailures);
    }

    void setActualState(ITaskConfig config) {
      this.task = Optional.of(taskUtil.makeTask(config, PENDING));
    }

    void setActualStateAbsent() {
      this.task = Optional.absent();
    }

    private Result changeStatusAndEvaluate(ScheduleStatus status) {
      ScheduledTask builder = task.get().newBuilder();
      if (builder.getStatus() != status) {
        // Only add a task event if this is a state change.
        builder.addToTaskEvents(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(status));
      }
      builder.setStatus(status);

      task = Optional.of(IScheduledTask.build(builder));
      return updater.evaluate(task);
    }

    void evaluateCurrentState(Result expectedResult) {
      assertEquals(expectedResult, updater.evaluate(task));
    }

    void evaluate(
        Result expectedResult,
        ScheduleStatus status,
        ScheduleStatus... statuses) {

      assertEquals(expectedResult, changeStatusAndEvaluate(status));
      for (ScheduleStatus s : statuses) {
        assertEquals(expectedResult, changeStatusAndEvaluate(s));
      }
    }

    void advanceTime(Amount<Long, Time> time) {
      clock.advance(time);
    }
  }

  @Test
  public void testSuccessfulUpdate() {
    TestFixture f = new TestFixture(NEW, 1);
    f.setActualState(OLD);
    f.evaluate(KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE, RUNNING);
    f.evaluate(EVALUATE_ON_STATE_CHANGE, KILLING);
    f.evaluate(REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE, FINISHED);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING, ASSIGNED, STARTING, RUNNING);
    f.advanceTime(MIN_RUNNING_TIME);
    f.evaluateCurrentState(SUCCEEDED);
  }

  @Test
  public void testUpdateRetryOnTaskExit() {
    TestFixture f = new TestFixture(NEW, 1);
    f.setActualState(OLD);
    f.evaluate(KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE, RUNNING);
    f.evaluate(EVALUATE_ON_STATE_CHANGE, KILLING);
    f.evaluate(REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE, FINISHED);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING, ASSIGNED, STARTING, RUNNING);
    f.evaluate(EVALUATE_ON_STATE_CHANGE, FAILED);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING, ASSIGNED, STARTING);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, RUNNING);
    f.advanceTime(MIN_RUNNING_TIME);
    f.evaluateCurrentState(SUCCEEDED);
  }

  @Test
  public void testUpdateRetryFailure() {
    TestFixture f = new TestFixture(NEW, 0);
    f.setActualState(OLD);
    f.evaluate(KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE, RUNNING);
    f.evaluate(EVALUATE_ON_STATE_CHANGE, KILLING);
    f.evaluate(REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE, FINISHED);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING, ASSIGNED, STARTING, RUNNING);
    f.evaluate(Result.FAILED, FAILED);
  }

  @Test
  public void testNoopUpdate() {
    TestFixture f = new TestFixture(NEW, 1);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, RUNNING);
    f.advanceTime(MIN_RUNNING_TIME);
    f.evaluate(SUCCEEDED, RUNNING);
  }

  @Test
  public void testPointlessUpdate() {
    TestFixture f = new TestFixture(NO_CONFIG, 1);
    f.setActualStateAbsent();
    f.evaluateCurrentState(SUCCEEDED);
  }

  @Test
  public void testNoOldConfig() {
    TestFixture f = new TestFixture(NEW, 1);
    f.setActualStateAbsent();
    f.evaluateCurrentState(REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING, ASSIGNED, STARTING, RUNNING);
    f.advanceTime(MIN_RUNNING_TIME);
    f.evaluateCurrentState(SUCCEEDED);
  }

  @Test
  public void testNoNewConfig() {
    TestFixture f = new TestFixture(NO_CONFIG, 1);
    f.setActualState(OLD);
    f.evaluate(KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE, RUNNING);
    f.evaluate(EVALUATE_ON_STATE_CHANGE, KILLING, FINISHED);
    f.setActualStateAbsent();
    f.evaluateCurrentState(SUCCEEDED);
  }

  @Test
  public void testStuckInPending() {
    TestFixture f = new TestFixture(NEW, 1);
    f.setActualState(OLD);
    f.evaluate(KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE, RUNNING);
    f.evaluate(EVALUATE_ON_STATE_CHANGE, KILLING);
    f.evaluate(REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE, FINISHED);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING);
    f.advanceTime(MAX_NON_RUNNING_TIME);
    f.evaluateCurrentState(KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE);
    f.setActualStateAbsent();
    f.evaluateCurrentState(REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING);
    f.advanceTime(MAX_NON_RUNNING_TIME);
    f.evaluateCurrentState(Result.FAILED);
  }

  @Test
  public void testStuckInKilling() {
    TestFixture f = new TestFixture(NEW, 1);
    f.setActualState(OLD);
    f.evaluate(KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE, RUNNING);
    f.evaluate(EVALUATE_ON_STATE_CHANGE, KILLING);
    f.evaluate(REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE, FINISHED);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING);
    f.advanceTime(MAX_NON_RUNNING_TIME);
    f.evaluateCurrentState(KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE);
    f.setActualStateAbsent();
    f.evaluateCurrentState(REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, ASSIGNED, STARTING, RUNNING);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, KILLING);
    f.advanceTime(MAX_NON_RUNNING_TIME);
    f.evaluateCurrentState(Result.FAILED);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidInput() {
    TestFixture f = new TestFixture(NEW, 1);
    ScheduledTask noEvents = new TaskUtil(new FakeClock())
        .makeTask(OLD, RUNNING).newBuilder().setTaskEvents(ImmutableList.<TaskEvent>of());
    f.updater.evaluate(Optional.of(IScheduledTask.build(noEvents)));
  }

  @Test
  public void testOldTaskDies() {
    // If the original task dies, the updater should not add a replacement if the task will be
    // resuscitated.  Only a task that has passed through KILLING will not be resuscitated.

    TestFixture f = new TestFixture(NEW, 1);
    f.setActualState(OLD);
    // Task did not pass through KILLING, therefore will be rescheduled.
    f.evaluate(EVALUATE_ON_STATE_CHANGE, FINISHED);
  }

  @Test
  public void testOldTaskDiesAndRescheduled() {
    // Identical to testOldTaskDies, with the follow-through of rescheduling and updating.
    // If the original task dies, the updater should not add a replacement if the task will be
    // resuscitated.  Only a task that has passed through KILLING will not be resuscitated.

    TestFixture f = new TestFixture(NEW, 1);
    f.setActualState(OLD);

    // Task did not pass through KILLING, therefore will be rescheduled.
    f.evaluate(EVALUATE_ON_STATE_CHANGE, FINISHED);
    f.evaluate(KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE, PENDING);
    f.setActualStateAbsent();
    f.evaluateCurrentState(REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE);
    f.setActualState(NEW);
    f.evaluate(EVALUATE_AFTER_MIN_RUNNING_MS, PENDING, ASSIGNED, STARTING, RUNNING);
    f.advanceTime(MIN_RUNNING_TIME);
    f.evaluateCurrentState(SUCCEEDED);
  }
}
