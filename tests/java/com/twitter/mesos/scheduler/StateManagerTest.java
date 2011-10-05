package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

import org.easymock.EasyMock;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.gen.ScheduleStatus;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.PREEMPTING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertTrue;

/**
 * @author William Farner
 */
public class StateManagerTest extends BaseStateManagerTest {

  private static final String HOST_A = "host_a";

  @Test
  public void testAbandonRunningTask() {
    control.replay();

    Set<String> taskIds = stateManager.insertTasks(ImmutableSet.of(
        makeTask("jim", "myJob", 0),
        makeTask("jack", "otherJob", 0)
    ));
    String task1 = Iterables.get(taskIds, 0);
    assignTask(task1, HOST_A);
    changeState(task1, RUNNING);
    stateManager.abandonTasks(ImmutableSet.of(task1));
    assertTrue(stateManager.fetchTasks(Query.byId(task1)).isEmpty());
  }

  @Test
  public void testAbandonFinishedTask() {
    control.replay();

    Set<String> taskIds = stateManager.insertTasks(ImmutableSet.of(
        makeTask("jim", "myJob", 0),
        makeTask("jack", "otherJob", 0)
    ));
    String task1 = Iterables.get(taskIds, 0);
    assignTask(task1, HOST_A);
    changeState(task1, RUNNING);
    changeState(task1, FINISHED);
    stateManager.abandonTasks(ImmutableSet.of(task1));
    assertTrue(stateManager.fetchTasks(Query.byId(task1)).isEmpty());
  }

  @Test
  public void testTimedoutTask() {
    Multimap<ScheduleStatus, ScheduleStatus> testCases =
        ImmutableMultimap.<ScheduleStatus, ScheduleStatus>builder()
            .putAll(ASSIGNED)
            .putAll(STARTING, ASSIGNED)
            .putAll(PREEMPTING, ASSIGNED, RUNNING)
            .putAll(RESTARTING, ASSIGNED, RUNNING)
            .putAll(KILLING, ASSIGNED, RUNNING)
            .build();

    killTaskCallback.execute(EasyMock.<String>anyObject());
    expectLastCall().times(testCases.keySet().size());

    control.replay();

    for (ScheduleStatus finalState : testCases.keySet()) {
      String taskId = Iterables.getOnlyElement(stateManager.insertTasks(ImmutableSet.of(
          makeTask("jim", "lost_" + finalState, 0)
      )));

      for (ScheduleStatus prepState : testCases.get(finalState)) {
        changeState(taskId, prepState);
      }

      clock.advance(StateManager.MISSING_TASK_GRACE_PERIOD.get());
      clock.advance(Amount.of(1L, Time.MILLISECONDS));
      stateManager.scanOutstandingTasks();
    }
  }

  private void changeState(String taskId, ScheduleStatus status) {
    stateManager.changeState(Query.byId(taskId), status);
  }
}
