package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Test;

import com.twitter.mesos.gen.ScheduleStatus;

import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
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
    changeState(task1, ScheduleStatus.FINISHED);
    stateManager.abandonTasks(ImmutableSet.of(task1));
    assertTrue(stateManager.fetchTasks(Query.byId(task1)).isEmpty());
  }

  private void changeState(String taskId, ScheduleStatus status) {
    stateManager.changeState(Query.byId(taskId), status);
  }
}
