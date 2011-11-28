package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.easymock.EasyMock;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.PREEMPTING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author William Farner
 */
public class StateManagerTest extends BaseStateManagerTest {

  private static final String HOST_A = "host_a";

  @Test
  public void testAbandonRunningTask() {
    control.replay();

    Set<String> taskIds = insertTasks(
        makeTask("jim", "myJob", 0),
        makeTask("jack", "otherJob", 0));
    assertVarCount("jim", "myJob", PENDING, 1);
    assertVarCount("jack", "otherJob", PENDING, 1);

    String task1 = Iterables.get(taskIds, 0);
    assignTask(task1, HOST_A);
    assertVarCount("jim", "myJob", PENDING, 0);
    assertVarCount("jim", "myJob", ASSIGNED, 1);
    changeState(task1, RUNNING);
    assertVarCount("jim", "myJob", ASSIGNED, 0);
    assertVarCount("jim", "myJob", RUNNING, 1);
    stateManager.abandonTasks(ImmutableSet.of(task1));
    assertVarCount("jim", "myJob", RUNNING, 0);
    assertVarCount("jim", "myJob", LOST, 0);
    assertVarCount("jim", "myJob", PENDING, 1);
    assertVarCount("jim", "myJob", UNKNOWN, 0);
    assertTrue(stateManager.fetchTasks(Query.byId(task1)).isEmpty());
  }

  @Test
  public void testAbandonFinishedTask() {
    control.replay();

    Set<String> taskIds = insertTasks(
        makeTask("jim", "myJob", 0),
        makeTask("jack", "otherJob", 0));
    String task1 = Iterables.get(taskIds, 0);
    assignTask(task1, HOST_A);
    changeState(task1, RUNNING);
    changeState(task1, FINISHED);
    assertVarCount("jim", "myJob", FINISHED, 1);
    stateManager.abandonTasks(ImmutableSet.of(task1));
    assertVarCount("jim", "myJob", FINISHED, 0);
    assertTrue(stateManager.fetchTasks(Query.byId(task1)).isEmpty());
  }

  @Test
  public void testKillPendingTask() {
    control.replay();

    String taskId = insertTask(makeTask("jim", "myJob", 0));
    assertVarCount("jim", "myJob", PENDING, 1);
    changeState(taskId, KILLING);
    assertVarCount("jim", "myJob", PENDING, 0);
    assertVarCount("jim", "myJob", KILLING, 0);
    assertVarCount("jim", "myJob", UNKNOWN, 0);
  }

  @Test
  public void testLostKillingTask() {
    killTaskCallback.execute(EasyMock.<String>anyObject());

    control.replay();

    String taskId = insertTask(makeTask("jim", "myJob", 0));

    assignTask(taskId, HOST_A);
    changeState(taskId, RUNNING);
    changeState(taskId, KILLING);
    assertVarCount("jim", "myJob", KILLING, 1);
    changeState(taskId, UNKNOWN);
    assertVarCount("jim", "myJob", KILLING, 0);
    assertVarCount("jim", "myJob", UNKNOWN, 0);
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
      String taskId = insertTask(makeTask("jim", "lost_" + finalState, 0));

      for (ScheduleStatus prepState : testCases.get(finalState)) {
        changeState(taskId, prepState);
      }

      clock.advance(StateManager.MISSING_TASK_GRACE_PERIOD.get());
      clock.advance(Amount.of(1L, Time.MILLISECONDS));
      stateManager.scanOutstandingTasks();
    }
  }

  @Test
  public void testInitNormallyHidden() throws Exception {
    control.replay();

    insertTask(makeTask("jim", "myJob", 0));
    assertVarCount("jim", "myJob", PENDING, 1);
  }

  @Test
  public void testTracksInit() throws Exception {
    final TwitterTaskInfo task = makeTask("jim", "myJob", 0);

    // Insert a task in the INIT state, and restart the state manager.
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override
      protected void execute(StoreProvider storeProvider) {
        storeProvider.getTaskStore()
            .saveTasks(ImmutableSet.of(stateManager.taskCreator.apply(task)));
      }
    });
    stateManager = createStateManager(storage);

    control.replay();

    assertVarCount("jim", "myJob", INIT, 1);
  }

  @Test
  public void testTracksUnknown() throws Exception {
    final TwitterTaskInfo task = makeTask("jim", "myJob", 0);

    // Insert a task in the INIT state, and restart the state manager.
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
        ScheduledTask scheduledTask = stateManager.taskCreator.apply(task);
        scheduledTask.setStatus(UNKNOWN);
        storeProvider.getTaskStore()
            .saveTasks(ImmutableSet.of(scheduledTask));
      }
    });
    stateManager = createStateManager(storage);

    control.replay();

    assertVarCount("jim", "myJob", UNKNOWN, 1);
  }


  @Test
  public void testUpdateRackName() throws Exception {
    final TwitterTaskInfo task = makeTask("jim", "myJob", 0);

    // Insert a task in the INIT state, and set a fake slave host.
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
        ScheduledTask scheduledTask = stateManager.taskCreator.apply(task);
        scheduledTask.getAssignedTask().setSlaveHost("slca-aaa-01-sr1");
        storeProvider.getTaskStore().saveTasks(ImmutableSet.of(scheduledTask));
      }
    });
    stateManager = createStateManager(storage);

    // Check if the rack name is updated after the state manger restarts.
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override
      protected void execute(StoreProvider storeProvider) {
        ScheduledTask result = Iterables.getOnlyElement(
            storeProvider.getTaskStore().fetchTasks(Query.GET_ALL));

        assertEquals("aaa", result.getAssignedTask().getRackName());
      }
    });
    control.replay();
  }

  private String insertTask(TwitterTaskInfo task) {
    return Iterables.getOnlyElement(insertTasks(task));
  }

  private Set<String> insertTasks(TwitterTaskInfo... tasks) {
    return stateManager.insertTasks(ImmutableSet.copyOf(tasks));
  }

  private void assertVarCount(String owner, String job, ScheduleStatus status, long expected) {
    assertEquals(expected, stateManager.getTaskCounter(Tasks.jobKey(owner, job), status));
  }

  private void changeState(String taskId, ScheduleStatus status) {
    stateManager.changeState(Query.byId(taskId), status);
  }
}
