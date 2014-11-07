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
package org.apache.aurora.scheduler.state;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.async.RescheduleCalculator;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.mem.MemStorage;
import org.apache.mesos.Protos.SlaveID;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IArgumentMatcher;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StateManagerImplTest extends EasyMockTest {

  private static final String HOST_A = "host_a";
  private static final Identity JIM = new Identity("jim", "jim-user");
  private static final String MY_JOB = "myJob";
  private static final IJobKey JOB_KEY = JobKeys.from(JIM.getRole(), "devel", MY_JOB);

  private Driver driver;
  private TaskIdGenerator taskIdGenerator;
  private EventSink eventSink;
  private RescheduleCalculator rescheduleCalculator;
  private StateManagerImpl stateManager;
  private final FakeClock clock = new FakeClock();
  private Storage storage;

  @Before
  public void setUp() throws Exception {
    taskIdGenerator = createMock(TaskIdGenerator.class);
    driver = createMock(Driver.class);
    eventSink = createMock(EventSink.class);
    rescheduleCalculator = createMock(RescheduleCalculator.class);
    // TODO(William Farner): Use a mocked storage.
    storage = MemStorage.newEmptyStorage();
    stateManager = new StateManagerImpl(
        storage,
        clock,
        driver,
        taskIdGenerator,
        eventSink,
        rescheduleCalculator);
  }

  private static class StateChangeMatcher implements IArgumentMatcher {
    private final String taskId;
    private final ScheduleStatus from;
    private final ScheduleStatus to;

    StateChangeMatcher(String taskId, ScheduleStatus from, ScheduleStatus to) {
      this.taskId = taskId;
      this.from = from;
      this.to = to;
    }

    @Override
    public boolean matches(Object argument) {
      if (!(argument instanceof TaskStateChange)) {
        return false;
      }

      TaskStateChange change = (TaskStateChange) argument;
      return taskId.equals(Tasks.id(change.getTask()))
          && (from == change.getOldState().get())
          && (to == change.getNewState());
    }

    @Override
    public void appendTo(StringBuffer buffer) {
      buffer.append(taskId).append(" ").append(from).append("->").append(to);
    }
  }

  PubsubEvent matchStateChange(String task, ScheduleStatus from, ScheduleStatus to) {
    EasyMock.reportMatcher(new StateChangeMatcher(task, from, to));
    return null;
  }

  private static class DeletedTasksMatcher implements IArgumentMatcher {
    private final Set<String> taskIds;

    DeletedTasksMatcher(String taskId, String... taskIds) {
      this.taskIds = ImmutableSet.<String>builder().add(taskId).add(taskIds).build();
    }

    @Override
    public boolean matches(Object argument) {
      if (!(argument instanceof TasksDeleted)) {
        return false;
      }

      TasksDeleted deleted = (TasksDeleted) argument;
      return taskIds.equals(Tasks.ids(deleted.getTasks()));
    }

    @Override
    public void appendTo(StringBuffer buffer) {
      buffer.append(taskIds);
    }
  }

  PubsubEvent matchTasksDeleted(String id, String... ids) {
    EasyMock.reportMatcher(new DeletedTasksMatcher(id, ids));
    return null;
  }

  @Test
  public void testAddTasks() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 3)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);

    control.replay();

    insertTask(task, 3);
    ScheduledTask expected = new ScheduledTask()
        .setStatus(PENDING)
        .setTaskEvents(ImmutableList.of(new TaskEvent()
            .setTimestamp(clock.nowMillis())
            .setScheduler(StateManagerImpl.LOCAL_HOST_SUPPLIER.get())
            .setStatus(PENDING)))
        .setAssignedTask(new AssignedTask()
            .setInstanceId(3)
            .setTaskId(taskId)
            .setTask(task.newBuilder()));
    assertEquals(ImmutableSet.of(IScheduledTask.build(expected)),
        Storage.Util.consistentFetchTasks(storage, Query.taskScoped(taskId)));
  }

  @Test
  public void testKillPendingTask() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);
    eventSink.post(matchTasksDeleted(taskId));

    control.replay();

    insertTask(task, 0);
    assertEquals(true, changeState(taskId, KILLING));
    assertEquals(false, changeState(taskId, KILLING));
  }

  @Test
  public void testKillRunningTask() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, RUNNING, KILLING, KILLED);
    driver.killTask(EasyMock.<String>anyObject());

    control.replay();

    insertTask(task, 0);
    assignTask(taskId, HOST_A);
    assertEquals(true, changeState(taskId, RUNNING));
    assertEquals(true, changeState(taskId, KILLING));
    assertEquals(true, changeState(taskId, KILLED));
    assertEquals(false, changeState(taskId, KILLED));
  }

  @Test
  public void testLostKillingTask() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, RUNNING, KILLING, LOST);

    driver.killTask(EasyMock.<String>anyObject());

    control.replay();

    insertTask(task, 0);

    assignTask(taskId, HOST_A);
    changeState(taskId, RUNNING);
    changeState(taskId, KILLING);
    changeState(taskId, LOST);
  }

  @Test
  public void testNestedEvents() {
    final String id = "a";
    ITaskConfig task = makeTask(JIM, MY_JOB);
    expect(taskIdGenerator.generate(task, 0)).andReturn(id);

    // Trigger an event that produces a side-effect and a PubSub event .
    eventSink.post(matchStateChange(id, INIT, PENDING));
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override
      public Void answer() throws Throwable {
        changeState(id, ASSIGNED);
        return null;
      }
    });

    // Final event sink execution that adds no side effect or event.
    expectStateTransitions(id, PENDING, ASSIGNED);

    control.replay();

    insertTask(task, 0);
  }

  @Test
  public void testDeletePendingTask() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);
    eventSink.post(matchTasksDeleted(taskId));

    control.replay();

    insertTask(task, 0);
    changeState(taskId, KILLING);
  }

  @Test
  public void testThrottleTask() {
    ITaskConfig task = ITaskConfig.build(makeTask(JIM, MY_JOB).newBuilder().setIsService(true));
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, RUNNING, FAILED);
    String newTaskId = "b";
    expect(taskIdGenerator.generate(task, 0)).andReturn(newTaskId);
    expect(rescheduleCalculator.getFlappingPenaltyMs(EasyMock.<IScheduledTask>anyObject()))
        .andReturn(100L);
    expectStateTransitions(newTaskId, INIT, THROTTLED);

    control.replay();

    insertTask(task, 0);
    changeState(taskId, ASSIGNED);
    changeState(taskId, RUNNING);
    changeState(taskId, FAILED);
  }

  @Test
  public void testKillUnknownTask() {
    String unknownTask = "unknown";

    driver.killTask(unknownTask);

    control.replay();

    changeState(unknownTask, RUNNING);
  }

  private void noFlappingPenalty() {
    expect(rescheduleCalculator.getFlappingPenaltyMs(EasyMock.<IScheduledTask>anyObject()))
        .andReturn(0L);
  }

  @Test
  public void testIncrementFailureCount() {
    ITaskConfig task = ITaskConfig.build(makeTask(JIM, MY_JOB).newBuilder().setIsService(true));
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, RUNNING, FAILED);

    String taskId2 = "a2";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId2);
    noFlappingPenalty();
    expectStateTransitions(taskId2, INIT, PENDING);

    control.replay();

    insertTask(task, 0);

    assignTask(taskId, HOST_A);
    changeState(taskId, RUNNING);
    changeState(taskId, FAILED);
    IScheduledTask rescheduledTask = Iterables.getOnlyElement(
        Storage.Util.consistentFetchTasks(storage, Query.taskScoped(taskId2)));
    assertEquals(taskId, rescheduledTask.getAncestorId());
    assertEquals(1, rescheduledTask.getFailureCount());
  }

  @Test
  public void testCasTaskPresent() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, FAILED);

    control.replay();

    insertTask(task, 0);
    assignTask(taskId, HOST_A);
    assertFalse(stateManager.changeState(
        taskId,
        Optional.of(PENDING),
        RUNNING,
        Optional.<String>absent()));
    assertTrue(stateManager.changeState(
        taskId,
        Optional.of(ASSIGNED),
        FAILED,
        Optional.<String>absent()));
  }

  @Test
  public void testCasTaskNotFound() {
    control.replay();

    assertFalse(stateManager.changeState(
        "a",
        Optional.of(PENDING),
        ASSIGNED,
        Optional.<String>absent()));
  }

  @Test
  public void testDeleteTasks() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, RUNNING, FINISHED);
    eventSink.post(matchTasksDeleted(taskId));

    control.replay();

    insertTask(task, 0);
    assignTask(taskId, HOST_A);
    changeState(taskId, RUNNING);
    changeState(taskId, FINISHED);
    stateManager.deleteTasks(ImmutableSet.of(taskId));
  }

  @Test
  public void testPortResource() throws Exception {
    Set<String> requestedPorts = ImmutableSet.of("one", "two", "three");
    ITaskConfig task = ITaskConfig.build(makeTask(JIM, MY_JOB).newBuilder()
        .setRequestedPorts(requestedPorts));

    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED);

    control.replay();

    insertTask(task, 0);
    assignTask(taskId, HOST_A, ImmutableSet.of(80, 81, 82));

    IScheduledTask actual = Iterables.getOnlyElement(
        Storage.Util.consistentFetchTasks(storage, Query.taskScoped(taskId)));

    assertEquals(
        requestedPorts,
        actual.getAssignedTask().getTask().getRequestedPorts());
  }

  @Test
  public void testPortResourceResetAfterReschedule() throws Exception {
    Set<String> requestedPorts = ImmutableSet.of("one");
    ITaskConfig task = ITaskConfig.build(makeTask(JIM, MY_JOB).newBuilder()
        .setRequestedPorts(requestedPorts));

    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, RUNNING, LOST);

    String newTaskId = "b";
    expect(taskIdGenerator.generate(task, 0)).andReturn(newTaskId);
    expectStateTransitions(newTaskId, INIT, PENDING, ASSIGNED);
    noFlappingPenalty();

    control.replay();

    insertTask(task, 0);
    assignTask(taskId, HOST_A, ImmutableSet.of(80));
    changeState(taskId, RUNNING);
    changeState(taskId, LOST);

    assignTask(newTaskId, HOST_A, ImmutableSet.of(86));

    IScheduledTask actual = Iterables.getOnlyElement(
        Storage.Util.consistentFetchTasks(storage, Query.taskScoped(newTaskId)));

    assertEquals(ImmutableMap.of("one", 86), actual.getAssignedTask().getAssignedPorts());
  }

  @Test(expected = IllegalArgumentException.class)
  public void insertEmptyPendingInstancesFails() {
    control.replay();
    stateManager.insertPendingTasks(makeTask(JIM, MY_JOB), ImmutableSet.<Integer>of());
  }

  @Test(expected = IllegalArgumentException.class)
  public void insertPendingInstancesInstanceCollision() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId).times(2);
    expectStateTransitions(taskId, INIT, PENDING);

    control.replay();

    insertTask(task, 0);
    Iterables.getOnlyElement(Storage.Util.consistentFetchTasks(storage, Query.taskScoped(taskId)));

    insertTask(task, 0);
  }

  private void expectStateTransitions(
      String taskId,
      ScheduleStatus initial,
      ScheduleStatus next,
      ScheduleStatus... others) {

    List<ScheduleStatus> statuses = ImmutableList.<ScheduleStatus>builder()
        .add(initial)
        .add(next)
        .add(others)
        .build();
    PeekingIterator<ScheduleStatus> it = Iterators.peekingIterator(statuses.iterator());
    while (it.hasNext()) {
      ScheduleStatus cur = it.next();
      try {
        eventSink.post(matchStateChange(taskId, cur, it.peek()));
      } catch (NoSuchElementException e) {
        // Expected.
      }
    }
  }

  private void insertTask(ITaskConfig task, int instanceId) {
    stateManager.insertPendingTasks(task, ImmutableSet.of(instanceId));
  }

  private boolean changeState(String taskId, ScheduleStatus status) {
    return stateManager.changeState(
        taskId,
        Optional.<ScheduleStatus>absent(),
        status,
        Optional.<String>absent());
  }

  private static ITaskConfig makeTask(Identity owner, String job) {
    return ITaskConfig.build(new TaskConfig()
        .setJob(new JobKey(owner.getRole(), "devel", job))
        .setOwner(owner)
        .setEnvironment("devel")
        .setJobName(job)
        .setRequestedPorts(ImmutableSet.<String>of()));
  }

  private void assignTask(String taskId, String host) {
    assignTask(taskId, host, ImmutableSet.<Integer>of());
  }

  private void assignTask(String taskId, String host, Set<Integer> ports) {
    stateManager.assignTask(taskId, host, SlaveID.newBuilder().setValue(host).build(), ports);
  }
}
