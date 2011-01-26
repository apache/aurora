package com.twitter.mesos.scheduler;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.testing.junit4.TearDownTestCase;
import com.twitter.common.base.Closure;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.TaskStore.TaskState;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * A basic test that verifies a {@link TaskStore} implementation conforms to expected behavior.
 *
 * @author wfarner
 */
public abstract class BaseTaskStoreTest<T extends TaskStore> extends TearDownTestCase {

  protected T taskStore;

  private static final String TASK_A_ID = "fake-task-id-a";
  private static final ScheduleStatus TASK_A_STATUS = PENDING;
  private TaskState taskA;

  private static final String TASK_B_ID = "fake-task-id-b";
  private static final ScheduleStatus TASK_B_STATUS = RUNNING;
  private TaskState taskB;

  private Iterable<ScheduledTask> tasks;

  @Before
  public void setUp() throws Exception {
    taskStore = createTaskStore();
    taskA = new TaskState(makeTask(TASK_A_ID).setStatus(TASK_A_STATUS));
    taskB = new TaskState(makeTask(TASK_B_ID).setStatus(TASK_B_STATUS));
    tasks = Lists.transform(Arrays.asList(taskA, taskB), Tasks.STATE_TO_SCHEDULED);
  }

  /**
   * Subclasses should create the {@code TaskStore} implementation to exercise in tests.  This
   * method will be called as part of each test method's set up.
   *
   * @return the {@code TaskStore} to test
   * @throws Exception if there is a problem creating the task store
   */
  protected abstract T createTaskStore() throws Exception;

  @Test
  public void testAddAndFetchTasks() {
    store(tasks);

    assertThat(Iterables.getOnlyElement(taskStore.fetch(Query.byId(TASK_A_ID))), is(taskA));

    Predicate<TaskState> taskIdFilter = new Predicate<TaskState>() {
      @Override public boolean apply(TaskState state) {
        return state.task.getAssignedTask().getTaskId().equals(TASK_A_ID);
      }
    };

    assertThat(Iterables
        .getOnlyElement(taskStore.fetch(new Query(new TaskQuery(), taskIdFilter))), is(taskA));
  }

  @Test(expected = IllegalStateException.class)
  public void testRejectsTaskIdCollision() {
    store(tasks);
    store(Arrays.asList(makeTask(TASK_A_ID).setStatus(FAILED)));
  }

  @Test(expected = IllegalStateException.class)
  public void testRejectsDuplicateTaskIds() {
    ScheduledTask first = makeTask("asdf");
    first.getAssignedTask().getTask().setOwner("A");

    ScheduledTask second = makeTask("asdf");
    second.getAssignedTask().getTask().setOwner("B");

    store(Arrays.asList(first, second));
  }

  @Test
  public void testImmutable() {
    store(tasks);

    taskA.task.setStatus(RUNNING);

    assertThat(Iterables.getOnlyElement(taskStore.fetch(Query.byId(TASK_A_ID))).task
        .getStatus(), is(PENDING));
  }

  @Test
  public void testMutate() {
    store(tasks);

    // Mutate by query.
    taskStore.mutate(Query.byId(TASK_A_ID), new Closure<TaskState>() {
        @Override public void execute(TaskState state) {
          state.task.setStatus(RUNNING);
        }
      });
    assertThat(Iterables.getOnlyElement(taskStore.fetch(Query.byId(TASK_A_ID))).task.getStatus(),
        is(RUNNING));

    taskStore.mutate(Query.byId(TASK_B_ID), new Closure<TaskState>() {
        @Override public void execute(TaskState state) throws RuntimeException {
          state.task.setStatus(LOST);
        }
      });
    assertThat(Iterables.getOnlyElement(taskStore.fetch(Query.byId(TASK_B_ID))).task.getStatus(),
        is(LOST));
  }

  @Test
  public void testRemove() {
    store(tasks);
    taskStore.remove(Sets.newHashSet(taskA.task.getAssignedTask().getTaskId()));
    assertThat(Iterables.getOnlyElement(taskStore.fetch(Query.GET_ALL)), is(taskB));
  }

  protected void store(Iterable<ScheduledTask> tasks) {
    taskStore.add(ImmutableSet.copyOf(tasks));
  }

  protected static ScheduledTask makeTask(String taskId) {
    TwitterTaskInfo taskInfo =
        new TwitterTaskInfo().setOwner("jake").setJobName("spin").setShardId(42);
    AssignedTask assignedTask =
        new AssignedTask().setTaskId(taskId).setTask(taskInfo).setSlaveHost("localhost");
    return new ScheduledTask().setAssignedTask(assignedTask).setStatus(ScheduleStatus.STARTING);
  }
}
