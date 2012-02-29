package com.twitter.mesos.scheduler.storage;

import java.util.Arrays;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.testing.junit4.TearDownTestCase;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Query;

import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * A basic test that verifies a {@link TaskStore} implementation conforms to expected behavior.
 *
 * @author William Farner
 */
public abstract class BaseTaskStoreTest<T extends TaskStore> extends TearDownTestCase {

  protected T store;

  private static final String TASK_A_ID = "fake-task-id-a";
  private static final ScheduleStatus TASK_A_STATUS = PENDING;
  private ScheduledTask taskA;

  private static final String TASK_B_ID = "fake-task-id-b";
  private static final ScheduleStatus TASK_B_STATUS = RUNNING;
  private ScheduledTask taskB;

  private Iterable<ScheduledTask> tasks;

  @Before
  public void setUp() throws Exception {
    store = createTaskStore();
    taskA = makeTask(TASK_A_ID).setStatus(TASK_A_STATUS);
    taskB = makeTask(TASK_B_ID).setStatus(TASK_B_STATUS);
    tasks = Arrays.asList(taskA, taskB);
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
  public void testQueryByTaskId() {
    store(ImmutableList.of(makeTask("task1"), makeTask("task2"), makeTask("task3")));

    assertEquals(ImmutableSet.of("task1", "task2", "task3"),
        store.fetchTaskIds(new TaskQuery().setTaskIds(null)));

    // SchedulerCoreImpl currently requires the semantics that match [] ids == never match
    assertEquals(ImmutableSet.<String>of(),
        store.fetchTaskIds(new TaskQuery().setTaskIds(ImmutableSet.<String>of())));

    assertEquals(ImmutableSet.of("task1"),
        store.fetchTaskIds(new TaskQuery().setTaskIds(ImmutableSet.of("task1"))));
  }

  @Test
  public void testAddAndFetchTasks() {
    store(tasks);
    assertThat(Iterables.getOnlyElement(store.fetchTasks(Query.byId(TASK_A_ID))), is(taskA));
  }

  @Test(expected = IllegalStateException.class)
  public void testRejectsDuplicateTaskIds() {
    ScheduledTask first = makeTask("asdf");
    first.getAssignedTask().getTask().setOwner(new Identity("A", "A"));

    ScheduledTask second = makeTask("asdf");
    second.getAssignedTask().getTask().setOwner(new Identity("B", "B"));

    store(Arrays.asList(first, second));
  }

  @Test
  public void testImmutable() {
    store(tasks);

    taskA.setStatus(RUNNING);

    assertThat(Iterables.getOnlyElement(store.fetchTasks(Query.byId(TASK_A_ID))).getStatus(),
        is(PENDING));
  }

  @Test
  public void testMutate() {
    store(tasks);

    // Mutate by query.
    store.mutateTasks(Query.byId(TASK_A_ID), new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.setStatus(RUNNING);
      }
    });
    assertThat(Iterables.getOnlyElement(store.fetchTasks(Query.byId(TASK_A_ID))).getStatus(),
        is(RUNNING));

    store.mutateTasks(Query.byId(TASK_B_ID), new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.setStatus(LOST);
      }
    });
    assertThat(Iterables.getOnlyElement(store.fetchTasks(Query.byId(TASK_B_ID))).getStatus(),
        is(LOST));
  }

  @Test
  public void testSave() {
    store(tasks);

    store.saveTasks(ImmutableSet.<ScheduledTask>of(taskA));
    assertThat(Iterables.getOnlyElement(store.fetchTasks(Query.byId(TASK_A_ID))), is(taskA));

    ScheduledTask updated = taskA.deepCopy();
    updated.setStatus(ScheduleStatus.FAILED);
    updated.setAncestorId("parent");
    store.saveTasks(ImmutableSet.<ScheduledTask>of(updated));
    assertThat(Iterables.getOnlyElement(store.fetchTasks(Query.byId(TASK_A_ID))), is(updated));
  }

  @Test
  public void testRemove() {
    store(tasks);
    store.removeTasks(Sets.newHashSet(taskA.getAssignedTask().getTaskId()));
    assertThat(Iterables.getOnlyElement(store.fetchTasks(Query.GET_ALL)), is(taskB));
  }

  protected void store(Iterable<ScheduledTask> tasks) {
    store.saveTasks(ImmutableSet.copyOf(tasks));
  }

  protected static ScheduledTask makeTask(String taskId) {
    TwitterTaskInfo taskInfo =
        new TwitterTaskInfo()
            .setOwner(new Identity("jake", "jake"))
            .setJobName("spin")
            .setShardId(42);
    AssignedTask assignedTask =
        new AssignedTask().setTaskId(taskId).setTask(taskInfo).setSlaveHost("localhost");
    return new ScheduledTask().setAssignedTask(assignedTask).setStatus(ScheduleStatus.STARTING);
  }
}
