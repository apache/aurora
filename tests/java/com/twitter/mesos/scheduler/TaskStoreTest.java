package com.twitter.mesos.scheduler;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.twitter.common.base.Closure;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.*;
import com.twitter.mesos.scheduler.TaskStore.TaskState;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.twitter.mesos.gen.ScheduleStatus.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author wfarner
 */
public class TaskStoreTest {

  private TaskStore taskStore;

  private static final String TASK_A_ID = "fake-task-id-a";
  private static final ScheduleStatus TASK_A_STATUS = PENDING;
  private TaskState taskA;

  private static final String TASK_B_ID = "fake-task-id-b";
  private static final ScheduleStatus TASK_B_STATUS = RUNNING;
  private TaskState taskB;

  private Iterable<ScheduledTask> tasks;

  @Before
  public void setUp() throws Exception {
    taskStore = new MapTaskStore();
    taskA = new TaskState(makeTask(TASK_A_ID).setStatus(TASK_A_STATUS));
    taskB = new TaskState(makeTask(TASK_B_ID).setStatus(TASK_B_STATUS));
    tasks = Lists.transform(Arrays.asList(taskA, taskB), Tasks.STATE_TO_SCHEDULED);
  }

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

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsTaskIdCollision() {
    store(tasks);
    store(Arrays.asList(makeTask(TASK_A_ID).setStatus(FAILED)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsDuplicateTaskIds() {
    store(Arrays.asList(makeTask("asdf").setAssignedTask(new AssignedTask()
        .setTask(new TwitterTaskInfo().setOwner("A"))), makeTask("asdf")
        .setAssignedTask(new AssignedTask().setTask(new TwitterTaskInfo().setOwner("B")))));
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

  private void store(Iterable<ScheduledTask> tasks) {
    taskStore.add(ImmutableSet.copyOf(tasks));
  }

  private static ScheduledTask makeTask(String taskId) {
    return new ScheduledTask().setAssignedTask(
        new AssignedTask().setTaskId(taskId).setTask(new TwitterTaskInfo()));
  }
}
