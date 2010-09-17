package com.twitter.mesos.scheduler;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TrackedTask;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * @author wfarner
 */
public class TaskStoreTest {

  private TaskStore taskStore;

  private static final int TASK_A_ID = 1;
  private static final ScheduleStatus TASK_A_STATUS = ScheduleStatus.PENDING;
  private TrackedTask taskA;

  private static final int TASK_B_ID = 2;
  private static final ScheduleStatus TASK_B_STATUS = ScheduleStatus.RUNNING;
  private TrackedTask taskB;

  private Iterable<TrackedTask> tasks;

  @Before
  public void setUp() throws Exception {
    taskStore = new TaskStore();
    taskA = makeTask(TASK_A_ID).setStatus(TASK_A_STATUS);
    taskB = makeTask(TASK_B_ID).setStatus(TASK_B_STATUS);
    tasks = Arrays.asList(taskA, taskB);
  }

  @Test
  public void testAddAndFetchTasks() {
    taskStore.add(tasks);

    assertThat(Iterables.getOnlyElement(taskStore.fetch(getById(1))), is(taskA));

    Predicate<TrackedTask> taskIdFilter = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return task.getTaskId() == 1;
      }
    };

    assertThat(Iterables.getOnlyElement(taskStore.fetch(new TaskQuery(), taskIdFilter)), is(taskA));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsTaskIdCollision() {
    taskStore.add(tasks);
    taskStore.add(Arrays.asList(makeTask(TASK_A_ID).setStatus(ScheduleStatus.FAILED)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsDuplicateTaskIds() {
    taskStore.add(Arrays.asList(makeTask(10), makeTask(10)));
  }

  @Test
  public void testImmutable() {
    taskStore.add(tasks);

    taskA.setStatus(ScheduleStatus.RUNNING);

    assertThat(Iterables.getOnlyElement(taskStore.fetch(getById(TASK_A_ID))).getStatus(),
        is(ScheduleStatus.PENDING));
  }

  @Test
  public void testMutate() {
    taskStore.add(tasks);

    // Mutate by query.
    taskStore.mutate(getById(TASK_A_ID), new Closure<TrackedTask>() {
        @Override public void execute(TrackedTask task) {
          task.setStatus(ScheduleStatus.RUNNING);
        }
      });
    assertThat(Iterables.getOnlyElement(taskStore.fetch(getById(TASK_A_ID))).getStatus(),
        is(ScheduleStatus.RUNNING));

    // Mutate by reference.
    TrackedTask immutable = Iterables.getOnlyElement(taskStore.fetch(getById(TASK_B_ID)));
    taskStore.mutate(immutable, new Closure<TrackedTask>() {
        @Override public void execute(TrackedTask task) throws RuntimeException {
          task.setStatus(ScheduleStatus.LOST);
        }
      });
    assertThat(Iterables.getOnlyElement(taskStore.fetch(getById(TASK_B_ID))).getStatus(),
        is(ScheduleStatus.LOST));
  }

  @Test
  public void testRemove() {
    taskStore.add(tasks);
    taskStore.remove(Arrays.asList(taskA));
    assertThat(Iterables.getOnlyElement(taskStore.fetch(new TaskQuery())), is(taskB));
  }

  private static TrackedTask makeTask(int taskId) {
    return new TrackedTask().setTaskId(taskId);
  }

  private static TaskQuery getById(int taskId) {
    return new TaskQuery().setTaskIds(Sets.newHashSet(taskId));
  }
}
