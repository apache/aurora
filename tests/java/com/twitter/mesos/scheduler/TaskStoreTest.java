package com.twitter.mesos.scheduler;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.List;

import static com.twitter.mesos.gen.ScheduleStatus.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author wfarner
 */
public class TaskStoreTest {

  private TaskStore taskStore;

  private static final int TASK_A_ID = 1;
  private static final ScheduleStatus TASK_A_STATUS = PENDING;
  private TaskState taskA;

  private static final int TASK_B_ID = 2;
  private static final ScheduleStatus TASK_B_STATUS = RUNNING;
  private TaskState taskB;

  private Iterable<ScheduledTask> tasks;

  @Before
  public void setUp() throws Exception {
    taskStore = new TaskStore();
    taskA = new TaskState(makeTask(TASK_A_ID).setStatus(TASK_A_STATUS));
    taskB = new TaskState(makeTask(TASK_B_ID).setStatus(TASK_B_STATUS));
    tasks = Lists.transform(Arrays.asList(taskA, taskB), Tasks.STATE_TO_SCHEDULED);
  }

  @Test
  public void testAddAndFetchTasks() {
    taskStore.add(tasks);

    assertThat(Iterables.getOnlyElement(taskStore.fetch(Query.byId(1))), is(taskA));

    Predicate<TaskState> taskIdFilter = new Predicate<TaskState>() {
      @Override public boolean apply(TaskState state) {
        return state.task.getAssignedTask().getTaskId() == 1;
      }
    };

    assertThat(Iterables.getOnlyElement(taskStore.fetch(new Query(new TaskQuery(), taskIdFilter))),
        is(taskA));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsTaskIdCollision() {
    taskStore.add(tasks);
    taskStore.add(Arrays.asList(makeTask(TASK_A_ID).setStatus(FAILED)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsDuplicateTasks() {
    taskStore.add(Arrays.asList(makeTask(10), makeTask(10)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsDuplicateTaskIds() {
    taskStore.add(Arrays.asList(
        makeTask(10).setAssignedTask(new AssignedTask()
            .setTask(new TwitterTaskInfo().setOwner("A"))),
        makeTask(10).setAssignedTask(new AssignedTask()
            .setTask(new TwitterTaskInfo().setOwner("B")))));
  }

  @Test
  public void testImmutable() {
    taskStore.add(tasks);

    taskA.task.setStatus(RUNNING);

    assertThat(Iterables.getOnlyElement(taskStore.fetch(Query.byId(TASK_A_ID))).task.getStatus(),
        is(PENDING));
  }

  @Test
  public void testSortOrder() {
    int id = 1;
    int priority = 100;
    List<ScheduledTask> tasks = Arrays.asList(
        makeTask(id++, priority--).setStatus(PENDING),
        makeTask(id++, priority--).setStatus(PENDING),
        makeTask(id++, priority--).setStatus(KILLED),
        makeTask(id++, priority--).setStatus(RUNNING),
        makeTask(id++, priority--).setStatus(PENDING),
        makeTask(id++, priority--).setStatus(KILLED),
        makeTask(id++, priority--).setStatus(STARTING)
    );

    taskStore.add(tasks);

    assertThat(Lists.newArrayList(Iterables.transform(taskStore.fetch(Query.GET_ALL),
        Tasks.STATE_TO_SCHEDULED)), is(tasks));
    assertThat(Lists.newArrayList(Iterables.transform(
        taskStore.fetch(Query.GET_ALL, Query.SORT_BY_TASK_ID), Tasks.STATE_TO_SCHEDULED)),
        is(tasks));
    assertThat(Lists.newArrayList(Iterables.transform(
        taskStore.fetch(Query.GET_ALL, Query.SORT_BY_PRIORITY), Tasks.STATE_TO_SCHEDULED)),
        is(Lists.reverse(tasks)));
    assertThat(Lists.newArrayList(Iterables.transform(
        taskStore.fetch(Query.byStatus(PENDING)), Tasks.STATE_TO_SCHEDULED)),
        is(Arrays.asList(tasks.get(0), tasks.get(1), tasks.get(4))));
    assertThat(Lists.newArrayList(Iterables.transform(
        taskStore.fetch(Query.byStatus(Tasks.ACTIVE_STATES),
            Query.SORT_BY_PRIORITY, Tasks.ACTIVE_FILTER), Tasks.STATE_TO_SCHEDULED)),
        is(Arrays.asList(tasks.get(6), tasks.get(4), tasks.get(3), tasks.get(1), tasks.get(0))));
  }

  @Test
  public void testMutate() {
    taskStore.add(tasks);

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
    taskStore.add(tasks);
    taskStore.remove(Sets.newHashSet(taskA.task.getAssignedTask().getTaskId()));
    assertThat(Iterables.getOnlyElement(taskStore.fetch(Query.GET_ALL)), is(taskB));
  }

  private static ScheduledTask makeTask(int taskId) {
    return new ScheduledTask().setAssignedTask(
        new AssignedTask().setTaskId(taskId).setTask(new TwitterTaskInfo()));
  }

  private static ScheduledTask makeTask(int taskId, int priority) {
    ScheduledTask task = new ScheduledTask().setAssignedTask(
        new AssignedTask().setTaskId(taskId).setTask(new TwitterTaskInfo()));
    task.getAssignedTask().getTask().setPriority(priority);
    return task;
  }
}
