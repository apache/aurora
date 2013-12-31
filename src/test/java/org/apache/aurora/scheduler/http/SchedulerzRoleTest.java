package org.apache.aurora.scheduler.http;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.http.SchedulerzRole.getFreshestTask;

import static org.junit.Assert.assertEquals;

public class SchedulerzRoleTest {

  @Test
  public void testFreshestTasks() {
    final IScheduledTask F1 = makeTask(FINISHED, 100);
    final IScheduledTask F2 = makeTask(FINISHED, 200);
    final IScheduledTask F3 = makeTask(FINISHED, 300);
    final IScheduledTask R1 = makeTask(RUNNING, 400);
    final IScheduledTask R2 = makeTask(RUNNING, 500);
    final IScheduledTask R3 = makeTask(RUNNING, 600);

    assertEquals(R1, getFreshestTask(ImmutableList.of(R1)));
    assertEquals(R2, getFreshestTask(ImmutableList.of(R1, R2)));
    assertEquals(R2, getFreshestTask(ImmutableList.of(R2, R1)));
    assertEquals(R3, getFreshestTask(ImmutableList.of(R2, R1, R3)));
    assertEquals(R3, getFreshestTask(ImmutableList.of(R3, R2, R1)));

    assertEquals(F1, getFreshestTask(ImmutableList.of(F1)));
    assertEquals(F2, getFreshestTask(ImmutableList.of(F1, F2)));
    assertEquals(F2, getFreshestTask(ImmutableList.of(F2, F1)));
    assertEquals(F3, getFreshestTask(ImmutableList.of(F2, F1, F3)));
    assertEquals(F3, getFreshestTask(ImmutableList.of(F3, F2, F1)));

    assertEquals(R1, getFreshestTask(ImmutableList.of(F2, F1, R1)));
    assertEquals(R2, getFreshestTask(ImmutableList.of(F2, F1, R1, R2)));
    assertEquals(R3, getFreshestTask(ImmutableList.of(F2, F1, F3, R1, R2, R3)));
    assertEquals(R3, getFreshestTask(ImmutableList.of(R1, R3, R2, F3, F1, F2)));
  }

  private IScheduledTask makeTask(ScheduleStatus status, long startTime) {
    return makeTask(status, makeTaskEvents(startTime, 3));
  }

  private IScheduledTask makeTask(ScheduleStatus status, List<TaskEvent> taskEvents) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId("task_id")
            .setSlaveHost("host1")
            .setTask(new TaskConfig()
                .setJobName("job_a")
                .setOwner(new Identity("role_a", "role_a" + "-user"))))
        .setTaskEvents(taskEvents));
  }

  private List<TaskEvent> makeTaskEvents(long startTs, int count) {
    List<TaskEvent> taskEvents = Lists.newArrayListWithCapacity(3);
    for (int i = 0; i < count; i++) {
      taskEvents.add(makeTaskEvent(startTs - (i * 10)));
    }
    Collections.reverse(taskEvents);
    return taskEvents;
  }

  private TaskEvent makeTaskEvent(long ts) {
    return new TaskEvent().setTimestamp(ts);
  }
}
