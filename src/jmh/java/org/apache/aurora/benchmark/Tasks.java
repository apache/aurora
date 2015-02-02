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
package org.apache.aurora.benchmark;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

/**
 * Task factory.
 */
final class Tasks {

  private Tasks() {
    // Utility class.
  }

  /**
   * Builds tasks for the specified configuration.
   */
  static final class Builder {
    private static final JobKey JOB_KEY = new JobKey("jmh", "dev", "benchmark");
    private static final String USER_FORMAT = "user-%s";

    private String taskIdFormat = "default_task-%s";
    private boolean isProduction = false;
    private double cpu = 6.0;
    private Amount<Long, Data> ram = Amount.of(8L, Data.GB);
    private Amount<Long, Data> disk = Amount.of(128L, Data.GB);
    private ScheduleStatus scheduleStatus = ScheduleStatus.PENDING;
    private ImmutableSet.Builder<Constraint> constraints = ImmutableSet.builder();

    Builder setTaskIdFormat(String newTaskIdFormat) {
      taskIdFormat = newTaskIdFormat;
      return this;
    }

    Builder setCpu(double newCpu) {
      cpu = newCpu;
      return this;
    }

    Builder setRam(Amount<Long, Data> newRam) {
      ram = newRam;
      return this;
    }

    Builder setDisk(Amount<Long, Data> newDisk) {
      disk = newDisk;
      return this;
    }

    Builder setScheduleStatus(ScheduleStatus newScheduleStatus) {
      scheduleStatus = newScheduleStatus;
      return this;
    }

    Builder setProduction(boolean newProduction) {
      isProduction = newProduction;
      return this;
    }

    Builder addValueConstraint(String name, String value) {
      constraints.add(new Constraint()
          .setName(name)
          .setConstraint(TaskConstraint.value(new ValueConstraint()
              .setNegated(false)
              .setValues(ImmutableSet.of(value)))));

      return this;
    }

    Builder addLimitConstraint(String name, int limit) {
      constraints.add(new Constraint()
          .setName(name)
          .setConstraint(TaskConstraint.limit(new LimitConstraint()
              .setLimit(limit))));

      return this;
    }

    /**
     * Builds a set of {@link IScheduledTask} for the current configuration.
     *
     * @param count Number of tasks to build.
     * @return Set of tasks.
     */
    Set<IScheduledTask> build(int count) {
      ImmutableSet.Builder<IScheduledTask> tasks = ImmutableSet.builder();

      for (int i = 0; i < count; i++) {
        String taskId = String.format(taskIdFormat, i);

        tasks.add(IScheduledTask.build(new ScheduledTask()
            .setTaskEvents(Lists.newArrayList(new TaskEvent(0, ScheduleStatus.PENDING)))
            .setStatus(scheduleStatus)
            .setAssignedTask(new AssignedTask()
                .setInstanceId(i)
                .setTaskId(taskId)
                .setTask(new TaskConfig()
                    .setConstraints(constraints.build())
                    .setNumCpus(cpu)
                    .setRamMb(ram.as(Data.MB))
                    .setDiskMb(disk.as(Data.MB))
                    .setProduction(isProduction)
                    .setRequestedPorts(ImmutableSet.<String>of())
                    .setJob(JOB_KEY)
                    .setJobName(JOB_KEY.getName())
                    .setEnvironment(JOB_KEY.getEnvironment())
                    .setOwner(new Identity()
                        .setRole(JOB_KEY.getRole())
                        .setUser(String.format(USER_FORMAT, taskId)))))));
      }

      return tasks.build();
    }
  }
}
