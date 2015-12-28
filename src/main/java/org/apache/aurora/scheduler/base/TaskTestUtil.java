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
package org.apache.aurora.scheduler.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.Container._Fields;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Convenience methods for working with tasks.
 * <p>
 * TODO(wfarner): This lives in under src/main only so benchmarks can access it.  Reconsider the
 * project layout so this is not necessary.
 */
public final class TaskTestUtil {

  public static final IJobKey JOB = JobKeys.from("role", "env", "job");
  public static final TierInfo REVOCABLE_TIER = new TierInfo(true);
  public static final ConfigurationManager CONFIGURATION_MANAGER =
      new ConfigurationManager(ImmutableSet.of(_Fields.MESOS), false);

  private TaskTestUtil() {
    // Utility class.
  }

  public static ITaskConfig makeConfig(IJobKey job) {
    return ITaskConfig.build(new TaskConfig()
        .setJob(job.newBuilder())
        .setJobName(job.getName())
        .setEnvironment(job.getEnvironment())
        .setOwner(new Identity(job.getRole(), job.getRole() + "-user"))
        .setIsService(true)
        .setNumCpus(1.0)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setPriority(1)
        .setMaxTaskFailures(-1)
        .setProduction(true)
        .setTier("tier-" + job.getEnvironment())
        .setConstraints(ImmutableSet.of(
            new Constraint(
                "valueConstraint",
                TaskConstraint.value(
                    new ValueConstraint(true, ImmutableSet.of("value1", "value2")))),
            new Constraint(
                "limitConstraint",
                TaskConstraint.limit(new LimitConstraint(5)))))
        .setRequestedPorts(ImmutableSet.of("http"))
        .setTaskLinks(ImmutableMap.of("http", "link", "admin", "otherLink"))
        .setContactEmail("foo@bar.com")
        .setMetadata(ImmutableSet.of(new Metadata("key", "value")))
        .setExecutorConfig(new ExecutorConfig("name", "config"))
        .setContainer(Container.docker(
            new DockerContainer("imagename")
                .setParameters(ImmutableList.of(
                    new DockerParameter("a", "b"),
                    new DockerParameter("c", "d"))))));
  }

  public static IScheduledTask makeTask(String id, IJobKey job) {
    return makeTask(id, makeConfig(job));
  }

  public static IScheduledTask makeTask(String id, ITaskConfig config) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(ScheduleStatus.PENDING)
        .setTaskEvents(ImmutableList.of(
            new TaskEvent(100L, ScheduleStatus.PENDING)
                .setMessage("message")
                .setScheduler("scheduler"),
            new TaskEvent(101L, ScheduleStatus.ASSIGNED)
                .setMessage("message")
                .setScheduler("scheduler2")))
        .setAncestorId("ancestor")
        .setFailureCount(3)
        .setAssignedTask(new AssignedTask()
            .setInstanceId(2)
            .setTaskId(id)
            .setAssignedPorts(ImmutableMap.of("http", 1000))
            .setTask(config.newBuilder())));
  }

  public static IScheduledTask addStateTransition(
      IScheduledTask task,
      ScheduleStatus status,
      long timestamp) {

    ScheduledTask builder = task.newBuilder();
    builder.setStatus(status);
    builder.addToTaskEvents(new TaskEvent()
        .setTimestamp(timestamp)
        .setStatus(status)
        .setScheduler("scheduler"));
    return IScheduledTask.build(builder);
  }
}
