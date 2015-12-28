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
package org.apache.aurora.scheduler.cron.quartz;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.quartz.JobKey;

/**
 * Fixtures used across quartz tests.
 */
final class QuartzTestUtil {
  static final IJobKey AURORA_JOB_KEY = JobKeys.from("role", "env", "job");
  static final IJobConfiguration JOB = IJobConfiguration.build(
      new JobConfiguration()
          .setCronSchedule("* * * * SUN")
          .setInstanceCount(10)
          .setOwner(new Identity("role", "user"))
          .setKey(AURORA_JOB_KEY.newBuilder())
          .setTaskConfig(new TaskConfig()
              .setJob(AURORA_JOB_KEY.newBuilder())
              .setOwner(new Identity("role", "user"))
              .setJobName(AURORA_JOB_KEY.getName())
              .setEnvironment(AURORA_JOB_KEY.getEnvironment())
              .setDiskMb(3)
              .setRamMb(4)
              .setNumCpus(5)
              .setMetadata(ImmutableSet.<Metadata>of())
              .setExecutorConfig(new ExecutorConfig()
                  .setName("cmd.exe")
                  .setData("echo hello world")))
  );
  static final JobKey QUARTZ_JOB_KEY = Quartz.jobKey(AURORA_JOB_KEY);

  private QuartzTestUtil() {
    // Utility class.
  }

  static SanitizedCronJob makeSanitizedCronJob(CronCollisionPolicy collisionPolicy) {
    try {
      return SanitizedCronJob.fromUnsanitized(
          TaskTestUtil.CONFIGURATION_MANAGER,
          IJobConfiguration.build(JOB.newBuilder().setCronCollisionPolicy(collisionPolicy)));
    } catch (CronException | ConfigurationManager.TaskDescriptionException e) {
      throw Throwables.propagate(e);
    }
  }

  static SanitizedCronJob makeSanitizedCronJob() {
    return makeSanitizedCronJob(CronCollisionPolicy.KILL_EXISTING);
  }

  static SanitizedCronJob makeUpdatedJob() throws Exception {
    return SanitizedCronJob.fromUnsanitized(
        TaskTestUtil.CONFIGURATION_MANAGER,
        IJobConfiguration.build(JOB.newBuilder().setCronSchedule("* * 1 * *")));
  }
}
