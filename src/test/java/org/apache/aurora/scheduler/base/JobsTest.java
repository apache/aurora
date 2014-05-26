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

import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.apache.aurora.gen.JobStats;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.storage.entities.IJobStats;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Test;

import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTaskEvents;
import static org.junit.Assert.assertEquals;

public class JobsTest {

  @Test
  public void testGetJobStats() {
    ImmutableList<IScheduledTask> tasks =
        FluentIterable
            .from(Sets.immutableEnumSet(Arrays.asList(ScheduleStatus.values())))
            .transform(new Function<ScheduleStatus, IScheduledTask>() {
              @Override
              public IScheduledTask apply(ScheduleStatus status) {
                int startTime = 100;
                if (status == ScheduleStatus.SANDBOX_DELETED) {
                  return makeTask(
                      status,
                      makeTaskEvents(
                          startTime,
                          ScheduleStatus.FAILED,
                          ScheduleStatus.SANDBOX_DELETED));
                }
                return makeTask(status, makeTaskEvents(startTime, status));
              }
            }).toList();

    IJobStats expectedStats = IJobStats.build(new JobStats()
        .setActiveTaskCount(7)
        .setFailedTaskCount(3)
        .setFinishedTaskCount(2)
        .setPendingTaskCount(3));

    assertEquals(expectedStats, Jobs.getJobStats(tasks));
  }
}
