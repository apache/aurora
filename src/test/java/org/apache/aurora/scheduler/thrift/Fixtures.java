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
package org.apache.aurora.scheduler.thrift;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ResponseDetail;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResult;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.junit.Assert.assertEquals;

final class Fixtures {
  static final String ROLE = "bar_role";
  static final String USER = "foo_user";
  static final Identity IDENTITY = new Identity().setUser(USER);
  static final String JOB_NAME = "job_foo";
  static final IJobKey JOB_KEY = JobKeys.from(ROLE, "devel", JOB_NAME);
  static final ILockKey LOCK_KEY = ILockKey.build(LockKey.job(JOB_KEY.newBuilder()));
  static final JobConfiguration CRON_JOB = makeJob().setCronSchedule("* * * * *");
  static final String TASK_ID = "task_id";
  static final String UPDATE_ID = "82d6d790-3212-11e3-aa6e-0800200c9a74";
  static final IJobUpdateKey UPDATE_KEY =
      IJobUpdateKey.build(new JobUpdateKey(JOB_KEY.newBuilder(), UPDATE_ID));
  static final UUID UU_ID = UUID.fromString(UPDATE_ID);
  private static final Function<String, ResponseDetail> MESSAGE_TO_DETAIL =
      message -> new ResponseDetail().setMessage(message);
  static final String CRON_SCHEDULE = "0 * * * *";
  static final ResourceBag QUOTA = ResourceTestUtil.bag(10.0, 1024, 2048);
  static final QuotaCheckResult ENOUGH_QUOTA = new QuotaCheckResult(SUFFICIENT_QUOTA);
  static final QuotaCheckResult NOT_ENOUGH_QUOTA = new QuotaCheckResult(INSUFFICIENT_QUOTA);
  static final InstanceKey INSTANCE_KEY = new InstanceKey(JOB_KEY.newBuilder(), 0);
  static final TaskConfig INVALID_TASK_CONFIG = defaultTask(true).setTier(",");

  private Fixtures() {
    // Utility class.
  }

  static JobConfiguration makeJob() {
    return makeJob(nonProductionTask(), 1);
  }

  static JobConfiguration makeJob(TaskConfig task, int shardCount) {
    return new JobConfiguration()
        .setOwner(IDENTITY)
        .setInstanceCount(shardCount)
        .setTaskConfig(task)
        .setKey(JOB_KEY.newBuilder());
  }

  static TaskConfig defaultTask(boolean production) {
    return new TaskConfig()
        .setJob(JOB_KEY.newBuilder())
        .setOwner(IDENTITY)
        .setContactEmail("testing@twitter.com")
        .setExecutorConfig(new ExecutorConfig("aurora", "data"))
        .setNumCpus(1)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setProduction(production)
        .setTier(production ? TaskTestUtil.PROD_TIER_NAME : TaskTestUtil.DEV_TIER_NAME)
        .setRequestedPorts(ImmutableSet.of())
        .setTaskLinks(ImmutableMap.of())
        .setMaxTaskFailures(1)
        .setConstraints(ImmutableSet.of())
        .setMetadata(ImmutableSet.of())
        .setMesosFetcherUris(ImmutableSet.of())
        .setContainer(Container.mesos(new MesosContainer()))
        .setResources(ImmutableSet.of(
            Resource.numCpus(1),
            Resource.ramMb(1024),
            Resource.diskMb(1024)));
  }

  static TaskConfig nonProductionTask() {
    return defaultTask(false);
  }

  static Response jobSummaryResponse(Set<JobSummary> jobSummaries) {
    return okResponse(Result.jobSummaryResult(new JobSummaryResult().setSummaries(jobSummaries)));
  }

  static Response response(ResponseCode code, Optional<Result> result, String... messages) {
    Response response = Responses.empty()
        .setResponseCode(code)
        .setResult(result.orNull());
    if (messages.length > 0) {
      response.setDetails(FluentIterable.from(Arrays.asList(messages)).transform(MESSAGE_TO_DETAIL)
          .toList());
    }

    return response;
  }

  static Response okResponse(Result result) {
    return response(OK, Optional.of(IResult.build(result).newBuilder()));
  }

  static JobConfiguration makeProdJob() {
    return makeJob(productionTask(), 1);
  }

  static TaskConfig productionTask() {
    return defaultTask(true);
  }

  static JobConfiguration makeJob(TaskConfig task) {
    return makeJob(task, 1);
  }

  static Iterable<IScheduledTask> makeDefaultScheduledTasks(int n) {
    return makeDefaultScheduledTasks(n, defaultTask(true));
  }

  static Iterable<IScheduledTask> makeDefaultScheduledTasks(int n, TaskConfig config) {
    List<IScheduledTask> tasks = Lists.newArrayList();
    for (int i = 0; i < n; i++) {
      tasks.add(IScheduledTask.build(new ScheduledTask()
          .setAssignedTask(new AssignedTask().setTask(config).setInstanceId(i))));
    }

    return tasks;
  }

  static Response assertOkResponse(Response response) {
    return assertResponse(OK, response);
  }

  static Response assertResponse(ResponseCode expected, Response response) {
    assertEquals(expected, response.getResponseCode());
    return response;
  }
}
