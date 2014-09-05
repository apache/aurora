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
package org.apache.aurora.scheduler.state;

import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.TaskLimitValidator.TaskLimitValidatorImpl;
import org.apache.aurora.scheduler.state.TaskLimitValidator.TaskValidationException;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.apiConstants.DEFAULT_ENVIRONMENT;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;

public class TaskLimitValidatorTest extends EasyMockTest {
  private static final Identity JIM = new Identity("jim", "jim-user");
  private static final String MY_JOB = "myJob";
  private static final String TASK_ID = "a";

  private static final QuotaCheckResult ENOUGH_QUOTA = new QuotaCheckResult(SUFFICIENT_QUOTA);
  private static final QuotaCheckResult NOT_ENOUGH_QUOTA = new QuotaCheckResult(INSUFFICIENT_QUOTA);

  private TaskIdGenerator taskIdGenerator;
  private QuotaManager quotaManager;
  private TaskLimitValidatorImpl taskLimitValidator;

  @Before
  public void setUp() {
    taskIdGenerator = createMock(TaskIdGenerator.class);
    quotaManager = createMock(QuotaManager.class);
    taskLimitValidator = new TaskLimitValidatorImpl(taskIdGenerator, quotaManager);
  }

  @Test
  public void testValidateTask() throws Exception {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    expect(taskIdGenerator.generate(task, 1)).andReturn(TASK_ID);
    expect(quotaManager.checkQuota(
        EasyMock.<Map<ITaskConfig, Integer>>anyObject(),
        anyObject(ITaskConfig.class),
        anyInt())).andStubReturn(ENOUGH_QUOTA);

    control.replay();

    taskLimitValidator.validateTaskLimits(task, 1);
  }

  @Test(expected = TaskValidationException.class)
  public void testValidatesFailsTaskIdTooLong() throws Exception {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    expect(taskIdGenerator.generate(task, 1))
        .andReturn(Strings.repeat(TASK_ID, TaskLimitValidatorImpl.MAX_TASK_ID_LENGTH + 1));

    control.replay();

    taskLimitValidator.validateTaskLimits(task, 1);
  }

  @Test(expected = TaskValidationException.class)
  public void testValidatesFailsTooManyInstances() throws Exception {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    expect(taskIdGenerator.generate(task, TaskLimitValidatorImpl.MAX_TASKS_PER_JOB.get() + 1))
        .andReturn(TASK_ID);

    control.replay();

    taskLimitValidator.validateTaskLimits(task, TaskLimitValidatorImpl.MAX_TASKS_PER_JOB.get() + 1);
  }

  @Test(expected = TaskValidationException.class)
  public void testValidatesFailsQuotaCheck() throws Exception {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    expect(taskIdGenerator.generate(task, 1)).andReturn(TASK_ID);
    expect(quotaManager.checkQuota(
        EasyMock.<Map<ITaskConfig, Integer>>anyObject(),
        anyObject(ITaskConfig.class),
        anyInt())).andStubReturn(NOT_ENOUGH_QUOTA);

    control.replay();

    taskLimitValidator.validateTaskLimits(task, 1);
  }

  private static ITaskConfig makeTask(Identity owner, String job) {
    return ITaskConfig.build(new TaskConfig()
        .setOwner(owner)
        .setEnvironment(DEFAULT_ENVIRONMENT)
        .setJobName(job)
        .setRequestedPorts(ImmutableSet.<String>of()));
  }
}
