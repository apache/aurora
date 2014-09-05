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

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Positive;

import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;

/**
 * Validates task-specific requirements including name, count and quota checks.
 */
public interface TaskLimitValidator {

  /**
   * Validates adding {@code instances} of {@code task} does not violate certain task/job limits.
   * <p>
   * Validated rules:
   * <ul>
   *   <li>Max task ID length</li>
   *   <li>Max number of tasks per job</li>
   *   <li>Role resource quota</li>
   * </ul>
   *
   * @param task Task configuration.
   * @param newInstances Number of new task instances.
   * @throws {@link TaskValidationException} If validation fails.
   */
  void validateTaskLimits(ITaskConfig task, int newInstances) throws TaskValidationException;

  /**
   * Thrown when task fails validation.
   */
  class TaskValidationException extends Exception {
    public TaskValidationException(String msg) {
      super(msg);
    }
  }

  class TaskLimitValidatorImpl implements TaskLimitValidator {

    @Positive
    @CmdLine(name = "max_tasks_per_job", help = "Maximum number of allowed tasks in a single job.")
    public static final Arg<Integer> MAX_TASKS_PER_JOB = Arg.create(4000);

    // This number is derived from the maximum file name length limit on most UNIX systems, less
    // the number of characters we've observed being added by mesos for the executor ID, prefix, and
    // delimiters.
    @VisibleForTesting
    static final int MAX_TASK_ID_LENGTH = 255 - 90;

    private final TaskIdGenerator taskIdGenerator;
    private final QuotaManager quotaManager;

    @Inject
    TaskLimitValidatorImpl(TaskIdGenerator taskIdGenerator, QuotaManager quotaManager) {
      this.taskIdGenerator = requireNonNull(taskIdGenerator);
      this.quotaManager = requireNonNull(quotaManager);
    }

    @Override
    public void validateTaskLimits(ITaskConfig task, int newInstances)
        throws TaskValidationException {

      // TODO(maximk): This is a short-term hack to stop the bleeding from
      //               https://issues.apache.org/jira/browse/MESOS-691
      if (taskIdGenerator.generate(task, newInstances).length() > MAX_TASK_ID_LENGTH) {
        throw new TaskValidationException(
            "Task ID is too long, please shorten your role or job name.");
      }

      // TODO(maximk): This check must consider ALL existing tasks not just the new instances.
      if (newInstances > MAX_TASKS_PER_JOB.get()) {
        throw new TaskValidationException("Job exceeds task limit of " + MAX_TASKS_PER_JOB.get());
      }

      QuotaCheckResult quotaCheck = quotaManager.checkQuota(
          ImmutableMap.<ITaskConfig, Integer>of(),
          task,
          newInstances);

      if (quotaCheck.getResult() == INSUFFICIENT_QUOTA) {
        throw new TaskValidationException("Insufficient resource quota: "
            + quotaCheck.getDetails().or(""));
      }

    }
  }
}
