/**
 * Copyright 2013 Apache Software Foundation
 *
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

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility class providing convenience functions relating to JobKeys.
 */
public final class JobKeys {
  private JobKeys() {
    // Utility class.
  }

  public static final Function<IJobConfiguration, IJobKey> FROM_CONFIG =
      new Function<IJobConfiguration, IJobKey>() {
        @Override
        public IJobKey apply(IJobConfiguration job) {
          return job.getKey();
        }
      };

  public static final Function<IJobKey, String> TO_ROLE =
      new Function<IJobKey, String>() {
        @Override
        public String apply(IJobKey jobKey) {
          return jobKey.getRole();
        }
      };

  public static final Function<IJobKey, String> TO_ENVIRONMENT =
      new Function<IJobKey, String>() {
        @Override
        public String apply(IJobKey jobKey) {
          return jobKey.getEnvironment();
        }
      };

  public static final Function<IJobKey, String> TO_JOB_NAME =
      new Function<IJobKey, String>() {
        @Override
        public String apply(IJobKey jobKey) {
          return jobKey.getName();
        }
      };

  public static final Function<IJobConfiguration, String> CONFIG_TO_ROLE =
      Functions.compose(TO_ROLE, FROM_CONFIG);

  /**
   * Check that a jobKey struct is valid.
   *
   * @param jobKey The jobKey to validate.
   * @return {@code true} if the jobKey validates.
   */
  public static boolean isValid(@Nullable IJobKey jobKey) {
    return jobKey != null
        && !Strings.isNullOrEmpty(jobKey.getRole())
        && !Strings.isNullOrEmpty(jobKey.getEnvironment())
        && !Strings.isNullOrEmpty(jobKey.getName());
  }

  /**
   * Assert that a jobKey struct is valid.
   *
   * @param jobKey The key struct to validate.
   * @return The validated jobKey argument.
   * @throws IllegalArgumentException if the key struct fails to validate.
   */
  public static IJobKey assertValid(IJobKey jobKey) throws IllegalArgumentException {
    checkArgument(isValid(jobKey));

    return jobKey;
  }

  /**
   * Attempt to create a valid JobKey from the given (role, environment, name) triple.
   *
   * @param role The job's role.
   * @param environment The job's environment.
   * @param name The job's name.
   * @return A valid JobKey if it can be created.
   * @throws IllegalArgumentException if the key fails to validate.
   */
  public static IJobKey from(String role, String environment, String name)
      throws IllegalArgumentException {

    IJobKey job = IJobKey.build(new JobKey()
        .setRole(role)
        .setEnvironment(environment)
        .setName(name));
    return assertValid(job);
  }

  /**
   * Attempts to create a valid JobKey from the given task.
   *
   * @param task The task to create job key from.
   * @return A valid JobKey if it can be created.
   * @throws IllegalArgumentException if the key fails to validate.
   */
  public static IJobKey from(ITaskConfig task) throws IllegalArgumentException {
    return from(task.getOwner().getRole(), task.getEnvironment(), task.getJobName());
  }

  /**
   * Create a "/"-delimited String representation of a job key, suitable for logging but not
   * necessarily suitable for use as a unique identifier.
   *
   * @param jobKey Key to represent.
   * @return "/"-delimited representation of the key.
   */
  public static String toPath(IJobKey jobKey) {
    return jobKey.getRole() + "/" + jobKey.getEnvironment() + "/" + jobKey.getName();
  }

  /**
   * Create a "/"-delimited String representation of job key, suitable for logging but not
   * necessarily suitable for use as a unique identifier.
   *
   * @param job Job to represent.
   * @return "/"-delimited representation of the job's key.
   */
  public static String toPath(IJobConfiguration job) {
    return toPath(job.getKey());
  }

  /**
   * Attempt to extract a job key from the given query if it is scoped to a single job.
   *
   * @param query Query to extract the key from.
   * @return A present if one can be extracted, absent otherwise.
   */
  public static Optional<IJobKey> from(Query.Builder query) {
    if (Query.isJobScoped(query)) {
      TaskQuery taskQuery = query.get();
      return Optional.of(
          from(taskQuery.getOwner().getRole(), taskQuery.getEnvironment(), taskQuery.getJobName()));

    } else {
      return Optional.absent();
    }
  }
}
