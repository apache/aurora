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
package org.apache.aurora.scheduler.configuration;

import java.util.Objects;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.common.base.Closure;
import com.twitter.common.base.MorePreconditions;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConfig._Fields;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IIdentity;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskConstraint;
import org.apache.aurora.scheduler.storage.entities.IValueConstraint;
import org.apache.commons.lang.StringUtils;

import static org.apache.aurora.gen.apiConstants.GOOD_IDENTIFIER_PATTERN_JVM;

/**
 * Manages translation from a string-mapped configuration to a concrete configuration type, and
 * defaults for optional values.
 *
 * TODO(William Farner): Add input validation to all fields (strings not empty, positive ints, etc).
 */
public final class ConfigurationManager {

  public static final String DEDICATED_ATTRIBUTE = "dedicated";

  @VisibleForTesting public static final String HOST_CONSTRAINT = "host";
  @VisibleForTesting public static final String RACK_CONSTRAINT = "rack";

  private static final Pattern GOOD_IDENTIFIER = Pattern.compile(GOOD_IDENTIFIER_PATTERN_JVM);

  private static final int MAX_IDENTIFIER_LENGTH = 255;

  private static class DefaultField implements Closure<TaskConfig> {
    private final _Fields field;
    private final Object defaultValue;

    DefaultField(_Fields field, Object defaultValue) {
      this.field = field;
      this.defaultValue = defaultValue;
    }

    @Override
    public void execute(TaskConfig task) {
      if (!task.isSet(field)) {
        task.setFieldValue(field, defaultValue);
      }
    }
  }

  private interface Validator<T> {
    void validate(T value) throws TaskDescriptionException;
  }

  private static class GreaterThan implements Validator<Number> {
    private final double min;
    private final String label;

    GreaterThan(double min, String label) {
      this.min = min;
      this.label = label;
    }

    @Override
    public void validate(Number value) throws TaskDescriptionException {
      if (this.min >= value.doubleValue()) {
        throw new TaskDescriptionException(label + " must be greater than " + this.min);
      }
    }
  }

  private static class RequiredFieldValidator<T> implements Validator<TaskConfig> {
    private final _Fields field;
    private final Validator<T> validator;

    RequiredFieldValidator(_Fields field, Validator<T> validator) {
      this.field = field;
      this.validator = validator;
    }

    public void validate(TaskConfig task) throws TaskDescriptionException {
      if (!task.isSet(field)) {
        throw new TaskDescriptionException("Field " + field.getFieldName() + " is required.");
      }
      @SuppressWarnings("unchecked")
      T value = (T) task.getFieldValue(field);
      validator.validate(value);
    }
  }

  private static final Iterable<Closure<TaskConfig>> DEFAULT_FIELD_POPULATORS =
      ImmutableList.of(
          new DefaultField(_Fields.IS_SERVICE, false),
          new DefaultField(_Fields.PRIORITY, 0),
          new DefaultField(_Fields.PRODUCTION, false),
          new DefaultField(_Fields.MAX_TASK_FAILURES, 1),
          new DefaultField(_Fields.TASK_LINKS, Maps.<String, String>newHashMap()),
          new DefaultField(_Fields.REQUESTED_PORTS, Sets.<String>newHashSet()),
          new DefaultField(_Fields.CONSTRAINTS, Sets.<Constraint>newHashSet()),
          new Closure<TaskConfig>() {
            @Override
            public void execute(TaskConfig task) {
              if (!Iterables.any(task.getConstraints(), hasName(HOST_CONSTRAINT))) {
                task.addToConstraints(hostLimitConstraint(1));
              }
            }
          },
          new Closure<TaskConfig>() {
            @Override
            public void execute(TaskConfig task) {
              if (!isDedicated(ITaskConfig.build(task))
                  && task.isProduction()
                  && task.isIsService()
                  && !Iterables.any(task.getConstraints(), hasName(RACK_CONSTRAINT))) {

                task.addToConstraints(rackLimitConstraint(1));
              }
            }
          });

  private static final Iterable<RequiredFieldValidator<?>> REQUIRED_FIELDS_VALIDATORS =
      ImmutableList.<RequiredFieldValidator<?>>of(
          new RequiredFieldValidator<>(_Fields.NUM_CPUS, new GreaterThan(0.0, "num_cpus")),
          new RequiredFieldValidator<>(_Fields.RAM_MB, new GreaterThan(0.0, "ram_mb")),
          new RequiredFieldValidator<>(_Fields.DISK_MB, new GreaterThan(0.0, "disk_mb")));

  private ConfigurationManager() {
    // Utility class.
  }

  /**
   * Verifies that an identifier is an acceptable name component.
   *
   * @param identifier Identifier to check.
   * @return false if the identifier is null or invalid.
   */
  public static boolean isGoodIdentifier(@Nullable String identifier) {
    return identifier != null
        && GOOD_IDENTIFIER.matcher(identifier).matches()
        && identifier.length() <= MAX_IDENTIFIER_LENGTH;
  }

  private static void requireNonNull(Object value, String error) throws TaskDescriptionException {
    if (value == null) {
      throw new TaskDescriptionException(error);
    }
  }

  private static void assertOwnerValidity(IIdentity jobOwner) throws TaskDescriptionException {
    requireNonNull(jobOwner, "No job owner specified!");
    requireNonNull(jobOwner.getRole(), "No job role specified!");
    requireNonNull(jobOwner.getUser(), "No job user specified!");

    if (!isGoodIdentifier(jobOwner.getRole())) {
      throw new TaskDescriptionException(
          "Job role contains illegal characters: " + jobOwner.getRole());
    }

    if (!isGoodIdentifier(jobOwner.getUser())) {
      throw new TaskDescriptionException(
          "Job user contains illegal characters: " + jobOwner.getUser());
    }
  }

  private static String getRole(IValueConstraint constraint) {
    return Iterables.getOnlyElement(constraint.getValues()).split("/")[0];
  }

  private static boolean isValueConstraint(ITaskConstraint taskConstraint) {
    return taskConstraint.getSetField() == TaskConstraint._Fields.VALUE;
  }

  public static boolean isDedicated(ITaskConfig task) {
    return Iterables.any(task.getConstraints(), getConstraintByName(DEDICATED_ATTRIBUTE));
  }

  @Nullable
  private static IConstraint getDedicatedConstraint(ITaskConfig task) {
    return Iterables.find(task.getConstraints(), getConstraintByName(DEDICATED_ATTRIBUTE), null);
  }

  /**
   * Check validity of and populates defaults in a job configuration.  This will return a deep copy
   * of the provided job configuration with default configuration values applied, and configuration
   * map values sanitized and applied to their respective struct fields.
   *
   * @param job Job to validate and populate.
   * @return A deep copy of {@code job} that has been populated.
   * @throws TaskDescriptionException If the job configuration is invalid.
   */
  public static IJobConfiguration validateAndPopulate(IJobConfiguration job)
      throws TaskDescriptionException {

    Objects.requireNonNull(job);

    if (!job.isSetTaskConfig()) {
      throw new TaskDescriptionException("Job configuration must have taskConfig set.");
    }

    if (!job.isSetInstanceCount()) {
      throw new TaskDescriptionException("Job configuration does not have instanceCount set.");
    }

    if (job.getInstanceCount() <= 0) {
      throw new TaskDescriptionException("Instance count must be positive.");
    }

    JobConfiguration builder = job.newBuilder();

    if (!JobKeys.isValid(job.getKey())) {
      throw new TaskDescriptionException("Job key " + job.getKey() + " is invalid.");
    }

    if (job.isSetOwner()) {
      assertOwnerValidity(job.getOwner());

      if (!job.getKey().getRole().equals(job.getOwner().getRole())) {
        throw new TaskDescriptionException("Role in job key must match job owner.");
      }
    }

    builder.setTaskConfig(
        validateAndPopulate(ITaskConfig.build(builder.getTaskConfig())).newBuilder());

    // Only one of [service=true, cron_schedule] may be set.
    if (!StringUtils.isEmpty(job.getCronSchedule()) && builder.getTaskConfig().isIsService()) {
      throw new TaskDescriptionException(
          "A service task may not be run on a cron schedule: " + builder);
    }

    return IJobConfiguration.build(builder);
  }

  /**
   * Check validity of and populates defaults in a task configuration.  This will return a deep copy
   * of the provided task configuration with default configuration values applied, and configuration
   * map values sanitized and applied to their respective struct fields.
   *
   *
   * @param config Task config to validate and populate.
   * @return A reference to the modified {@code config} (for chaining).
   * @throws TaskDescriptionException If the task is invalid.
   */
  public static ITaskConfig validateAndPopulate(ITaskConfig config)
      throws TaskDescriptionException {

    TaskConfig builder = config.newBuilder();

    if (!builder.isSetRequestedPorts()) {
      builder.setRequestedPorts(ImmutableSet.<String>of());
    }

    maybeFillLinks(builder);

    if (!isGoodIdentifier(config.getJobName())) {
      throw new TaskDescriptionException(
          "Job name contains illegal characters: " + config.getJobName());
    }

    if (!isGoodIdentifier(config.getEnvironment())) {
      throw new TaskDescriptionException(
          "Environment contains illegal characters: " + config.getEnvironment());
    }

    if (config.isSetJob()) {
      if (!JobKeys.isValid(config.getJob())) {
        // Job key is set but invalid
        throw new TaskDescriptionException("Job key " + config.getJob() + " is invalid.");
      }

      if (!config.getJob().getRole().equals(config.getOwner().getRole())) {
        // Both owner and job key are set but don't match
        throw new TaskDescriptionException("Role must match job owner.");
      }
    } else {
      // TODO(maxim): Make sure both key and owner are populated to support older clients.
      // Remove in 0.7.0. (AURORA-749).
      // Job key is not set -> populate from owner, environment and name
      assertOwnerValidity(config.getOwner());
      builder.setJob(JobKeys.from(
          config.getOwner().getRole(),
          config.getEnvironment(),
          config.getJobName()).newBuilder());
    }

    if (!builder.isSetExecutorConfig()) {
      throw new TaskDescriptionException("Configuration may not be null");
    }

    // Maximize the usefulness of any thrown error message by checking required fields first.
    for (RequiredFieldValidator<?> validator : REQUIRED_FIELDS_VALIDATORS) {
      validator.validate(builder);
    }

    IConstraint constraint = getDedicatedConstraint(config);
    if (constraint != null) {
      if (!isValueConstraint(constraint.getConstraint())) {
        throw new TaskDescriptionException("A dedicated constraint must be of value type.");
      }

      IValueConstraint valueConstraint = constraint.getConstraint().getValue();

      if (valueConstraint.getValues().size() != 1) {
        throw new TaskDescriptionException("A dedicated constraint must have exactly one value");
      }

      String dedicatedRole = getRole(valueConstraint);
      if (!config.getOwner().getRole().equals(dedicatedRole)) {
        throw new TaskDescriptionException(
            "Only " + dedicatedRole + " may use hosts dedicated for that role.");
      }
    }

    return ITaskConfig.build(applyDefaultsIfUnset(builder));
  }

  /**
   * Provides a filter for the given constraint name.
   *
   * @param name The name of the constraint.
   * @return A filter that matches the constraint.
   */
  public static Predicate<IConstraint> getConstraintByName(final String name) {
    return new Predicate<IConstraint>() {
      @Override
      public boolean apply(IConstraint constraint) {
        return constraint.getName().equals(name);
      }
    };
  }

  @VisibleForTesting
  public static Constraint hostLimitConstraint(int limit) {
    return new Constraint(HOST_CONSTRAINT, TaskConstraint.limit(new LimitConstraint(limit)));
  }

  @VisibleForTesting
  public static Constraint rackLimitConstraint(int limit) {
    return new Constraint(RACK_CONSTRAINT, TaskConstraint.limit(new LimitConstraint(limit)));
  }

  private static Predicate<Constraint> hasName(final String name) {
    MorePreconditions.checkNotBlank(name);
    return new Predicate<Constraint>() {
      @Override
      public boolean apply(Constraint constraint) {
        return name.equals(constraint.getName());
      }
    };
  }

  /**
   * Applies defaults to unset values in a task.
   *
   * @param task Task to apply defaults to.
   * @return A reference to the (modified) {@code task}.
   */
  @VisibleForTesting
  public static TaskConfig applyDefaultsIfUnset(TaskConfig task) {
    for (Closure<TaskConfig> populator : DEFAULT_FIELD_POPULATORS) {
      populator.execute(task);
    }

    return task;
  }

  /**
   * Applies defaults to unset values in a job and its tasks.
   *
   * @param job Job to apply defaults to.
   */
  @VisibleForTesting
  public static void applyDefaultsIfUnset(JobConfiguration job) {
    ConfigurationManager.applyDefaultsIfUnset(job.getTaskConfig());
  }

  private static void maybeFillLinks(TaskConfig task) {
    if (task.getTaskLinksSize() == 0) {
      ImmutableMap.Builder<String, String> links = ImmutableMap.builder();
      if (task.getRequestedPorts().contains("health")) {
        links.put("health", "http://%host%:%port:health%");
      }
      if (task.getRequestedPorts().contains("http")) {
        links.put("http", "http://%host%:%port:http%");
      }
      task.setTaskLinks(links.build());
    }
  }

  /**
   * Thrown when an invalid task or job configuration is encountered.
   */
  public static class TaskDescriptionException extends Exception {
    public TaskDescriptionException(String msg) {
      super(msg);
    }
  }
}
