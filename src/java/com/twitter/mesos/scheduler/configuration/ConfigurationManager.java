package com.twitter.mesos.scheduler.configuration;

import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Positive;
import com.twitter.common.base.Closure;
import com.twitter.common.base.MorePreconditions;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.JobKey;
import com.twitter.mesos.gen.LimitConstraint;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.TwitterTaskInfo._Fields;
import com.twitter.mesos.gen.ValueConstraint;
import com.twitter.mesos.scheduler.JobKeys;

import static com.twitter.mesos.gen.Constants.DEFAULT_ENVIRONMENT;
import static com.twitter.mesos.gen.Constants.GOOD_IDENTIFIER_PATTERN_JVM;

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

  @Positive
  @CmdLine(name = "max_tasks_per_job", help = "Maximum number of allowed tasks in a single job.")
  public static final Arg<Integer> MAX_TASKS_PER_JOB = Arg.create(1000);

  @CmdLine(name = "require_contact_email",
      help = "If true, reject jobs that do not specify a contact email address.")
  public static final Arg<Boolean> REQUIRE_CONTACT_EMAIL = Arg.create(true);

  private static final Logger LOG = Logger.getLogger(ConfigurationManager.class.getName());

  private static final Pattern GOOD_IDENTIFIER = Pattern.compile(GOOD_IDENTIFIER_PATTERN_JVM);

  private static final int MAX_IDENTIFIER_LENGTH = 255;

  private static class DefaultField implements Closure<TwitterTaskInfo> {
    private final _Fields field;
    private final Object defaultValue;

    DefaultField(_Fields field, Object defaultValue) {
      this.field = field;
      this.defaultValue = defaultValue;
    }

    @Override public void execute(TwitterTaskInfo task) {
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

    @Override public void validate(Number value) throws TaskDescriptionException {
      if (this.min >= value.doubleValue()) {
        throw new TaskDescriptionException(label + " must be greater than " + this.min);
      }
    }
  }

  private static class RequiredFieldValidator<T> implements Validator<TwitterTaskInfo> {
    final _Fields field;
    final Validator<T> validator;

    RequiredFieldValidator(_Fields field, Validator<T> validator) {
      this.field = field;
      this.validator = validator;
    }

    public void validate(TwitterTaskInfo task) throws TaskDescriptionException {
      if (!task.isSet(field)) {
        throw new TaskDescriptionException("Field " + field.getFieldName() + " is required.");
      }
      @SuppressWarnings("unchecked")
      T value = (T) task.getFieldValue(field);
      validator.validate(value);
    }
  }

  private static final Iterable<Closure<TwitterTaskInfo>> DEFAULT_FIELD_POPULATORS =
      ImmutableList.of(
          new DefaultField(_Fields.IS_SERVICE, false),
          new DefaultField(_Fields.PRIORITY, 0),
          new DefaultField(_Fields.PRODUCTION, false),
          new DefaultField(_Fields.HEALTH_CHECK_INTERVAL_SECS, 30),
          new DefaultField(_Fields.MAX_TASK_FAILURES, 1),
          new DefaultField(_Fields.TASK_LINKS, Maps.<String, String>newHashMap()),
          new DefaultField(_Fields.REQUESTED_PORTS, Sets.<String>newHashSet()),
          new DefaultField(_Fields.CONSTRAINTS, Sets.<Constraint>newHashSet()),
          new DefaultField(_Fields.ENVIRONMENT, DEFAULT_ENVIRONMENT),
          new Closure<TwitterTaskInfo>() {
            @Override public void execute(TwitterTaskInfo task) {
              if (!Iterables.any(task.getConstraints(), hasName(HOST_CONSTRAINT))) {
                task.addToConstraints(hostLimitConstraint(1));
              }
            }
          },
          new Closure<TwitterTaskInfo>() {
            @Override public void execute(TwitterTaskInfo task) {
              if (!isDedicated(task)
                  && task.isProduction()
                  && task.isIsService()
                  && !Iterables.any(task.getConstraints(), hasName(RACK_CONSTRAINT))) {

                task.addToConstraints(rackLimitConstraint(1));
              }
            }
          });

  private static final Iterable<RequiredFieldValidator<?>> REQUIRED_FIELDS_VALIDATORS =
      ImmutableList.<RequiredFieldValidator<?>>of(
          new RequiredFieldValidator<Number>(_Fields.NUM_CPUS, new GreaterThan(0.0, "num_cpus")),
          new RequiredFieldValidator<Number>(_Fields.RAM_MB, new GreaterThan(0.0, "ram_mb")),
          new RequiredFieldValidator<Number>(_Fields.DISK_MB, new GreaterThan(0.0, "disk_mb")));

  @VisibleForTesting
  public static enum ShardIdState { PRESENT, ABSENT };

  private ConfigurationManager() {
    // Utility class.
  }

  @VisibleForTesting
  static boolean isGoodIdentifier(String identifier) {
    return GOOD_IDENTIFIER.matcher(identifier).matches()
        && (identifier.length() <= MAX_IDENTIFIER_LENGTH);
  }

  private static void checkNotNull(Object value, String error) throws TaskDescriptionException {
    if (value == null) {
       throw new TaskDescriptionException(error);
     }
  }

  private static void assertOwnerValidity(Identity jobOwner) throws TaskDescriptionException {
    checkNotNull(jobOwner, "No job owner specified!");
    checkNotNull(jobOwner.getRole(), "No job role specified!");
    checkNotNull(jobOwner.getUser(), "No job user specified!");

    if (!isGoodIdentifier(jobOwner.getRole())) {
      throw new TaskDescriptionException(
          "Job role contains illegal characters: " + jobOwner.getRole());
    }

    if (!isGoodIdentifier(jobOwner.getUser())) {
      throw new TaskDescriptionException(
          "Job user contains illegal characters: " + jobOwner.getUser());
    }
  }

  private static String getRole(ValueConstraint constraint) {
    return Iterables.getOnlyElement(constraint.getValues()).split("/")[0];
  }

  private static boolean isValueConstraint(TaskConstraint taskConstraint) {
    return taskConstraint.getSetField() == TaskConstraint._Fields.VALUE;
  }

  public static boolean isDedicated(TwitterTaskInfo task) {
    return Iterables.any(task.getConstraints(), getConstraintByName(DEDICATED_ATTRIBUTE));
  }

  @Nullable
  private static Constraint getDedicatedConstraint(TwitterTaskInfo task) {
    return Iterables.find(task.getConstraints(), getConstraintByName(DEDICATED_ATTRIBUTE), null);
  }

  /**
   * Check validity of and populates defaults in a job configuration.  This will return a deep copy
   * of the provided job configuration with default configuration values applied, and configuration
   * map values parsed and applied to their respective struct fields.
   *
   * @param job Job to validate and populate.
   * @return A deep copy of {@code job} that has been populated.
   * @throws TaskDescriptionException If the job configuration is invalid.
   */
  public static JobConfiguration validateAndPopulate(JobConfiguration job)
      throws TaskDescriptionException {

    Preconditions.checkNotNull(job);

    if (job.getTaskConfigsSize() > MAX_TASKS_PER_JOB.get()) {
      throw new TaskDescriptionException("Job exceeds task limit of " + MAX_TASKS_PER_JOB.get());
    }

    JobConfiguration copy = job.deepCopy();

    assertOwnerValidity(job.getOwner());

    if (!isGoodIdentifier(copy.getName())) {
      throw new TaskDescriptionException("Job name contains illegal characters: " + copy.getName());
    }

    // Temporarily support JobConfiguration objects that are 'heterogeneous'.  This allows the
    // client to begin sending homogeneous jobs before the scheduler uses them internally.
    if (copy.isSetTaskConfig()) {
      if (copy.getShardCount() <= 0) {
        throw new TaskDescriptionException("Shard count must be positive.");
      }
      if (copy.isSetTaskConfigs()) {
        throw new TaskDescriptionException(
            "'taskConfigs' and 'taskConfig' fields may not be used together.");
      }
      for (int i = 0; i < copy.getShardCount(); i++) {
        copy.addToTaskConfigs(copy.getTaskConfig().deepCopy().setShardId(i));
      }
    } else if (copy.getTaskConfigsSize() == 0) {
      throw new TaskDescriptionException("No tasks specified.");
    }

    Set<Integer> shardIds = Sets.newHashSet();

    // We need to fill in defaults on the template if one is present.
    if (copy.isSetTaskConfig()) {
      populateFields(copy, copy.getTaskConfig(), ShardIdState.ABSENT);
    }

    List<TwitterTaskInfo> configsCopy = Lists.newArrayList(copy.getTaskConfigs());
    for (TwitterTaskInfo config : configsCopy) {
      populateFields(copy, config, ShardIdState.PRESENT);
      if (!shardIds.add(config.getShardId())) {
        throw new TaskDescriptionException("Duplicate shard ID " + config.getShardId());
      }

      if (REQUIRE_CONTACT_EMAIL.get()
          && (!config.isSetContactEmail()
              || !config.getContactEmail().matches("[^@]+@twitter.com"))) {
        throw new TaskDescriptionException(
            "A valid twitter.com contact email address is required.");
      }

      Constraint constraint = getDedicatedConstraint(config);
      if (constraint == null) {
        continue;
      }

      if (!isValueConstraint(constraint.getConstraint())) {
        throw new TaskDescriptionException("A dedicated constraint must be of value type.");
      }

      ValueConstraint valueConstraint = constraint.getConstraint().getValue();

      if (!(valueConstraint.getValues().size() == 1)) {
        throw new TaskDescriptionException("A dedicated constraint must have exactly one value");
      }

      String dedicatedRole = getRole(valueConstraint);
      if (!job.getOwner().getRole().equals(dedicatedRole)) {
        throw new TaskDescriptionException(
            "Only " + dedicatedRole + " may use hosts dedicated for that role.");
      }
    }

    Set<TwitterTaskInfo> modifiedConfigs = ImmutableSet.copyOf(configsCopy);
    Preconditions.checkState(modifiedConfigs.size() == configsCopy.size(),
        "Task count changed after populating fields.");

    // Ensure that all production flags are equal.
    int numProductionTasks = Iterables.size(Iterables.filter(modifiedConfigs, Tasks.IS_PRODUCTION));
    if ((numProductionTasks != 0) && (numProductionTasks != modifiedConfigs.size())) {
      throw new TaskDescriptionException("Tasks within a job must use the same production flag.");
    }

    // The configs were mutated, so we need to refresh the Set.
    copy.setTaskConfigs(modifiedConfigs);

    // We might need to add a JobKey populated from the task shards.
    // TODO(ksweeney): Call maybe fill job key here.

    // Check for contiguous shard IDs.
    for (int i = 0; i < copy.getTaskConfigsSize(); i++) {
      if (!shardIds.contains(i)) {
        throw new TaskDescriptionException("Shard ID " + i + " is missing.");
      }
    }

    return copy;
  }

  /**
   * Populates fields in an individual task configuration, using the job as context.
   *
   * @param job The job that the task is a member of.
   * @param config Task to populate.
   * @param shardIdState Whether the task should have a shard ID or not.
   * @return A reference to the modified {@code config} (for chaining).
   * @throws TaskDescriptionException If the task is invalid.
   */
  @VisibleForTesting
  public static TwitterTaskInfo populateFields(
      JobConfiguration job,
      TwitterTaskInfo config,
      ShardIdState shardIdState)
      throws TaskDescriptionException {
    if (config == null) {
      throw new TaskDescriptionException("Task may not be null.");
    }

    if (shardIdState == ShardIdState.PRESENT) {
      if (!config.isSetShardId()) {
        throw new TaskDescriptionException("Tasks must have a shard ID.");
      }
    } else if (shardIdState == ShardIdState.ABSENT) {
      if (config.isSetShardId()) {
        throw new TaskDescriptionException("Template tasks must not have a shard ID.");
      }
    }

    if (!config.isSetRequestedPorts()) {
      config.setRequestedPorts(ImmutableSet.<String>of());
    }

    maybeFillLinks(config);

    config.setOwner(job.getOwner());
    config.setJobName(job.getName());

    // TODO(ksweeney): Remove the check for job.isSetKey when it's no longer optional.
    if (StringUtils.isBlank(config.getEnvironment()) && job.isSetKey()) {
      config.setEnvironment(job.getKey().getEnvironment());
    }

    // Only one of [service=true, cron_schedule] may be set.
    if (!StringUtils.isEmpty(job.getCronSchedule()) && config.isIsService()) {
      throw new TaskDescriptionException(
          "A service task may not be run on a cron schedule: " + config);
    }

    if (!config.isSetThermosConfig()) {
      throw new TaskDescriptionException("Configuration may not be null");
    }

    // Maximize the usefulness of any thrown error message by checking required fields first.
    for (RequiredFieldValidator<?> validator : REQUIRED_FIELDS_VALIDATORS) {
      validator.validate(config);
    }

    return applyDefaultsIfUnset(config);
  }

  /**
   * Provides a filter for the given constraint name.
   *
   * @param name The name of the constraint.
   * @return A filter that matches the constraint.
   */
  public static Predicate<Constraint> getConstraintByName(final String name) {
    return new Predicate<Constraint>() {
      @Override public boolean apply(Constraint constraint) {
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
      @Override public boolean apply(Constraint constraint) {
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
  public static TwitterTaskInfo applyDefaultsIfUnset(TwitterTaskInfo task) {
    for (Closure<TwitterTaskInfo> populator: DEFAULT_FIELD_POPULATORS) {
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
    for (TwitterTaskInfo task : job.getTaskConfigs()) {
      ConfigurationManager.applyDefaultsIfUnset(task);
    }
    // While we support heterogeneous tasks we need to backfill both the template and the replicas.
    if (job.isSetTaskConfig()) {
      ConfigurationManager.applyDefaultsIfUnset(job.getTaskConfig());
    }

    try {
      maybeFillJobKey(job);
    } catch (TaskDescriptionException e) {
      LOG.warning("Failed to fill job key in " + job + " due to " + e);
    }
  }

  private static void maybeFillLinks(TwitterTaskInfo task) {
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

  private static Optional<TwitterTaskInfo> getTemplateTaskInfo(JobConfiguration job) {
    // If no taskConfig (template) is set use an arbitrary shard to fill in missing job fields.
    return Optional.fromNullable(job.getTaskConfig())
        .or(Optional.fromNullable(Iterables.getFirst(job.getTaskConfigs(), null)));
  }

  /**
    * Fill in a missing job key from task fields.
    *
    * TODO(ksweeney): Make this private and call it in populateFields.
    *
    * @param job The job to mutate.
    * @throws TaskDescriptionException If job has no tasks.
    */
  public static void maybeFillJobKey(JobConfiguration job) throws TaskDescriptionException {
    if (JobKeys.isValid(job.getKey())) {
      return;
    }

    Optional<TwitterTaskInfo> template = getTemplateTaskInfo(job);
    if (!template.isPresent()) {
      throw new TaskDescriptionException("Job must have at least one task");
    }

    LOG.info("Attempting to synthesize key for job " + job + " using template task " + template);
    JobKey synthesizedJobKey = new JobKey()
        .setRole(job.getOwner().getRole())
        .setEnvironment(template.get().getEnvironment())
        .setName(job.getName());

    if (!JobKeys.isValid(synthesizedJobKey)) {
      throw new TaskDescriptionException(String.format(
          "Could not make valid key from template %s: synthesized key %s is invalid",
          template, synthesizedJobKey));
    }

    job.setKey(synthesizedJobKey);
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
