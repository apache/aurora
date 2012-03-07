package com.twitter.mesos.scheduler.configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Positive;
import com.twitter.common.base.MorePreconditions;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LimitConstraint;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.ValueConstraint;
import com.twitter.mesos.scheduler.CommandLineExpander;
import com.twitter.mesos.scheduler.ThermosJank;
import com.twitter.mesos.scheduler.configuration.ValueParser.ParseException;

/**
 * Manages translation from a string-mapped configuration to a concrete configuration type, and
 * defaults for optional values.
 *
 * TODO(William Farner): Add input validation to all fields (strings not empty, positive ints, etc).
 *
 * @author William Farner
 */
public final class ConfigurationManager {

  public static final String DEDICATED_ATTRIBUTE = "dedicated";

  @VisibleForTesting public static final String HOST_CONSTRAINT = "host";

  @VisibleForTesting
  @Positive
  @CmdLine(name = "max_tasks_per_job", help = "Maximum number of allowed tasks in a single job.")
  public static final Arg<Integer> MAX_TASKS_PER_JOB = Arg.create(500);

  private static final Logger LOG = Logger.getLogger(ConfigurationManager.class.getName());

  private static final Pattern GOOD_IDENTIFIER_PATTERN = Pattern.compile("[\\w\\-\\.]+");

  private static final int MAX_IDENTIFIER_LENGTH = 255;

  private static final String START_COMMAND_FIELD = "start_command";

  private static final List<Field<?>> FIELDS = ImmutableList.<Field<?>>builder()
      .add(new TypedField<String>(String.class, "hdfs_path", null) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetHdfsPath(); }

        @Override void apply(TwitterTaskInfo task, String value) throws TaskDescriptionException {
          task.setHdfsPath(value);
        }
      })
      .add(new TypedField<String>(String.class, START_COMMAND_FIELD) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetStartCommand(); }

        @Override void apply(TwitterTaskInfo task, String value) throws TaskDescriptionException {
          task.setStartCommand(value);
        }
      })
      .add(new TypedField<Boolean>(Boolean.class, "daemon", false) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetIsDaemon(); }

        @Override void apply(TwitterTaskInfo task, Boolean value) throws TaskDescriptionException {
          task.setIsDaemon(value);
        }
      })
      .add(new TypedField<Double>(Double.class, "num_cpus") {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetNumCpus(); }

        @Override void apply(TwitterTaskInfo task, Double value) throws TaskDescriptionException {
          task.setNumCpus(value);
        }
      })
      .add(new TypedField<Integer>(Integer.class, "ram_mb") {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetRamMb(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setRamMb(value);
        }
      })
      .add(new TypedField<Integer>(Integer.class, "disk_mb") {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetDiskMb(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setDiskMb(value);
        }
      })
      .add(new TypedField<Integer>(Integer.class, "priority", 0) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetPriority(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setPriority(value);
        }
      })
      .add(new TypedField<Boolean>(Boolean.class, "production", false) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetProduction(); }

        @Override void apply(TwitterTaskInfo task, Boolean value) throws TaskDescriptionException {
          task.setProduction(value);
        }
      })
      .add(new TypedField<Integer>(Integer.class, "health_check_interval_secs", 30) {
        @Override boolean isSet(TwitterTaskInfo task) {
          return task.isSetHealthCheckIntervalSecs();
        }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setHealthCheckIntervalSecs(value);
        }
      })
      .add(new TypedField<Integer>(Integer.class, "max_task_failures", 1) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetMaxTaskFailures(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setMaxTaskFailures(value);
        }
      })
      .add(new TypedField<Integer>(Integer.class, "max_per_host", 1) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetMaxPerHost(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setMaxPerHost(value);

          Constraint hostConstraint =
              Iterables.find(task.getConstraints(), hasName(HOST_CONSTRAINT), null);
          if (hostConstraint == null) {
            LOG.info("Task configuration uses deprecated max_per_host.");

            // TODO(wfarner): Remove this once the mesos client is updated to supply it.
            task.addToConstraints(hostLimitConstraint(1));
          }
        }
      })
      .add(new Field<Set<String>>("avoid_jobs", ImmutableSet.<String>of()) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetAvoidJobs(); }

        @Override Set<String> parse(String raw) throws ParseException {
          return getSet(raw, String.class);
        }

        @Override public void apply(TwitterTaskInfo task, Set<String> value)
            throws TaskDescriptionException {
          task.setAvoidJobs(value);
        }
      })
      .build();

  private ConfigurationManager() {
    // Utility class.
  }

  private static boolean isGoodIdentifier(String identifier) {
    return GOOD_IDENTIFIER_PATTERN.matcher(identifier).matches()
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
    String role = job.getOwner().getRole();

    if (job.getTaskConfigsSize() > MAX_TASKS_PER_JOB.get()) {
      throw new TaskDescriptionException("Job exceeds task limit of " + MAX_TASKS_PER_JOB.get());
    }

    JobConfiguration copy = job.deepCopy();

    assertOwnerValidity(job.getOwner());

    if (!isGoodIdentifier(copy.getName())) {
      throw new TaskDescriptionException("Job name contains illegal characters: " + copy.getName());
    }

    if (copy.getTaskConfigsSize() == 0) {
      throw new TaskDescriptionException("No tasks specified.");
    }

    Set<Integer> shardIds = Sets.newHashSet();

    List<TwitterTaskInfo> configsCopy = Lists.newArrayList(copy.getTaskConfigs());
    for (TwitterTaskInfo config : configsCopy) {
      populateFields(copy, config);
      if (!shardIds.add(config.getShardId())) {
        throw new TaskDescriptionException("Duplicate shard ID " + config.getShardId());
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
      if (!role.equals(dedicatedRole)) {
        throw new TaskDescriptionException(
            "Only " + dedicatedRole + " may use hosts dedicated for that role.");
      }
    }

    Set<TwitterTaskInfo> modifiedConfigs = ImmutableSet.copyOf(configsCopy);
    Preconditions.checkState(modifiedConfigs.size() == configsCopy.size(),
        "Task count changed after populating fields.");

    // Ensure that all production flags are equal.
    int numProductionTasks =
        Iterables.size(Iterables.filter(modifiedConfigs, Tasks.IS_PRODUCTION));
    if ((numProductionTasks != 0) && (numProductionTasks != modifiedConfigs.size())) {
      throw new TaskDescriptionException("Tasks within a job must use the same production flag.");
    }

    // The configs were mutated, so we need to refresh the Set.
    copy.setTaskConfigs(modifiedConfigs);

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
   * @return A reference to the modified {@code config} (for chaining).
   * @throws TaskDescriptionException If the task is invalid.
   */
  @ThermosJank
  @VisibleForTesting
  public static TwitterTaskInfo populateFields(JobConfiguration job, TwitterTaskInfo config)
      throws TaskDescriptionException {
    if (config == null) {
      throw new TaskDescriptionException("Task may not be null.");
    }

    if (config.isSetThermosConfig()) {
      config.setConfigParsed(true);
      return config;
    }

    if (config.getConfiguration() == null) {
      throw new TaskDescriptionException("Task configuration may not be null");
    }

    if (!config.isSetShardId()) {
      throw new TaskDescriptionException("Tasks must have a shard ID.");
    }

    if (!config.isSetRequestedPorts()) {
      config.setRequestedPorts(ImmutableSet.<String>of());
    }

    config.setOwner(job.getOwner());
    config.setJobName(job.getName());

    assertUnset(config);
    populateFields(config);

    // Only one of [daemon=true, cron_schedule] may be set.
    if (!StringUtils.isEmpty(job.getCronSchedule()) && config.isIsDaemon()) {
      throw new TaskDescriptionException(
          "A daemon task may not be run on a cron schedule: " + config);
    }

    config.setConfigParsed(true);

    return config;
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

  /**
   * A redundant class that should collapse into {@link TypedField}.
   * TODO(wfarner): Collapse this into TypedField.
   *
   * @param <T> Field type.
   */
  private abstract static class Field<T> {
    private final String key;
    private final T defaultValue;
    private final boolean required;

    private Field(String key) {
      this.key = key;
      this.defaultValue = null;
      this.required = true;
    }

    private Field(String key, T defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;
      this.required = false;
    }

    abstract boolean isSet(TwitterTaskInfo task);

    private void applyDefault(TwitterTaskInfo task) {
      try {
        apply(task, defaultValue);
      } catch (TaskDescriptionException e) {
        // Switch to runtime, since defaults should be safe.
        throw new IllegalStateException("Default value " + defaultValue + " rejected for " + key);
      }
    }

    abstract T parse(String raw) throws ParseException;

    abstract void apply(TwitterTaskInfo task, T value) throws TaskDescriptionException;

    void parseAndApply(TwitterTaskInfo task, String raw) throws TaskDescriptionException {
      try {
        apply(task, parse(raw));
      } catch (ParseException e) {
          throw new TaskDescriptionException("Value [" + raw
              + "] cannot be applied to field " + key + ", " + e.getMessage());
      }
    }

    T getDefaultValue() {
      return defaultValue;
    }

    boolean isRequired() {
      return required;
    }
  }

  /**
   * A typed configuration field that knows how to extract and parse a value from the raw
   * configuration, and apply the value to the appropriate task struct field.
   *
   * @param <T> Field type.
   */
  private abstract static class TypedField<T> extends Field<T> {
    private final ValueParser<T> parser;

    TypedField(Class<T> type, String key) {
      super(key);
      this.parser = ValueParser.Registry.getParser(type).get();
    }

    TypedField(Class<T> type, String key, T defaultValue) {
      super(key, defaultValue);
      this.parser = ValueParser.Registry.getParser(type).get();
    }

    @Override public T parse(String raw) throws ParseException {
      return isRequired() ? parser.parse(raw) : parser.parseWithDefault(raw, getDefaultValue());
    }
  }

  /**
   * Resets the {@code start_command} to the original value in the task's configuration.  This can
   * be used to undo any command-line expansion in a previous iteration of a task.
   *
   * @param task Task whose {@code start_command} should be reset.
   */
  public static void resetStartCommand(TwitterTaskInfo task) {
    Preconditions.checkNotNull(task);
    // This condition realistically only fails in unit tests.
    if (task.isSetConfiguration()) {
      task.setStartCommand(task.getConfiguration().get(START_COMMAND_FIELD));
    }
  }

  @VisibleForTesting
  public static Constraint hostLimitConstraint(int limit) {
    return new Constraint(HOST_CONSTRAINT, TaskConstraint.limit(new LimitConstraint(limit)));
  }

  private static Predicate<Constraint> hasName(final String name) {
    MorePreconditions.checkNotBlank(name);
    return new Predicate<Constraint>() {
      @Override public boolean apply(Constraint constraint) {
        return name.equals(constraint.getName());
      }
    };
  }

  private static void assertUnset(TwitterTaskInfo task) throws TaskDescriptionException {
    for (Field<?> field : FIELDS) {
      if (field.isSet(task)) {
        throw new TaskDescriptionException("Task field set before parsing: " + field.key);
      }
    }
  }

  private static TwitterTaskInfo populateFields(TwitterTaskInfo task)
      throws TaskDescriptionException {
    Map<String, String> config = task.getConfiguration();

    fillDataFields(task);

    for (Field<?> field : FIELDS) {
      String rawValue = config.get(field.key);
      if (rawValue == null) {
        if (field.required) {
          throw new TaskDescriptionException("Field " + field.key + " is required.");
        } else {
          field.applyDefault(task);
        }
      } else {
        field.parseAndApply(task, rawValue);
      }
    }

    return task;
  }

  /**
   * Applies defaults to unset values in a task.
   *
   * @param task Task to apply defaults to.
   * @return A reference to the (modified) {@code task}.
   */
  @VisibleForTesting
  public static TwitterTaskInfo applyDefaultsIfUnset(TwitterTaskInfo task) {
    fillDataFields(task);

    for (Field<?> field : FIELDS) {
      if (!field.isSet(task)) {
        field.applyDefault(task);
      }
    }

    // TODO(wfarner): Remove this when new client is fully deployed and all tasks are backfilled.
    maybeFillRequestedPorts(task);

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
  }

  private static void fillDataFields(TwitterTaskInfo task) {
    if (!task.isSetThermosConfig()) {
      // Workaround for thrift 0.5.0 NPE.  See MESOS-370.
      task.setThermosConfig(new byte[] {});
    }

    if (!task.isSetConstraints()) {
      task.setConstraints(Sets.<Constraint>newHashSet());
    }
  }

  private static void maybeFillRequestedPorts(TwitterTaskInfo task) {
    if (!task.isSetRequestedPorts()) {
      task.setRequestedPorts(CommandLineExpander.getPortNames(task.getStartCommand()));
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

  @SuppressWarnings("unchecked")
  private static <T> Set<T> getSet(String value, Class<T> type)
    throws ParseException {
    ImmutableSet.Builder<T> builder = ImmutableSet.builder();
    if (!StringUtils.isEmpty(value)) {
      for (String item : Splitter.on(',').split(value)) {
        builder.add(ValueParser.Registry.getParser(type).get().parse(item));
      }
    }
    return builder.build();
  }
}
