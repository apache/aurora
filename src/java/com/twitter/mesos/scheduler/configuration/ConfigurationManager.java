package com.twitter.mesos.scheduler.configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.CommandLineExpander;
import com.twitter.mesos.scheduler.configuration.ValueParser.ParseException;

/**
 * Manages translation from a string-mapped configuration to a concrete configuration type, and
 * defaults for optional values.
 *
 * TODO(William Farner): Add input validation to all fields (strings not empty, positive ints, etc).
 *
 * @author William Farner
 */
public class ConfigurationManager {

  private static final Pattern GOOD_IDENTIFIER_PATTERN = Pattern.compile("[\\w\\-\\.]+");
  private static final int MAX_IDENTIFIED_LENGTH = 255;

  private static boolean isGoodIdentifier(String identifier) {
    return GOOD_IDENTIFIER_PATTERN.matcher(identifier).matches()
           && (identifier.length() <= MAX_IDENTIFIED_LENGTH);
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
      throw new TaskDescriptionException("Job role contains illegal characters: " +
          jobOwner.getRole());
    }

    if (!isGoodIdentifier(jobOwner.getUser())) {
      throw new TaskDescriptionException("Job user contains illegal characters: " +
          jobOwner.getUser());
    }
  }

  public static JobConfiguration validateAndPopulate(JobConfiguration job)
      throws TaskDescriptionException {
    Preconditions.checkNotNull(job);

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

  @VisibleForTesting
  public static TwitterTaskInfo populateFields(JobConfiguration job, TwitterTaskInfo config)
      throws TaskDescriptionException {
    if (config == null) {
      throw new TaskDescriptionException("Task may not be null.");
    }

    if (config.getConfiguration() == null) {
      throw new TaskDescriptionException("Task configuration may not be null");
    }

    if (!config.isSetShardId()) {
      throw new TaskDescriptionException("Tasks must have a shard ID.");
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

  private abstract static class Field<T> {
    final String key;
    final T defaultValue;
    final boolean required;

    Field(String key) {
      this.key = key;
      this.defaultValue = null;
      this.required = true;
    }

    Field(String key, T defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;
      this.required = false;
    }

    abstract boolean isSet(TwitterTaskInfo task);

    void applyDefault(TwitterTaskInfo task) {
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
  }

  private abstract static class TypedField<T> extends Field<T> {
    final Class<T> type;
    final ValueParser<T> parser;

    TypedField(Class<T> type, String key) {
      super(key);
      this.type = type;
      this.parser = ValueParser.Registry.getParser(type);
    }

    TypedField(Class<T> type, String key, T defaultValue) {
      super(key, defaultValue);
      this.type = type;
      this.parser = ValueParser.Registry.getParser(type);
    }

    @Override public T parse(String raw) throws ParseException {
      return required ? parser.parse(raw) : parser.parseWithDefault(raw, defaultValue);
    }
  }

  private static final List<Field<?>> FIELDS = ImmutableList.of(
      new TypedField<String>(String.class, "hdfs_path", null) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetHdfsPath(); }

        @Override void apply(TwitterTaskInfo task, String value) throws TaskDescriptionException {
          task.setHdfsPath(value);
        }
      },
      new TypedField<String>(String.class, "start_command") {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetStartCommand(); }

        @Override void apply(TwitterTaskInfo task, String value) throws TaskDescriptionException {
          task.setStartCommand(value);
        }
      },
      new TypedField<Boolean>(Boolean.class, "daemon", false) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetIsDaemon(); }

        @Override void apply(TwitterTaskInfo task, Boolean value) throws TaskDescriptionException {
          task.setIsDaemon(value);
        }
      },
      new TypedField<Double>(Double.class, "num_cpus") {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetNumCpus(); }

        @Override void apply(TwitterTaskInfo task, Double value) throws TaskDescriptionException {
          task.setNumCpus(value);
        }
      },
      new TypedField<Integer>(Integer.class, "ram_mb") {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetRamMb(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setRamMb(value);
        }
      },
      new TypedField<Integer>(Integer.class, "disk_mb") {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetDiskMb(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setDiskMb(value);
        }
      },
      new TypedField<Integer>(Integer.class, "priority", 0) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetPriority(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setPriority(value);
        }
      },
      new TypedField<Boolean>(Boolean.class, "production", false) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetProduction(); }

        @Override void apply(TwitterTaskInfo task, Boolean value) throws TaskDescriptionException {
          task.setProduction(value);
        }
      },
      new TypedField<Integer>(Integer.class, "health_check_interval_secs", 30) {
        @Override boolean isSet(TwitterTaskInfo task) {
          return task.isSetHealthCheckIntervalSecs();
        }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setHealthCheckIntervalSecs(value);
        }
      },
      new TypedField<Integer>(Integer.class, "max_task_failures", 1) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetMaxTaskFailures(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setMaxTaskFailures(value);
        }
      },
      new TypedField<Integer>(Integer.class, "max_per_host", 1) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetMaxPerHost(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setMaxPerHost(value);
        }
      },
      new Field<Set<String>>("avoid_jobs", ImmutableSet.<String>of()) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetAvoidJobs(); }

        @Override Set<String> parse(String raw) throws ParseException {
          return getSet(raw, String.class);
        }

        @Override public void apply(TwitterTaskInfo task, Set<String> value)
            throws TaskDescriptionException {
          task.setAvoidJobs(value);
        }
      }
  );

  public static void assertUnset(TwitterTaskInfo task) throws TaskDescriptionException {
    for (Field<?> field : FIELDS) {
      if (field.isSet(task)) {
        throw new TaskDescriptionException("Task field set before parsing: " + field.key);
      }
    }
  }

  public static TwitterTaskInfo populateFields(TwitterTaskInfo task)
      throws TaskDescriptionException {
    Map<String, String> config = task.getConfiguration();

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

  public static TwitterTaskInfo applyDefaultsIfUnset(TwitterTaskInfo task) {
    for (Field<?> field : FIELDS) {
      if (!field.isSet(task)) {
        field.applyDefault(task);
      }
    }
    return task;
  }

  public static class TaskDescriptionException extends Exception {
    public TaskDescriptionException(String msg) {
      super(msg);
    }
  }

  private static final Splitter MULTI_VALUE_SPLITTER = Splitter.on(",");

  @SuppressWarnings("unchecked")
  private static <T> Set<T> getSet(String value, Class<T> type)
    throws ParseException {
    ImmutableSet.Builder<T> builder = ImmutableSet.builder();
    if (!StringUtils.isEmpty(value)) {
      for (String item : MULTI_VALUE_SPLITTER.split(value)) {
        builder.add(ValueParser.Registry.getParser(type).parse(item));
      }
    }
    return builder.build();
  }
}
