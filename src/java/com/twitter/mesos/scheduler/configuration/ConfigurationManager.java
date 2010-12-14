package com.twitter.mesos.scheduler.configuration;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ValueParser.ParseException;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages translation from a string-mapped configuration to a concrete configuration type, and
 * defaults for optional values.
 *
 * TODO(wfarner): Add input validation to all fields (strings not empty, positive ints, etc).
 *
 * @author wfarner
 */
public class ConfigurationManager {

  public static TwitterTaskInfo populateFields(JobConfiguration job, TwitterTaskInfo config)
      throws TaskDescriptionException {
    if (config == null) throw new TaskDescriptionException("Task may not be null.");

    Map<String, String> configMap =  config.getConfiguration();
    if (configMap == null) throw new TaskDescriptionException("Task configuration may not be null");

    config.setOwner(job.getOwner()).setJobName(job.getName());

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

  private static abstract class Field<T> {
    final String key;
    final T defaultValue;
    final boolean required;

    Field(String key, T defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;
      required = defaultValue == null;
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

  private static abstract class TypedField<T> extends Field<T> {
    final Class<T> type;
    final ValueParser<T> parser;

    TypedField(Class<T> type, String key) {
      this(type, key, null);
    }

    @SuppressWarnings("unchecked")
    TypedField(Class<T> type, String key, T defaultValue) {
      super(key, defaultValue);
      this.type = type;
      parser = ValueParser.REGISTRY.get(type);
    }

    @Override
    public T parse(String raw) throws ParseException {
      return parser.parse(raw);
    }
  }

  private static List<Field<?>> FIELDS = ImmutableList.of(
      new TypedField<String>(String.class, "hdfs_path") {
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
      new TypedField<Double>(Double.class, "num_cpus", 1.0) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetRamMb(); }

        @Override void apply(TwitterTaskInfo task, Double value) throws TaskDescriptionException {
          task.setNumCpus(value);
        }
      },
      new TypedField<Integer>(Integer.class, "ram_mb", 1024) {
        @Override boolean isSet(TwitterTaskInfo task) { return task.isSetRamMb(); }

        @Override void apply(TwitterTaskInfo task, Integer value) throws TaskDescriptionException {
          task.setRamMb(value);
        }
      },
      new TypedField<Integer>(Integer.class, "disk_mb", 1024) {
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

  public static TwitterTaskInfo makeConcrete(Map<String, String> params)
      throws TaskDescriptionException {
    if (params == null) throw new TaskDescriptionException("Task configuration may not be null");

    return new TwitterTaskInfo()
      .setNumCpus(getValue(params, "cpus", Double.class))
      .setRamMb(getValue(params, "mem", Long.class))
      .setDiskMb(100 * 1024); // TODO(wfarner): Get this included in offers when mesos supports it.
  }

  public static boolean satisfied(TwitterTaskInfo request, TwitterTaskInfo offer) {
    return request.getNumCpus() <= offer.getNumCpus()
        && request.getRamMb() <= offer.getRamMb()
        && request.getDiskMb() <= offer.getDiskMb();
  }

  public static class TaskDescriptionException extends Exception {
    public TaskDescriptionException(String msg) {
      super(msg);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T getValue(Map<String, String> config, String key, Class<T> type)
      throws TaskDescriptionException {
    if (!config.containsKey(key)) {
      throw new TaskDescriptionException("Must specify value for " + key);
    }
    try {
      return (T) ValueParser.REGISTRY.get(type).parse(config.get(key));
    } catch (ValueParser.ParseException e) {
      throw new TaskDescriptionException("Invalid value for " + key + ": " + e.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T getValue(Map<String, String> config, String key, T defaultValue,
      Class<T> type) throws TaskDescriptionException {
    try {
      return (T) ValueParser.REGISTRY.get(type).parseWithDefault(config.get(key), defaultValue);
    } catch (ValueParser.ParseException e) {
      throw new TaskDescriptionException("Invalid value for " + key + ": " + e.getMessage());
    }
  }

  private static final Splitter MULTI_VALUE_SPLITTER = Splitter.on(",");

  @SuppressWarnings("unchecked")
  private static <T> Set<T> getSet(String value, Class<T> type)
    throws ParseException {
    ImmutableSet.Builder<T> builder = ImmutableSet.builder();
    if (value != null) {
      for (String item : MULTI_VALUE_SPLITTER.split(value)) {
        builder.add((T) ValueParser.REGISTRY.get(type).parse(item));
      }
    }
    return builder.build();
  }
}
