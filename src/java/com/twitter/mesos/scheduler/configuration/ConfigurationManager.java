package com.twitter.mesos.scheduler.configuration;

import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.TwitterTaskInfo;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Manages translation from a string-mapped configuration to a concrete configuration type, and
 * defaults for optional values.
 *
 * TODO(wfarner): Add input validation to all fields (strings not empty, positive ints, etc).
 *
 * @author wfarner
 */
public class ConfigurationManager {
  private static Logger LOG = Logger.getLogger(ConfigurationManager.class.getName());

  private static final boolean DEFAULT_TO_DAEMON = false;
  private static final double DEFAULT_NUM_CPUS = 1.0;
  private static final long DEFAULT_RAM_MB = 1024;
  private static final long DEFAULT_DISK_MB = 1024;
  private static final int DEFAULT_PRIORITY = 0;
  private static final int DEFAULT_HEALTH_CHECK_INTERVAL_SECS = 30;
  private static final int DEFAULT_MAX_TASK_FAILURES = 1;

  public static TwitterTaskInfo populateFields(JobConfiguration job, TwitterTaskInfo config)
      throws TaskDescriptionException {
    if (config == null) throw new TaskDescriptionException("Task may not be null.");

    Map<String, String> configMap =  config.getConfiguration();
    if (configMap == null) throw new TaskDescriptionException("Task configuration may not be null");

    config
        .setConfigParsed(true)
        .setOwner(job.getOwner())
        .setJobName(job.getName())
        .setHdfsPath(getValue(configMap, "hdfs_path", String.class))
        .setStartCommand(getValue(configMap, "start_command", String.class))
        .setIsDaemon(getValue(configMap, "daemon", DEFAULT_TO_DAEMON, Boolean.class))
        .setNumCpus(getValue(configMap, "num_cpus", DEFAULT_NUM_CPUS, Double.class))
        .setRamMb(getValue(configMap, "ram_mb", DEFAULT_RAM_MB, Long.class))
        .setDiskMb(getValue(configMap, "disk_bytes", DEFAULT_DISK_MB, Long.class))
        .setPriority(getValue(configMap, "priority", DEFAULT_PRIORITY, Integer.class))
        .setHealthCheckIntervalSecs(getValue(configMap, "health_check_interval_secs",
            DEFAULT_HEALTH_CHECK_INTERVAL_SECS, Integer.class))
        .setMaxTaskFailures(getValue(configMap, "max_task_failures", DEFAULT_MAX_TASK_FAILURES,
            Integer.class));

    // Only one of [daemon=true, cron_schedule] may be set.
    if (!StringUtils.isEmpty(job.getCronSchedule()) && config.isIsDaemon()) {
      throw new TaskDescriptionException(
          "A daemon task may not be run on a cron schedule: " + config);
    }

    return config;
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
    if (!config.containsKey(key)) throw new TaskDescriptionException("Must specify value for " + key);
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
}
