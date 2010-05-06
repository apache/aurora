package com.twitter.nexus.scheduler;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.nexus.gen.TwitterTaskInfo;
import nexus.SlaveOffer;
import nexus.StringMap;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Manages translation from a string-mapped configuration to a concrete configuration type, and
 * defaults for optional values.
 *
 * @author wfarner
 */
public class ConfigurationManager {
  private static Logger LOG = Logger.getLogger(ConfigurationManager.class.getName());

  private static final boolean DEFAULT_TO_DAEMON = false;
  private static final double DEFAULT_NUM_CPUS = 0.1;
  private static final long DEFAULT_RAM_BYTES = Amount.of(1, Data.GB).as(Data.BYTES);
  private static final long DEFAULT_DISK_BYTES = Amount.of(1, Data.GB).as(Data.BYTES);
  private static final int DEFAULT_PRIORITY = 0;

  public static TwitterTaskInfo parse(TwitterTaskInfo config)
      throws TaskDescriptionException {
    if ( config == null) throw new TaskDescriptionException("Task may not be null.");

    Map<String, String> configMap =  config.getConfiguration();
    if (configMap == null) throw new TaskDescriptionException("Task configuration may not be null");

    return config
      .setHdfsPath(getValue(configMap, "hdfs_path", String.class))
      .setCmdLineArgs(getValue(configMap, "cmd_line_args", "", String.class))
      .setNumCpus(getValue(configMap, "num_cpus", DEFAULT_NUM_CPUS, Double.class))
      .setRamBytes(getValue(configMap, "ram_bytes", DEFAULT_RAM_BYTES, Long.class));
  }

  public static TwitterTaskInfo makeConcrete(SlaveOffer offer)
      throws TaskDescriptionException {
    if (offer== null) throw new TaskDescriptionException("Task may not be null.");

    StringMap params = offer.getParams();
    if (params == null) throw new TaskDescriptionException("Task configuration may not be null");

    return new TwitterTaskInfo()
      .setNumCpus(getValue(params, "cpus", Double.class))
      .setRamBytes(getValue(params, "mem", Long.class));

    /* TODO(wfarner): Make configuration more generic in nexus.
    return new TwitterTaskInfo()
      .setNumCpus(getValue(params, "num_cpus", Double.class))
      .setRamBytes(getValue(params, "ram_bytes", Long.class))
      .setRamBytes(getValue(params, "disk_bytes", Long.class));
     */
  }

  public static boolean satisfied(TwitterTaskInfo request, TwitterTaskInfo offer) {
    return request.getNumCpus() <= offer.getNumCpus()
        && request.getRamBytes() <= offer.getRamBytes()
        && request.getDiskBytes() <= offer.getRamBytes();
  }

  public static class TaskDescriptionException extends Exception {
    public TaskDescriptionException(String msg) {
      super(msg);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T getValue(Map<String, String> config, String key, Class<T> type)
      throws TaskDescriptionException {
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

  @SuppressWarnings("unchecked")
  private static <T> T getValue(StringMap params, String key, Class<T> type)
      throws TaskDescriptionException {
    if (!params.has_key(key)) throw new TaskDescriptionException("Must specify value for " + key);
    try {
      return (T) ValueParser.REGISTRY.get(type).parse(params.get(key));
    } catch (ValueParser.ParseException e) {
      throw new TaskDescriptionException("Invalid value for " + key + ": " + e.getMessage());
    }
  }
}
