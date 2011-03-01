package com.twitter.mesos.updater;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.twitter.mesos.gen.UpdateConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.twitter.mesos.updater.ConfigParser.Type.NOT_NEGATIVE;
import static com.twitter.mesos.updater.ConfigParser.Type.POSITIVE;

/**
 * Handles parsing of the update config map and populating a config object.
 *
 * @author William Farner
 */
public class ConfigParser {

  public static void parseAndVerify(final UpdateConfig config) throws UpdateConfigException {
    Preconditions.checkNotNull(config);

    List<Field> fields = Arrays.asList(
        new Field("canary_size", 1, POSITIVE) {
          @Override void set(int value) { config.setCanaryTaskCount(value); }
          @Override boolean isAlreadySet() { return config.isSetCanaryTaskCount(); }
        },
        new Field("tolerated_canary_failures", 0, NOT_NEGATIVE) {
          @Override void set(int value) { config.setToleratedCanaryFailures(value); }
          @Override boolean isAlreadySet() { return config.isSetToleratedCanaryFailures(); }
        },
        new Field("canary_watch_secs", 30, POSITIVE) {
          @Override void set(int value) { config.setCanaryWatchDurationSecs(value); }
          @Override boolean isAlreadySet() { return config.isSetCanaryWatchDurationSecs(); }
        },
        new Field("update_batch_size", 1, POSITIVE) {
          @Override void set(int value) { config.setUpdateBatchSize(value); }
          @Override boolean isAlreadySet() { return config.isSetUpdateBatchSize(); }
        },
        new Field("tolerated_total_failures", 0, NOT_NEGATIVE) {
          @Override void set(int value) { config.setToleratedUpdateFailures(value); }
          @Override boolean isAlreadySet() { return config.isSetToleratedUpdateFailures(); }
        },
        new Field("update_watch_secs", 30, POSITIVE) {
          @Override void set(int value) { config.setUpdateWatchDurationSecs(value); }
          @Override boolean isAlreadySet() { return config.isSetUpdateWatchDurationSecs(); }
        },
        new Field("restart_timeout_secs", 30, POSITIVE) {
          @Override void set(int value) { config.setRestartTimeoutSecs(value); }
          @Override boolean isAlreadySet() { return config.isSetRestartTimeoutSecs(); }
        }
    );

    Map<String, String> map = config.getConfig();
    if (map == null) map = Maps.newHashMap();

    for (Field field : fields) {
      String mappedValue = map.get(field.mapKey);

      if (field.isAlreadySet()) {
        throw new UpdateConfigException("Field " + field.mapKey + " set before parsing, aborting.");
      }

      int value;
      try {
        value = mappedValue == null ? field.defaultValue : Integer.parseInt(mappedValue);
      } catch (NumberFormatException e) {
        throw new UpdateConfigException("Invalid number for field " + field.mapKey);
      }

      switch (field.validator) {
        case POSITIVE:
          if (value < 1) throw new UpdateConfigException(field.mapKey + " must be positive.");
          break;

        case NOT_NEGATIVE:
          if (value < 0) throw new UpdateConfigException(field.mapKey + " must be non-negative.");
          break;
      }

      field.set(value);
    }
  }

  enum Type {
    POSITIVE,
    NOT_NEGATIVE
  }

  private static abstract class Field {
    final String mapKey;
    private final int defaultValue;
    private final Type validator;

    Field(String mapKey, int defaultValue, Type validator) {
      this.mapKey = mapKey;
      this.defaultValue = defaultValue;
      this.validator = validator;
    }

    abstract void set(int value);
    abstract boolean isAlreadySet();
  }

  public static class UpdateConfigException extends Exception {
    public UpdateConfigException(String msg) {
      super(msg);
    }
  }

  private ConfigParser() {}
}
