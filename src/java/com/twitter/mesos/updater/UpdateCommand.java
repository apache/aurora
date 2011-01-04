package com.twitter.mesos.updater;

import org.apache.commons.lang.builder.EqualsBuilder;

import java.util.Set;

/**
 * Configuration options for a phase in an update flow.
 *
 * @author wfarner
 */
class UpdateCommand {
  static enum Type {
    UPDATE_TASK,
    ROLLBACK_TASK
  }

  final Type type;
  final Set<Integer> shardIds;
  final int restartTimeoutSecs;
  final int updateWatchSecs;
  final int allowedFailures;

  UpdateCommand(Type type, Set<Integer> shardIds, int restartTimeoutSecs, int updateWatchSecs,
      int allowedFailures) {
    this.type = type;
    this.shardIds = shardIds;
    this.restartTimeoutSecs = restartTimeoutSecs;
    this.updateWatchSecs = updateWatchSecs;
    this.allowedFailures = allowedFailures;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof UpdateCommand)) {
      return false;
    }

    UpdateCommand that = (UpdateCommand) other;
    return new EqualsBuilder()
        .append(type, that.type)
        .append(shardIds, that.shardIds)
        .append(restartTimeoutSecs, that.restartTimeoutSecs)
        .append(allowedFailures, that.allowedFailures)
        .append(updateWatchSecs, that.updateWatchSecs)
        .isEquals();
  }

  @Override public String toString() {
    return "UpdateCommand{" + "type=" + type + ", shardIds=" + shardIds + ", restartTimeoutSecs="
           + restartTimeoutSecs + ", updateWatchSecs=" + updateWatchSecs + ", allowedFailures="
           + allowedFailures + '}';
  }
}
