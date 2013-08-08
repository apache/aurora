package com.twitter.aurora.scheduler;

import java.util.UUID;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.aurora.gen.TaskConfig;
import com.twitter.common.util.Clock;

/**
 * A function that generates universally-unique (not guaranteed, but highly confident) task IDs.
 */
class TaskIdGenerator implements Function<TaskConfig, String> {

  private final Clock clock;

  @Inject
  TaskIdGenerator(Clock clock) {
    this.clock = Preconditions.checkNotNull(clock);
  }

  @Override
  public String apply(TaskConfig task) {
    String sep = "-";
    return new StringBuilder()
        .append(clock.nowMillis())               // Allows chronological sorting.
        .append(sep)
        .append(task.getOwner().getRole())       // Identification and collision prevention.
        .append(sep)
        .append(task.getJobName())
        .append(sep)
        .append(task.getShardId())               // Collision prevention within job.
        .append(sep)
        .append(UUID.randomUUID())               // Just-in-case collision prevention.
        .toString().replaceAll("[^\\w-]", sep);  // Constrain character set.
  }
}
