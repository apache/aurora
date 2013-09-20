/*
 * Copyright 2013 Twitter, Inc.
 *
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
        .append(task.getEnvironment())
        .append(sep)
        .append(task.getJobName())
        .append(sep)
        .append(task.getShardId())               // Collision prevention within job.
        .append(sep)
        .append(UUID.randomUUID())               // Just-in-case collision prevention.
        .toString().replaceAll("[^\\w-]", sep);  // Constrain character set.
  }
}
