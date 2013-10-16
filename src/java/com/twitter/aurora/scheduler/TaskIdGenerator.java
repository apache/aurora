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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.common.util.Clock;

/**
 * A function that generates universally-unique (not guaranteed, but highly confident) task IDs.
 */
public interface TaskIdGenerator {

  /**
   * Generates a universally-unique ID for the task.  This is not necessarily a repeatable
   * operation, two subsequent invocations with the same object need not return the same value.
   *
   * @param task Task to generate an ID for.
   * @return Unique ID for the task.
   */
  String generate(ITaskConfig task);

  class TaskIdGeneratorImpl implements TaskIdGenerator {
    private final Clock clock;

    @Inject
    TaskIdGeneratorImpl(Clock clock) {
      this.clock = Preconditions.checkNotNull(clock);
    }

    @Override
    public String generate(ITaskConfig task) {
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
          .append(task.getInstanceIdDEPRECATED())  // Collision prevention within job.
          .append(sep)
          .append(UUID.randomUUID())               // Just-in-case collision prevention.
          .toString().replaceAll("[^\\w-]", sep);  // Constrain character set.
    }
  }
}
