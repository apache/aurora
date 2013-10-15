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
package com.twitter.aurora.scheduler.configuration;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import com.twitter.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Wrapper for a configuration that has been fully-parsed and populated with defaults.
 * TODO(wfarner): Rename this to SanitizedConfiguration.
 */
public final class ParsedConfiguration {

  private final IJobConfiguration parsed;
  private final Map<Integer, ITaskConfig> tasks;

  /**
   * Constructs a ParsedConfiguration object and populates the set of {@link ITaskConfig}s for
   * the provided config.
   *
   * @param parsed A parsed configuration..
   */
  @VisibleForTesting
  public ParsedConfiguration(IJobConfiguration parsed) {
    this.parsed = parsed;

    // TODO(William Farner): Use Range.closedOpen, Maps.toMap, and Functions.constant once
    //                       TaskConfig instances are not duplicated.
    ImmutableMap.Builder<Integer, ITaskConfig> builder = ImmutableMap.builder();
    for (int i = 0; i < parsed.getInstanceCount(); i++) {
      builder.put(i, ITaskConfig.build(parsed.getTaskConfig().newBuilder().setInstanceId(i)));
    }
    this.tasks = builder.build();
  }

  /**
   * Wraps an unparsed job configuration.
   *
   * @param unparsed Unparsed configuration to parse/populate and wrap.
   * @return A wrapper containing the parsed configuration.
   * @throws TaskDescriptionException If the configuration is invalid.
   */
  public static ParsedConfiguration fromUnparsed(IJobConfiguration unparsed)
      throws TaskDescriptionException {

    return new ParsedConfiguration(ConfigurationManager.validateAndPopulate(unparsed));
  }

  public IJobConfiguration getJobConfig() {
    return parsed;
  }

  public Map<Integer, ITaskConfig> getTaskConfigs() {
    return tasks;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ParsedConfiguration)) {
      return false;
    }

    ParsedConfiguration other = (ParsedConfiguration) o;

    return Objects.equal(parsed, other.parsed);
  }

  @Override
  public int hashCode() {
    return parsed.hashCode();
  }

  @Override
  public String toString() {
    return parsed.toString();
  }
}
