package com.twitter.aurora.scheduler.configuration;

import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.TwitterTaskInfo;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;

/**
 * Wrapper for a configuration that has been fully-parsed and populated with defaults.
 * TODO(wfarner): Rename this to SanitizedConfiguration.
 */
public final class ParsedConfiguration {

  private final JobConfiguration parsed;
  private final Set<TwitterTaskInfo> tasks;

  /**
   * Constructs a ParsedConfiguration object and populates the set of {@link TwitterTaskInfo}s for
   * the provided config.
   *
   * @param parsed A parsed {@link JobConfiguration}.
   */
  @VisibleForTesting
  public ParsedConfiguration(JobConfiguration parsed) {
    this.parsed = parsed;
    ImmutableSet.Builder<TwitterTaskInfo> builder = ImmutableSet.builder();
    for (int i = 0; i < parsed.getShardCount(); i++) {
      builder.add(parsed.getTaskConfig().deepCopy().setShardId(i));
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
  public static ParsedConfiguration fromUnparsed(JobConfiguration unparsed)
      throws TaskDescriptionException {

    Preconditions.checkNotNull(unparsed);
    return new ParsedConfiguration(ConfigurationManager.validateAndPopulate(unparsed));
  }

  public JobConfiguration getJobConfig() {
    return parsed;
  }

  public Set<TwitterTaskInfo> getTaskConfigs() {
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
