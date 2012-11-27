package com.twitter.mesos.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;

/**
 * Wrapper for a configuration that has been fully-parsed and populated with defaults.
 */
public final class ParsedConfiguration {

  private static final Predicate<TwitterTaskInfo> IS_PARSED = new Predicate<TwitterTaskInfo>() {
    @Override public boolean apply(TwitterTaskInfo task) {
      return task.isConfigParsed();
    }
  };

  private final JobConfiguration parsed;

  @VisibleForTesting
  ParsedConfiguration(JobConfiguration parsed) throws TaskDescriptionException {
    this.parsed = parsed;
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
    Preconditions.checkArgument(Iterables.all(unparsed.getTaskConfigs(), Predicates.not(IS_PARSED)),
        "Cannot be invoked with a parsed configuration.");
    return new ParsedConfiguration(ConfigurationManager.validateAndPopulate(unparsed));
  }

  public JobConfiguration get() {
    return parsed;
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
