package com.twitter.mesos.scheduler.base;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;

import com.twitter.common.base.MorePreconditions;

/**
 * Utility class for constructing {@link CommandInfo} objects given an executor URI.
 */
public final class CommandUtil {

  private CommandUtil() {
    // Utility class.
  }

  private static String uriBasename(String uri) {
    int lastSlash = uri.lastIndexOf("/");
    if (lastSlash == -1) {
      return uri;
    } else {
      String basename = uri.substring(lastSlash + 1);
      MorePreconditions.checkNotBlank(basename, "URI must not end with a slash.");

      return basename;
    }
  }

  /**
   * Creates a description of a command that will fetch and execute the given URI to an executor
   * binary.
   *
   * @param executorUri URI to the executor.
   * @return A command that will fetch and execute the executor.
   */
  public static CommandInfo create(String executorUri) {
    MorePreconditions.checkNotBlank(executorUri);

    return CommandInfo.newBuilder()
        .addUris(URI.newBuilder().setValue(executorUri).setExecutable(true))
        .setValue("./" + uriBasename(executorUri))
        .build();
  }
}
