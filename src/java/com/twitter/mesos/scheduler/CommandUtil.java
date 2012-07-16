package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.Environment.Variable;

import com.twitter.common.base.MorePreconditions;

/**
 * Utility class for constructing {@link CommandInfo} objects given an executor URI.
 */
public final class CommandUtil {

  private static final Function<Entry<String, String>, Variable> ENV_ENTRY_TO_VARIABLE =
      new Function<Entry<String, String>, Variable>() {
        @Override public Variable apply(Entry<String, String> entry) {
          return Variable.newBuilder().setName(entry.getKey()).setValue(entry.getValue()).build();
        }
      };

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
   * @param env Environment to set for the executor before launching - may be empty.
   * @return A command that will fetch and execute the executor.
   */
  public static CommandInfo create(String executorUri, Map<String, String> env) {
    MorePreconditions.checkNotBlank(executorUri);

    return CommandInfo.newBuilder()
        .addUris(URI.newBuilder().setValue(executorUri).setExecutable(true))
        .setValue("./" + uriBasename(executorUri))
        .setEnvironment(
            Environment.newBuilder().addAllVariables(
                Iterables.transform(env.entrySet(), ENV_ENTRY_TO_VARIABLE)))
        .build();
  }
}
