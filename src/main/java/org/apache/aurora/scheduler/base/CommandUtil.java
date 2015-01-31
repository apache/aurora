/**
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
package org.apache.aurora.scheduler.base;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.twitter.common.base.MorePreconditions;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;

/**
 * Utility class for constructing {@link CommandInfo} objects given an executor URI.
 */
public final class CommandUtil {

  private static final Function<String, URI> STRING_TO_URI_RESOURCE = new Function<String, URI>() {
    @Override
    public URI apply(String resource) {
      return URI.newBuilder().setValue(resource).setExecutable(true).build();
    }
  };

  private CommandUtil() {
    // Utility class.
  }

  /**
   * Gets the last part of the path of a URI.
   *
   * @param uri URI to parse
   * @return The last segment of the URI.
   */
  public static String uriBasename(String uri) {
    int lastSlash = uri.lastIndexOf('/');
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
   * @param executorUri A URI to the executor
   * @param executorResources A list of URIs to be fetched into the sandbox with the executor.
   * @return A populated CommandInfo with correct resources set and command set.
   */
  public static CommandInfo create(String executorUri, List<String> executorResources) {
    return create(
        executorUri,
        executorResources,
        "./",
        Optional.<String>absent()).build();
  }

  /**
   * Creates a description of a command that will fetch and execute the given URI to an executor
   * binary.
   *
   * @param executorUri A URI to the executor
   * @param executorResources A list of URIs to be fetched into the sandbox with the executor.
   * @param commandBasePath The relative base path of the executor.
   * @param extraArguments Extra command line arguments to add to the generated command.
   * @return A CommandInfo.Builder populated with resources and a command.
   */
  public static CommandInfo.Builder create(
      String executorUri,
      List<String> executorResources,
      String commandBasePath,
      Optional<String> extraArguments) {

    Preconditions.checkNotNull(executorResources);
    MorePreconditions.checkNotBlank(executorUri);
    MorePreconditions.checkNotBlank(commandBasePath);
    CommandInfo.Builder builder = CommandInfo.newBuilder();

    builder.addAllUris(Iterables.transform(executorResources, STRING_TO_URI_RESOURCE));
    builder.addUris(STRING_TO_URI_RESOURCE.apply(executorUri));

    String cmdLine = commandBasePath
        + uriBasename(executorUri)
        + " " + extraArguments.or("");
    return builder.setValue(cmdLine.trim());
  }
}
