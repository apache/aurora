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
package com.twitter.aurora.scheduler.base;

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
