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
package org.apache.aurora.scheduler.storage.log;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Inject;
import javax.inject.Qualifier;

import org.apache.aurora.scheduler.log.Log;

import static java.util.Objects.requireNonNull;

/**
 * Manages opening, reading from and writing to a {@link Log}.
 */
public class LogManager {
  /**
   * Identifies the maximum log entry size to permit before chunking entries into frames.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.PARAMETER})
  @Qualifier
  public @interface MaxEntrySize { }

  /**
   * Hash function used to verify log entries.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.PARAMETER})
  @Qualifier
  public @interface LogEntryHashFunction { }

  private final Log log;
  private final StreamManagerFactory streamManagerFactory;

  @Inject
  LogManager(
      Log log,
      StreamManagerFactory streamManagerFactory) {

    this.log = requireNonNull(log);
    this.streamManagerFactory = requireNonNull(streamManagerFactory);
  }

  /**
   * Opens the log for reading and writing.
   *
   * @return A stream manager that can be used to manipulate the log stream.
   * @throws IOException If there is a problem opening the log.
   */
  public StreamManager open() throws IOException {
    return streamManagerFactory.create(log.open());
  }

}
