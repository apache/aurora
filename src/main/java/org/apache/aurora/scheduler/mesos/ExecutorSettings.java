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
package org.apache.aurora.scheduler.mesos;

import java.util.List;

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.configuration.Resources;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the executor to run, and resource overhead required for it.
 */
public class ExecutorSettings {
  private final String executorPath;
  private final List<String> executorResources;
  private final String thermosObserverRoot;
  private final Optional<String> executorFlags;
  private final Resources executorOverhead;

  public ExecutorSettings(
      String executorPath,
      List<String> executorResources,
      String thermosObserverRoot,
      Optional<String> executorFlags,
      Resources executorOverhead) {

    this.executorPath = requireNonNull(executorPath);
    this.executorResources = requireNonNull(executorResources);
    this.thermosObserverRoot = requireNonNull(thermosObserverRoot);
    this.executorFlags = requireNonNull(executorFlags);
    this.executorOverhead = requireNonNull(executorOverhead);
  }

  public String getExecutorPath() {
    return executorPath;
  }

  public List<String> getExecutorResources() {
    return executorResources;
  }

  public String getThermosObserverRoot() {
    return thermosObserverRoot;
  }

  public Optional<String> getExecutorFlags() {
    return executorFlags;
  }

  public Resources getExecutorOverhead() {
    return executorOverhead;
  }
}
