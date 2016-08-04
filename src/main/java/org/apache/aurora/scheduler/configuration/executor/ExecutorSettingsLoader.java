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

package org.apache.aurora.scheduler.configuration.executor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.protobuf.UninitializedMessageException;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

import org.apache.aurora.GuavaUtils;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Volume;

import static com.fasterxml.jackson.databind.PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES;

/**
 * A utility class to read JSON-formatted executor configurations.
 */
public final class ExecutorSettingsLoader {
  public static final ExecutorID PLACEHOLDER_EXECUTOR_ID = ExecutorID.newBuilder()
      .setValue("PLACEHOLDER")
      .build();

  private ExecutorSettingsLoader()  {
    // Utility class
  }

  /**
   * Thrown when an executor configuration could not be read.
   */
  public static class ExecutorConfigException extends Exception {
    public ExecutorConfigException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Reads an executor configuration from a JSON-encoded source.
   *
   * @param input The configuration data source.
   * @return A map of executor configurations.
   * @throws ExecutorConfigException If the input cannot be read or is not properly formatted.
   */
  public static Map<String, ExecutorConfig> read(Readable input) throws ExecutorConfigException {
    String configContents;
    try {
      configContents = CharStreams.toString(input);
    } catch (IOException e) {
      throw new ExecutorConfigException(e);
    }

    ObjectMapper mapper = new ObjectMapper()
        .registerModule(new ProtobufModule())
        .setPropertyNamingStrategy(CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    List<Schema> parsed;
    try {
      parsed = mapper.readValue(configContents, new TypeReference<List<Schema>>() { });
    } catch (IOException e) {
      throw new ExecutorConfigException(e);
    }

    Map<String, ExecutorConfig> customExecutors;
    try {
      // We apply a placeholder value for the executor ID so that we can construct and validate
      // the protobuf schema.  This allows us to catch many validation errors here rather than
      // later on when launching tasks.
      customExecutors = parsed.stream().collect(
          GuavaUtils.toImmutableMap(
              m -> m.executor.getName(),
              m -> new ExecutorConfig(
                  m.executor.setExecutorId(PLACEHOLDER_EXECUTOR_ID).build(),
                  Optional.fromNullable(m.volumeMounts).or(ImmutableList.of()),
                  m.taskPrefix)));

    } catch (UninitializedMessageException e) {
      throw new ExecutorConfigException(e);
    }

    return customExecutors;
  }

  /**
   * The JSON schema.  This is separated from the public {@link ExecutorConfig} so we can read
   * objects that do not have all fields required by the protobuf set in the JSON config.
   */
  private static class Schema {
    private ExecutorInfo.Builder executor;
    private List<Volume> volumeMounts;
    private String taskPrefix;

    ExecutorInfo.Builder getExecutor() {
      return executor;
    }

    void setExecutor(ExecutorInfo.Builder executor) {
      this.executor = executor;
    }

    List<Volume> getVolumeMounts() {
      return volumeMounts;
    }

    void setVolumeMounts(List<Volume> volumeMounts) {
      this.volumeMounts = volumeMounts;
    }

    String getTaskPrefix() {
      return taskPrefix;
    }

    void setTaskPrefix(String taskPrefix) {
      this.taskPrefix = taskPrefix;
    }
  }
}
