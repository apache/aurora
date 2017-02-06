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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.configuration.executor.ExecutorConfig;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.configuration.executor.Executors;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.mesos.v1.Protos.CommandInfo;
import org.apache.mesos.v1.Protos.CommandInfo.URI;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.Resource;
import org.apache.mesos.v1.Protos.Value.Scalar;
import org.apache.mesos.v1.Protos.Value.Type;

/**
 * Test utility class for executor fields.
 */
public final class TestExecutorSettings {
  private TestExecutorSettings() {
    // Utility class.
  }

  public static final ExecutorInfo THERMOS_EXECUTOR_INFO = ExecutorInfo.newBuilder()
      .setName(apiConstants.AURORA_EXECUTOR_NAME)
      .setExecutorId(Executors.PLACEHOLDER_EXECUTOR_ID)
      .setCommand(CommandInfo.newBuilder().setValue("thermos_executor.pex")
          .addAllArguments(ImmutableList.of(
              "--announcer-ensemble",
              "localhost:2181"))
          .addAllUris(ImmutableList.of(
              URI.newBuilder()
                  .setValue("/home/vagrant/aurora/dist/thermos_executor.pex")
                  .setExecutable(true)
                  .setExtract(false)
                  .setCache(false).build())))
      .addAllResources(ImmutableList.of(
          Resource.newBuilder()
              .setName(ResourceType.CPUS.getMesosName())
              .setType(Type.SCALAR)
              .setScalar(Scalar.newBuilder().setValue(0.25))
              .build(),
          Resource.newBuilder()
              .setName(ResourceType.RAM_MB.getMesosName())
              .setType(Type.SCALAR)
              .setScalar(Scalar.newBuilder().setValue(128))
              .build()
      ))
      .build();

  public static final String THERMOS_TASK_PREFIX = "thermos-";

  public static final ExecutorConfig THERMOS_CONFIG =
      new ExecutorConfig(THERMOS_EXECUTOR_INFO, ImmutableList.of(), THERMOS_TASK_PREFIX);

  public static final ExecutorSettings THERMOS_EXECUTOR = new ExecutorSettings(
      ImmutableMap.<String, ExecutorConfig>builder()
          .put(apiConstants.AURORA_EXECUTOR_NAME, THERMOS_CONFIG)
          .build(),
      false /* populate discovery info */);

  public static ExecutorSettings thermosOnlyWithOverhead(Iterable<Resource> resources) {
    ExecutorConfig config =
        THERMOS_EXECUTOR.getExecutorConfig(THERMOS_EXECUTOR_INFO.getName()).get();

    ExecutorInfo.Builder executor = config.getExecutor().toBuilder();
    executor.clearResources().addAllResources(resources);
    return new ExecutorSettings(
        ImmutableMap.<String, ExecutorConfig>builder().put(
            apiConstants.AURORA_EXECUTOR_NAME,
            new ExecutorConfig(
                executor.build(),
                config.getVolumeMounts(),
                THERMOS_TASK_PREFIX)).build(),
        false /* populate discovery info */);
  }
}
