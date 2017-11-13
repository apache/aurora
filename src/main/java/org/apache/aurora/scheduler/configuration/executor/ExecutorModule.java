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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.config.types.DataAmount;
import org.apache.aurora.scheduler.config.validators.ReadableFile;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.CommandInfo;
import org.apache.mesos.v1.Protos.CommandInfo.URI;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.Resource;
import org.apache.mesos.v1.Protos.Value.Scalar;
import org.apache.mesos.v1.Protos.Value.Type;

import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Binding module for {@link ExecutorSettings}.
 */
public class ExecutorModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(
        names = "-custom_executor_config",
        validateValueWith = ReadableFile.class,
        description = "Path to custom executor settings configuration file.")
    public File customExecutorConfig;

    @Parameter(names = "-thermos_executor_path",
        description = "Path to the thermos executor entry point.")
    public String thermosExecutorPath;

    @Parameter(names = "-thermos_executor_resources",
        description = "A comma separated list of additional resources to copy into the sandbox."
            + "Note: if thermos_executor_path is not the thermos_executor.pex file itself, "
            + "this must include it.")
    public List<String> thermosExecutorResources = ImmutableList.of();

    @Parameter(names = "-thermos_executor_flags",
        description = "Extra arguments to be passed to the thermos executor")
    public String thermosExecutorFlags;

    @Parameter(names = "-thermos_home_in_sandbox",
        description = "If true, changes HOME to the sandbox before running the executor. "
            + "This primarily has the effect of causing the executor and runner "
            + "to extract themselves into the sandbox.",
        arity = 1)
    public boolean thermosHomeInSandbox = false;

    /**
     * Extra CPU allocated for each executor.
     */
    @Parameter(names = "-thermos_executor_cpu",
        description = "The number of CPU cores to allocate for each instance of the executor.")
    public double executorOverheadCpus = 0.25;

    /**
     * Extra RAM allocated for the executor.
     */
    @Parameter(names = "-thermos_executor_ram",
        description = "The amount of RAM to allocate for each instance of the executor.")
    public DataAmount executorOverheadRam = new DataAmount(128, Data.MB);

    @Parameter(names = "-global_container_mounts",
        description = "A comma separated list of mount points (in host:container form) to mount "
            + "into all (non-mesos) containers.")
    public List<Volume> globalContainerMounts = ImmutableList.of();

    @Parameter(names = "-populate_discovery_info",
        description = "If true, Aurora populates DiscoveryInfo field of Mesos TaskInfo.",
        arity = 1)
    public boolean populateDiscoveryInfo = false;
  }

  private final Options options;

  public ExecutorModule(Options options) {
    this.options = options;
  }

  @VisibleForTesting
  static CommandInfo makeExecutorCommand(
      String thermosExecutorPath,
      List<String> thermosExecutorResources,
      boolean thermosHomeInSandbox,
      String thermosExecutorFlags) {

    Stream<String> resourcesToFetch = Stream.concat(
        ImmutableList.of(thermosExecutorPath).stream(),
        thermosExecutorResources.stream());

    StringBuilder sb = new StringBuilder();
    if (thermosHomeInSandbox) {
      sb.append("HOME=${MESOS_SANDBOX=.} ");
    }
    // Default to the value of $MESOS_SANDBOX if present.  This is necessary for docker tasks,
    // in which case the mesos agent is responsible for setting $MESOS_SANDBOX.
    sb.append("${MESOS_SANDBOX=.}/");
    sb.append(uriBasename(thermosExecutorPath));
    sb.append(" ");
    sb.append(Optional.ofNullable(thermosExecutorFlags).orElse(""));

    return CommandInfo.newBuilder()
        .setValue(sb.toString().trim())
        .addAllUris(resourcesToFetch
            .map(r -> URI.newBuilder().setValue(r).setExecutable(true).build())
            .collect(GuavaUtils.toImmutableList()))
        .build();
  }

  private static ExecutorConfig makeThermosExecutorConfig(Options opts)  {
    List<Protos.Volume> volumeMounts =
        ImmutableList.<Protos.Volume>builder()
            .addAll(Iterables.transform(
                opts.globalContainerMounts,
                v -> Protos.Volume.newBuilder()
                    .setHostPath(v.getHostPath())
                    .setContainerPath(v.getContainerPath())
                    .setMode(Protos.Volume.Mode.forNumber(v.getMode().getValue()))
                    .build()))
            .build();

    return new ExecutorConfig(
        ExecutorInfo.newBuilder()
            .setName(apiConstants.AURORA_EXECUTOR_NAME)
            // Necessary as executorId is a required field.
            .setExecutorId(Executors.PLACEHOLDER_EXECUTOR_ID)
            .setCommand(
                makeExecutorCommand(
                    opts.thermosExecutorPath,
                    opts.thermosExecutorResources,
                    opts.thermosHomeInSandbox,
                    opts.thermosExecutorFlags))
            .addResources(makeResource(CPUS, opts.executorOverheadCpus))
            .addResources(makeResource(RAM_MB, opts.executorOverheadRam.as(Data.MB)))
            .build(),
        volumeMounts,
        "thermos-");
  }

  private static ExecutorSettings makeExecutorSettings(Options opts) {
    try {
      ImmutableMap.Builder<String, ExecutorConfig> configsBuilder = ImmutableMap.builder();

      configsBuilder.put(apiConstants.AURORA_EXECUTOR_NAME, makeThermosExecutorConfig(opts));

      if (opts.customExecutorConfig != null) {
        configsBuilder.putAll(
            ExecutorSettingsLoader.read(
                Files.newBufferedReader(
                    opts.customExecutorConfig.toPath(),
                    StandardCharsets.UTF_8)));
      }

      return new ExecutorSettings(configsBuilder.build(), opts.populateDiscoveryInfo);

    } catch (ExecutorSettingsLoader.ExecutorConfigException | IOException e) {
      throw new IllegalArgumentException("Failed to read executor settings: " + e, e);
    }
  }

  @Override
  protected void configure() {
    bind(ExecutorSettings.class).toInstance(makeExecutorSettings(options));
  }

  private static Resource makeResource(ResourceType type, double value) {
    return Resource.newBuilder()
        .setType(Type.SCALAR)
        .setName(type.getMesosName())
        .setScalar(Scalar.newBuilder().setValue(value))
        .build();
  }

  private static String uriBasename(String uri) {
    int lastSlash = uri.lastIndexOf('/');
    if (lastSlash == -1) {
      return uri;
    } else {
      String basename = uri.substring(lastSlash + 1);
      MorePreconditions.checkNotBlank(basename, "URI must not end with a slash.");

      return basename;
    }
  }
}
