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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.CanRead;
import org.apache.aurora.common.args.constraints.Exists;
import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.ResourceType;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;

import static org.apache.aurora.scheduler.ResourceType.CPUS;
import static org.apache.aurora.scheduler.ResourceType.RAM_MB;

/**
 * Binding module for {@link ExecutorSettings}.
 */
public class ExecutorModule extends AbstractModule {

  @CmdLine(
      name = "custom_executor_config",
      help = "Path to custom executor settings configuration file.")
  @Exists
  @CanRead
  private static final Arg<File> CUSTOM_EXECUTOR_CONFIG = Arg.create(null);

  @CmdLine(name = "thermos_executor_path", help = "Path to the thermos executor entry point.")
  private static final Arg<String> THERMOS_EXECUTOR_PATH = Arg.create();

  @CmdLine(name = "thermos_executor_resources",
      help = "A comma seperated list of additional resources to copy into the sandbox."
          + "Note: if thermos_executor_path is not the thermos_executor.pex file itself, "
          + "this must include it.")
  private static final Arg<List<String>> THERMOS_EXECUTOR_RESOURCES =
      Arg.create(ImmutableList.of());

  @CmdLine(name = "thermos_executor_flags",
      help = "Extra arguments to be passed to the thermos executor")
  private static final Arg<String> THERMOS_EXECUTOR_FLAGS = Arg.create(null);

  @CmdLine(name = "thermos_observer_root",
      help = "Path to the thermos observer root (by default /var/run/thermos.)")
  private static final Arg<String> THERMOS_OBSERVER_ROOT = Arg.create("/var/run/thermos");

  /**
   * Extra CPU allocated for each executor.
   */
  @CmdLine(name = "thermos_executor_cpu",
      help = "The number of CPU cores to allocate for each instance of the executor.")
  private static final Arg<Double> EXECUTOR_OVERHEAD_CPUS = Arg.create(0.25);

  /**
   * Extra RAM allocated for the executor.
   */
  @CmdLine(name = "thermos_executor_ram",
      help = "The amount of RAM to allocate for each instance of the executor.")
  private static final Arg<Amount<Long, Data>> EXECUTOR_OVERHEAD_RAM =
      Arg.create(Amount.of(128L, Data.MB));

  @CmdLine(name = "global_container_mounts",
      help = "A comma seperated list of mount points (in host:container form) to mount "
          + "into all (non-mesos) containers.")
  private static final Arg<List<Volume>> GLOBAL_CONTAINER_MOUNTS = Arg.create(ImmutableList.of());

  private static CommandInfo makeExecutorCommand() {
    Stream<String> resourcesToFetch = Stream.concat(
        ImmutableList.of(THERMOS_EXECUTOR_PATH.get()).stream(),
        THERMOS_EXECUTOR_RESOURCES.get().stream());

    return CommandInfo.newBuilder()
        // Default to the value of $MESOS_SANDBOX if present.  This is necessary for docker tasks,
        // in which case the mesos agent is responsible for setting $MESOS_SANDBOX.
        .setValue("${MESOS_SANDBOX=.}/" + uriBasename(THERMOS_EXECUTOR_PATH.get())
            + " " + Optional.ofNullable(THERMOS_EXECUTOR_FLAGS.get()).orElse(""))
        .addAllUris(resourcesToFetch
            .map(r -> URI.newBuilder().setValue(r).setExecutable(true).build())
            .collect(GuavaUtils.toImmutableList()))
        .build();
  }

  private static ExecutorSettings makeThermosExecutorSettings()  {
    List<Protos.Volume> volumeMounts =
        ImmutableList.<Protos.Volume>builder()
            .add(Protos.Volume.newBuilder()
                .setHostPath(THERMOS_OBSERVER_ROOT.get())
                .setContainerPath(THERMOS_OBSERVER_ROOT.get())
                .setMode(Protos.Volume.Mode.RW)
                .build())
            .addAll(Iterables.transform(
                GLOBAL_CONTAINER_MOUNTS.get(),
                v -> Protos.Volume.newBuilder()
                    .setHostPath(v.getHostPath())
                    .setContainerPath(v.getContainerPath())
                    .setMode(Protos.Volume.Mode.valueOf(v.getMode().getValue()))
                    .build()))
            .build();

    return new ExecutorSettings(
        new ExecutorConfig(
            ExecutorInfo.newBuilder()
                .setName("aurora.task")
                // Necessary as executorId is a required field.
                .setExecutorId(Executors.PLACEHOLDER_EXECUTOR_ID)
                .setCommand(makeExecutorCommand())
                .addResources(makeResource(CPUS, EXECUTOR_OVERHEAD_CPUS.get()))
                .addResources(makeResource(RAM_MB, EXECUTOR_OVERHEAD_RAM.get().as(Data.MB)))
                .build(),
            volumeMounts));
  }

  private static ExecutorSettings makeCustomExecutorSettings() {
    try {
      return
          new ExecutorSettings(
              ExecutorSettingsLoader.read(
                  Files.newBufferedReader(
                      CUSTOM_EXECUTOR_CONFIG.get().toPath(),
                      StandardCharsets.UTF_8)));
    } catch (ExecutorSettingsLoader.ExecutorConfigException | IOException e) {
      throw new IllegalArgumentException("Failed to read executor settings: " + e, e);
    }
  }

  @Override
  protected void configure() {
    bind(ExecutorSettings.class).toInstance(CUSTOM_EXECUTOR_CONFIG.hasAppliedValue()
        ? makeCustomExecutorSettings()
        : makeThermosExecutorSettings());
  }

  private static Resource makeResource(ResourceType type, double value) {
    return Resource.newBuilder()
        .setType(Type.SCALAR)
        .setName(type.getName())
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
