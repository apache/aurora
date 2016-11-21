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
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import org.apache.aurora.Protobufs;
import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.resources.AcceptedOffer;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.storage.entities.IAppcImage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IDockerContainer;
import org.apache.aurora.scheduler.storage.entities.IDockerImage;
import org.apache.aurora.scheduler.storage.entities.IImage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IMesosContainer;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ContainerInfo;
import org.apache.mesos.Protos.DiscoveryInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Labels;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Port;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.apiConstants.TASK_FILESYSTEM_MOUNT_POINT;

/**
 * A factory to create mesos task objects.
 */
public interface MesosTaskFactory {

  /**
   * Creates a mesos task object.
   *
   * @param task Assigned task to translate into a task object.
   * @param offer Resource offer the task is being assigned to.
   * @return A new task.
   * @throws SchedulerException If the task could not be encoded.
   */
  TaskInfo createFrom(IAssignedTask task, Offer offer) throws SchedulerException;

  // TODO(wfarner): Move this class to its own file to reduce visibility to package private.
  class MesosTaskFactoryImpl implements MesosTaskFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MesosTaskFactoryImpl.class);

    private static final String AURORA_LABEL_PREFIX = "org.apache.aurora";

    @VisibleForTesting
    static final String METADATA_LABEL_PREFIX = AURORA_LABEL_PREFIX + ".metadata.";

    @VisibleForTesting
    static final String DEFAULT_PORT_PROTOCOL = "TCP";

    // N.B. We intentionally do not prefix this label. It was added when the explicit source field
    // was removed from the ExecutorInfo proto and named "source" per guidance from Mesos devs.
    @VisibleForTesting
    static final String SOURCE_LABEL = "source";

    @VisibleForTesting
    static final String TIER_LABEL = AURORA_LABEL_PREFIX + ".tier";

    private final ExecutorSettings executorSettings;
    private final TierManager tierManager;
    private final IServerInfo serverInfo;

    @Inject
    MesosTaskFactoryImpl(
        ExecutorSettings executorSettings,
        TierManager tierManager,
        IServerInfo serverInfo) {

      this.executorSettings = requireNonNull(executorSettings);
      this.tierManager = requireNonNull(tierManager);
      this.serverInfo = requireNonNull(serverInfo);
    }

    @VisibleForTesting
    static ExecutorID getExecutorId(String taskId, String taskPrefix) {
      return ExecutorID.newBuilder().setValue(taskPrefix + taskId).build();
    }

    private static String getJobSourceName(IJobKey jobkey) {
      return String.join(".", jobkey.getRole(), jobkey.getEnvironment(), jobkey.getName());
    }

    private static String getJobSourceName(ITaskConfig task) {
      return getJobSourceName(task.getJob());
    }

    private static String getExecutorName(IAssignedTask task) {
      return task.getTask().getExecutorConfig().getName();
    }

    @VisibleForTesting
    static String getInstanceSourceName(ITaskConfig task, int instanceId) {
      return String.join(".", getJobSourceName(task), Integer.toString(instanceId));
    }

    @VisibleForTesting
    static String getInverseJobSourceName(IJobKey job) {
      return String.join(".", job.getName(), job.getEnvironment(), job.getRole());
    }

    private static byte[] serializeTask(IAssignedTask task) throws SchedulerException {
      try {
        return ThriftBinaryCodec.encode(task.newBuilder());
      } catch (ThriftBinaryCodec.CodingException e) {
        LOG.error("Unable to serialize task.", e);
        throw new SchedulerException("Internal error.", e);
      }
    }

    @Override
    public TaskInfo createFrom(IAssignedTask task, Offer offer) throws SchedulerException {
      requireNonNull(task);
      requireNonNull(offer);

      ITaskConfig config = task.getTask();

      // Docker-based tasks don't need executors
      ResourceBag executorOverhead = ResourceBag.EMPTY;
      if (config.isSetExecutorConfig()) {
        executorOverhead =
            executorSettings.getExecutorOverhead(getExecutorName(task)).orElse(ResourceBag.EMPTY);
      }

      AcceptedOffer acceptedOffer;
      // TODO(wfarner): Re-evaluate if/why we need to continue handling unset assignedPorts field.
      try {
        acceptedOffer = AcceptedOffer.create(
            offer,
            task,
            executorOverhead,
            tierManager.getTier(task.getTask()));
      } catch (ResourceManager.InsufficientResourcesException e) {
        throw new SchedulerException(e);
      }
      Iterable<Resource> resources = acceptedOffer.getTaskResources();

      LOG.debug(
          "Setting task resources to {}",
          Iterables.transform(resources, Protobufs::toString));

      TaskInfo.Builder taskBuilder = TaskInfo.newBuilder()
          .setName(JobKeys.canonicalString(Tasks.getJob(task)))
          .setTaskId(TaskID.newBuilder().setValue(task.getTaskId()))
          .setSlaveId(offer.getSlaveId())
          .addAllResources(resources);

      configureTaskLabels(config, taskBuilder);

      if (executorSettings.shouldPopulateDiscoverInfo()) {
        configureDiscoveryInfos(task, taskBuilder);
      }

      if (config.getContainer().isSetMesos()) {
        ExecutorInfo.Builder executorInfoBuilder = configureTaskForExecutor(task, acceptedOffer);

        Optional<ContainerInfo.Builder> containerInfoBuilder = configureTaskForImage(
            task.getTask().getContainer().getMesos(),
            getExecutorName(task));
        if (containerInfoBuilder.isPresent()) {
          executorInfoBuilder.setContainer(containerInfoBuilder.get());
        }

        taskBuilder.setExecutor(executorInfoBuilder.build());
      } else if (config.getContainer().isSetDocker()) {
        IDockerContainer dockerContainer = config.getContainer().getDocker();
        if (config.isSetExecutorConfig()) {
          ExecutorInfo.Builder execBuilder = configureTaskForExecutor(task, acceptedOffer)
              .setContainer(getDockerContainerInfo(
                  dockerContainer,
                  Optional.of(getExecutorName(task))));
          taskBuilder.setExecutor(execBuilder.build());
        } else {
          LOG.warn("Running Docker-based task without an executor.");
          taskBuilder.setContainer(getDockerContainerInfo(dockerContainer, Optional.absent()))
              .setCommand(CommandInfo.newBuilder().setShell(false));
        }
      } else {
        throw new SchedulerException("Task had no supported container set.");
      }

      if (taskBuilder.hasExecutor()) {
        taskBuilder.setData(ByteString.copyFrom(serializeTask(task)));
      }

      return taskBuilder.build();
    }

    private Optional<ContainerInfo.Builder> configureTaskForImage(
        IMesosContainer mesosContainer,
        String executorName) {
      requireNonNull(mesosContainer);

      if (mesosContainer.isSetImage()) {
        IImage image = mesosContainer.getImage();

        Protos.Image.Builder imageBuilder = Protos.Image.newBuilder();

        if (image.isSetAppc()) {
          IAppcImage appcImage = image.getAppc();

          imageBuilder.setType(Protos.Image.Type.APPC);
          imageBuilder.setAppc(Protos.Image.Appc.newBuilder()
              .setName(appcImage.getName())
              .setId(appcImage.getImageId()));
        } else if (image.isSetDocker()) {
          IDockerImage dockerImage = image.getDocker();

          imageBuilder.setType(Protos.Image.Type.DOCKER);
          imageBuilder.setDocker(Protos.Image.Docker.newBuilder()
              .setName(dockerImage.getName() + ":" + dockerImage.getTag()));
        } else {
          throw new SchedulerException("Task had no supported image set.");
        }

        ContainerInfo.MesosInfo.Builder mesosContainerBuilder =
            ContainerInfo.MesosInfo.newBuilder();

        Iterable<Protos.Volume> containerVolumes = Iterables.transform(mesosContainer.getVolumes(),
            input -> Protos.Volume.newBuilder()
            .setMode(Protos.Volume.Mode.valueOf(input.getMode().name()))
            .setHostPath(input.getHostPath())
            .setContainerPath(input.getContainerPath())
            .build());

        Protos.Volume volume = Protos.Volume.newBuilder()
            .setImage(imageBuilder)
            .setContainerPath(TASK_FILESYSTEM_MOUNT_POINT)
            .setMode(Protos.Volume.Mode.RO)
            .build();

        return Optional.of(ContainerInfo.newBuilder()
            .setType(ContainerInfo.Type.MESOS)
            .setMesos(mesosContainerBuilder)
            .addAllVolumes(executorSettings.getExecutorConfig(executorName).get().getVolumeMounts())
            .addAllVolumes(containerVolumes)
            .addVolumes(volume));
      }

      return Optional.absent();
    }

    private ContainerInfo getDockerContainerInfo(
        IDockerContainer config,
        Optional<String> executorName) {

      Iterable<Protos.Parameter> parameters = Iterables.transform(config.getParameters(),
          item -> Protos.Parameter.newBuilder().setKey(item.getName())
            .setValue(item.getValue()).build());

      ContainerInfo.DockerInfo.Builder dockerBuilder = ContainerInfo.DockerInfo.newBuilder()
          .setImage(config.getImage()).addAllParameters(parameters);
      return ContainerInfo.newBuilder()
          .setType(ContainerInfo.Type.DOCKER)
          .setDocker(dockerBuilder.build())
          .addAllVolumes(
              executorName.isPresent()
                  ? executorSettings.getExecutorConfig(executorName.get()).get().getVolumeMounts()
                  : ImmutableList.of())
          .build();
    }

    @SuppressWarnings("deprecation") // we set the source field for backwards compat.
    private ExecutorInfo.Builder configureTaskForExecutor(
        IAssignedTask task,
        AcceptedOffer acceptedOffer) {

      String sourceName = getInstanceSourceName(task.getTask(), task.getInstanceId());

      ExecutorInfo.Builder builder =
          executorSettings.getExecutorConfig(getExecutorName(task)).get()
          .getExecutor()
          .toBuilder()
          .setExecutorId(getExecutorId(
              task.getTaskId(),
              executorSettings.getExecutorConfig(getExecutorName(task)).get().getTaskPrefix()))
          .setSource(sourceName)
          .setLabels(
              Labels.newBuilder().addLabels(
                  Label.newBuilder()
                      .setKey(SOURCE_LABEL)
                      .setValue(sourceName)));

      //TODO: (rdelvalle) add output_file when Aurora's Mesos dep is updated (MESOS-4735)
      List<CommandInfo.URI> mesosFetcherUris = task.getTask().getMesosFetcherUris().stream()
          .map(u -> Protos.CommandInfo.URI.newBuilder().setValue(u.getValue())
              .setExecutable(false)
              .setExtract(u.isExtract())
              .setCache(u.isCache()).build())
          .collect(Collectors.toList());

      builder.setCommand(builder.getCommand().toBuilder().addAllUris(mesosFetcherUris));

      Iterable<Resource> executorResources = acceptedOffer.getExecutorResources();
      LOG.debug(
          "Setting executor resources to {}",
          Iterables.transform(executorResources, Protobufs::toString));
      builder.clearResources().addAllResources(executorResources);
      return builder;
    }

    private void configureTaskLabels(ITaskConfig config, TaskInfo.Builder taskBuilder) {
      Labels.Builder labelsBuilder = Labels.newBuilder();
      labelsBuilder.addLabels(Label.newBuilder().setKey(TIER_LABEL).setValue(config.getTier()));

      config.getMetadata().stream().forEach(m -> labelsBuilder.addLabels(Label.newBuilder()
          .setKey(METADATA_LABEL_PREFIX + m.getKey())
          .setValue(m.getValue())
          .build()));

      taskBuilder.setLabels(labelsBuilder);
    }

    private void configureDiscoveryInfos(IAssignedTask task, TaskInfo.Builder taskBuilder) {
      DiscoveryInfo.Builder builder = taskBuilder.getDiscoveryBuilder();
      builder.setVisibility(DiscoveryInfo.Visibility.CLUSTER);
      builder.setName(getInverseJobSourceName(task.getTask().getJob()));
      builder.setEnvironment(task.getTask().getJob().getEnvironment());
      // A good sane choice for default location is current Aurora cluster name.
      builder.setLocation(serverInfo.getClusterName());
      for (Map.Entry<String, Integer> entry : task.getAssignedPorts().entrySet()) {
        builder.getPortsBuilder().addPorts(
            Port.newBuilder()
                .setName(entry.getKey())
                .setNumber(entry.getValue())
                .setProtocol(DEFAULT_PORT_PROTOCOL)
        );
      }
    }

  }
}
