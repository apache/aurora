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
package org.apache.aurora.scheduler.configuration;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.UserProvidedStrings;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.durability.ThriftBackfill;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IContainer;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IMesosContainer;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskConstraint;
import org.apache.aurora.scheduler.storage.entities.IValueConstraint;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.GPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Manages translation from a string-mapped configuration to a concrete configuration type, and
 * defaults for optional values.
 *
 * TODO(William Farner): Add input validation to all fields (strings not empty, positive ints, etc).
 */
public class ConfigurationManager {

  public static final String DEDICATED_ATTRIBUTE = "dedicated";
  public static final String DEFAULT_ALLOWED_JOB_ENVIRONMENTS = "^(prod|devel|test|staging\\d*)$";

  private interface Validator<T> {
    void validate(T value) throws TaskDescriptionException;
  }

  private static class GreaterThan implements Validator<Number> {
    private final double min;
    private final String label;

    GreaterThan(double min, String label) {
      this.min = min;
      this.label = label;
    }

    @Override
    public void validate(Number value) throws TaskDescriptionException {
      if (this.min >= value.doubleValue()) {
        throw new TaskDescriptionException(label + " must be greater than " + this.min);
      }
    }
  }

  public static class ConfigurationManagerSettings {
    private final ImmutableSet<Container._Fields> allowedContainerTypes;
    private final boolean allowDockerParameters;
    private final List<DockerParameter> defaultDockerParameters;
    private final boolean requireDockerUseExecutor;
    private final boolean allowGpuResource;
    private final boolean enableMesosFetcher;
    private final boolean allowContainerVolumes;
    private final Pattern allowedJobEnvironments;

    public ConfigurationManagerSettings(
        ImmutableSet<Container._Fields> allowedContainerTypes,
        boolean allowDockerParameters,
        List<DockerParameter> defaultDockerParameters,
        boolean requireDockerUseExecutor,
        boolean allowGpuResource,
        boolean enableMesosFetcher,
        boolean allowContainerVolumes,
        String allowedJobEnvironment) {

      this.allowedContainerTypes = requireNonNull(allowedContainerTypes);
      this.allowDockerParameters = allowDockerParameters;
      this.defaultDockerParameters = requireNonNull(defaultDockerParameters);
      this.requireDockerUseExecutor = requireDockerUseExecutor;
      this.allowGpuResource = allowGpuResource;
      this.enableMesosFetcher = enableMesosFetcher;
      this.allowContainerVolumes = allowContainerVolumes;
      this.allowedJobEnvironments = Pattern.compile(requireNonNull(allowedJobEnvironment));
    }
  }

  private final ConfigurationManagerSettings settings;
  private final TierManager tierManager;
  private final ThriftBackfill thriftBackfill;
  private final ExecutorSettings executorSettings;

  @Inject
  public ConfigurationManager(
      ConfigurationManagerSettings settings,
      TierManager tierManager,
      ThriftBackfill thriftBackfill,
      ExecutorSettings executorSettings) {

    this.settings = requireNonNull(settings);
    this.tierManager = requireNonNull(tierManager);
    this.thriftBackfill = requireNonNull(thriftBackfill);
    this.executorSettings = requireNonNull(executorSettings);
  }

  private static String getRole(IValueConstraint constraint) {
    return Iterables.getOnlyElement(constraint.getValues()).split("/")[0];
  }

  private static boolean isValueConstraint(ITaskConstraint taskConstraint) {
    return taskConstraint.getSetField() == TaskConstraint._Fields.VALUE;
  }

  public static boolean isDedicated(Iterable<IConstraint> taskConstraints) {
    return Iterables.any(taskConstraints, getConstraintByName(DEDICATED_ATTRIBUTE));
  }

  @Nullable
  private static IConstraint getDedicatedConstraint(ITaskConfig task) {
    return Iterables.find(task.getConstraints(), getConstraintByName(DEDICATED_ATTRIBUTE), null);
  }

  /**
   * Check validity of and populates defaults in a job configuration.  This will return a deep copy
   * of the provided job configuration with default configuration values applied, and configuration
   * map values sanitized and applied to their respective struct fields.
   *
   * @param job Job to validate and populate.
   * @return A deep copy of {@code job} that has been populated.
   * @throws TaskDescriptionException If the job configuration is invalid.
   */
  public IJobConfiguration validateAndPopulate(IJobConfiguration job)
      throws TaskDescriptionException {

    requireNonNull(job);

    if (!job.isSetTaskConfig()) {
      throw new TaskDescriptionException("Job configuration must have taskConfig set.");
    }

    if (job.getInstanceCount() <= 0) {
      throw new TaskDescriptionException("Instance count must be positive.");
    }

    JobConfiguration builder = job.newBuilder();

    if (!JobKeys.isValid(job.getKey())) {
      throw new TaskDescriptionException("Job key " + job.getKey() + " is invalid.");
    }

    if (!settings.allowedJobEnvironments.matcher(job.getKey().getEnvironment()).matches()) {
      throw new TaskDescriptionException(String.format(
              "Job environment %s doesn't match: %s", job.getKey().getEnvironment(),
              settings.allowedJobEnvironments.toString()));
    }

    if (job.isSetOwner() && !UserProvidedStrings.isGoodIdentifier(job.getOwner().getUser())) {
      throw new TaskDescriptionException(
          "Job user contains illegal characters: " + job.getOwner().getUser());
    }

    builder.setTaskConfig(
        validateAndPopulate(ITaskConfig.build(builder.getTaskConfig())).newBuilder());

    // Only one of [service=true, cron_schedule] may be set.
    if (!Strings.isNullOrEmpty(job.getCronSchedule()) && builder.getTaskConfig().isIsService()) {
      throw new TaskDescriptionException(
          "A service task may not be run on a cron schedule: " + builder);
    }

    return IJobConfiguration.build(builder);
  }

  @VisibleForTesting
  static final String NO_DOCKER_PARAMETERS =
      "This scheduler is configured to disallow Docker parameters.";

  @VisibleForTesting
  static final String EXECUTOR_REQUIRED_WITH_DOCKER =
      "This scheduler is configured to require an executor for Docker-based tasks.";

  @VisibleForTesting
  static final String MESOS_FETCHER_DISABLED =
      "Mesos Fetcher for individual jobs is disabled in this cluster.";

  @VisibleForTesting
  public static final String NO_EXECUTOR_OR_CONTAINER = "Configuration may not be null.";

  @VisibleForTesting
  static final String INVALID_EXECUTOR_CONFIG = "Executor name may not be left unset.";

  @VisibleForTesting
  static final String NO_CONTAINER_VOLUMES =
      "This scheduler is configured to disallow container volumes.";

  /**
   * Check validity of and populates defaults in a task configuration.  This will return a deep copy
   * of the provided task configuration with default configuration values applied, and configuration
   * map values sanitized and applied to their respective struct fields.
   *
   *
   * @param config Task config to validate and populate.
   * @return A reference to the modified {@code config} (for chaining).
   * @throws TaskDescriptionException If the task is invalid.
   */
  public ITaskConfig validateAndPopulate(ITaskConfig config) throws TaskDescriptionException {
    TaskConfig builder = config.newBuilder();

    if (config.isSetTier() && !UserProvidedStrings.isGoodIdentifier(config.getTier())) {
      throw new TaskDescriptionException("Tier contains illegal characters: " + config.getTier());
    }

    try {
      tierManager.getTier(config);
    } catch (IllegalArgumentException e) {
      throw new TaskDescriptionException(e.getMessage(), e);
    }

    if (!JobKeys.isValid(config.getJob())) {
      // Job key is set but invalid
      throw new TaskDescriptionException("Job key " + config.getJob() + " is invalid.");
    }

    // A task must either have an executor configuration or specify a Docker container.
    if (!builder.isSetExecutorConfig()
        && !(builder.isSetContainer() && builder.getContainer().isSetDocker())) {

      throw new TaskDescriptionException(NO_EXECUTOR_OR_CONTAINER);
    }

    // Docker containers don't require executors, validate the rest
    if (builder.isSetExecutorConfig()) {

      if (!builder.getExecutorConfig().isSetName())  {
        throw new TaskDescriptionException(INVALID_EXECUTOR_CONFIG);
      }

      executorSettings.getExecutorConfig(builder.getExecutorConfig().getName()).orElseThrow(
          () -> new TaskDescriptionException("Configuration for executor '"
              + builder.getExecutorConfig().getName()
              + "' doesn't exist."));
    }

    IConstraint constraint = getDedicatedConstraint(config);
    if (constraint != null) {
      if (!isValueConstraint(constraint.getConstraint())) {
        throw new TaskDescriptionException("A dedicated constraint must be of value type.");
      }

      IValueConstraint valueConstraint = constraint.getConstraint().getValue();

      if (valueConstraint.getValues().size() != 1) {
        throw new TaskDescriptionException("A dedicated constraint must have exactly one value");
      }

      String dedicatedRole = getRole(valueConstraint);
      if (!("*".equals(dedicatedRole) || config.getJob().getRole().equals(dedicatedRole))) {
        throw new TaskDescriptionException(
            "Only " + dedicatedRole + " may use hosts dedicated for that role.");
      }
    }

    Optional<Container._Fields> containerType;
    if (config.isSetContainer()) {
      IContainer containerConfig = config.getContainer();
      containerType = Optional.of(containerConfig.getSetField());
      if (containerConfig.isSetDocker()) {
        if (!containerConfig.getDocker().isSetImage()) {
          throw new TaskDescriptionException("A container must specify an image.");
        }
        if (containerConfig.getDocker().getParameters().isEmpty()) {
          builder.getContainer().getDocker()
              .setParameters(ImmutableList.copyOf(settings.defaultDockerParameters));
        } else {
          if (!settings.allowDockerParameters) {
            throw new TaskDescriptionException(NO_DOCKER_PARAMETERS);
          }
        }

        if (settings.requireDockerUseExecutor && !config.isSetExecutorConfig()) {
          throw new TaskDescriptionException(EXECUTOR_REQUIRED_WITH_DOCKER);
        }
      }
    } else {
      // Default to mesos container type if unset.
      containerType = Optional.of(Container._Fields.MESOS);
    }

    if (!containerType.isPresent()) {
      throw new TaskDescriptionException("A job must have a container type.");
    }
    if (!settings.allowedContainerTypes.contains(containerType.get())) {
      throw new TaskDescriptionException(
          "This scheduler is not configured to allow the container type "
              + containerType.get().toString());
    }

    thriftBackfill.backfillTask(builder);

    String types = config.getResources().stream()
        .collect(Collectors.groupingBy(e -> ResourceType.fromResource(e)))
        .entrySet().stream()
            .filter(e -> !e.getKey().isMultipleAllowed() && e.getValue().size() > 1)
            .map(r -> r.getKey().getAuroraName())
            .sorted()
            .collect(Collectors.joining(", "));

    if (!Strings.isNullOrEmpty(types)) {
      throw new TaskDescriptionException("Multiple resource values are not supported for " + types);
    }

    Validator<Number> cpuvalidator = new GreaterThan(0.0, "num_cpus");
    cpuvalidator.validate(
            ResourceManager.quantityOf(ResourceManager.getTaskResources(config, CPUS)));
    Validator<Number> ramvalidator = new GreaterThan(0.0, "ram_mb");
    ramvalidator.validate(
            ResourceManager.quantityOf(ResourceManager.getTaskResources(config, RAM_MB)));
    Validator<Number> diskvalidator = new GreaterThan(0.0, "disk_mb");
    diskvalidator.validate(
            ResourceManager.quantityOf(ResourceManager.getTaskResources(config, DISK_MB)));

    if (!settings.allowGpuResource && config.getResources().stream()
        .filter(r -> ResourceType.fromResource(r).equals(GPUS))
        .findAny()
        .isPresent()) {

      throw new TaskDescriptionException("GPU resource support is disabled in this cluster.");
    }

    if (!settings.enableMesosFetcher && !config.getMesosFetcherUris().isEmpty()) {
      throw new TaskDescriptionException(MESOS_FETCHER_DISABLED);
    }

    if (config.getContainer().isSetMesos()) {
      IMesosContainer container = config.getContainer().getMesos();
      if (!settings.allowContainerVolumes && !container.getVolumes().isEmpty()) {
        throw new TaskDescriptionException(NO_CONTAINER_VOLUMES);
      }
    }

    maybeFillLinks(builder);

    return ITaskConfig.build(builder);
  }

  /**
   * Provides a filter for the given constraint name.
   *
   * @param name The name of the constraint.
   * @return A filter that matches the constraint.
   */
  public static Predicate<IConstraint> getConstraintByName(final String name) {
    return constraint -> constraint.getName().equals(name);
  }

  private static void maybeFillLinks(TaskConfig task) {
    if (task.getTaskLinksSize() == 0) {
      ImmutableMap.Builder<String, String> links = ImmutableMap.builder();
      for (IResource resource : ResourceManager.getTaskResources(ITaskConfig.build(task), PORTS)) {
        if (resource.getNamedPort().equals("health")) {
          links.put("health", "http://%host%:%port:health%");
        } else if (resource.getNamedPort().equals("http")) {
          links.put("http", "http://%host%:%port:http%");
        }
      }
      task.setTaskLinks(links.build());
    }
  }

  /**
   * Thrown when an invalid task or job configuration is encountered.
   */
  public static class TaskDescriptionException extends Exception {
    public TaskDescriptionException(String msg, Exception e) {
      super(msg, e);
    }

    public TaskDescriptionException(String msg) {
      super(msg);
    }
  }
}
