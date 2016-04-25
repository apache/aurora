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
package org.apache.aurora.scheduler.storage.db;

import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import com.google.common.collect.Maps;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.db.views.DbTaskConfig;
import org.apache.aurora.scheduler.storage.db.views.Pairs;
import org.apache.aurora.scheduler.storage.entities.IAppcImage;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IDockerContainer;
import org.apache.aurora.scheduler.storage.entities.IDockerImage;
import org.apache.aurora.scheduler.storage.entities.IImage;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.IValueConstraint;

import static java.util.Objects.requireNonNull;

class TaskConfigManager {
  private final TaskConfigMapper configMapper;
  private final JobKeyMapper jobKeyMapper;

  @Inject
  TaskConfigManager(TaskConfigMapper configMapper, JobKeyMapper jobKeyMapper) {
    this.configMapper = requireNonNull(configMapper);
    this.jobKeyMapper = requireNonNull(jobKeyMapper);
  }

  private Optional<Long> getConfigRow(ITaskConfig config) {
    // NOTE: The 'config' object passed in MUST have all version-relevant fields populated in order
    // to correctly compare with objects loaded from DB. This may not hold true if a 'config' is
    // passed from storage recovery routine during version downgrade and fields are not properly
    // backfilled. See AURORA-1603 for more details.

    // We could optimize this slightly by first comparing the un-hydrated row and breaking early.
    Map<ITaskConfig, DbTaskConfig> rowsByConfig =
        Maps.uniqueIndex(
            configMapper.selectConfigsByJob(config.getJob()),
            DbTaskConfig::toImmutable);

    return Optional.ofNullable(rowsByConfig.get(config)).map(DbTaskConfig::getRowId);
  }

  long insert(ITaskConfig config) {
    InsertResult configInsert = new InsertResult();

    // Determine whether this config is already stored.
    Optional<Long> existingRow = getConfigRow(config);
    if (existingRow.isPresent()) {
      return existingRow.get();
    }

    jobKeyMapper.merge(config.getJob());
    configMapper.insert(config, configInsert);
    for (IConstraint constraint : config.getConstraints()) {
      InsertResult constraintResult = new InsertResult();
      configMapper.insertConstraint(configInsert.getId(), constraint, constraintResult);
      switch (constraint.getConstraint().getSetField()) {
        case VALUE:
          IValueConstraint valueConstraint = constraint.getConstraint().getValue();
          InsertResult valueResult = new InsertResult();
          configMapper.insertValueConstraint(
              constraintResult.getId(),
              valueConstraint,
              valueResult);
          configMapper.insertValueConstraintValues(
              valueResult.getId(),
              valueConstraint.getValues());
          break;

        case LIMIT:
          configMapper.insertLimitConstraint(
              constraintResult.getId(),
              constraint.getConstraint().getLimit());
          break;

        default:
          throw new IllegalStateException(
              "Unhandled constraint type " + constraint.getConstraint().getSetField());
      }
    }

    if (!config.getResources().isEmpty()) {
      configMapper.insertResources(
          configInsert.getId(),
          config.getResources().stream()
              .map(e -> Pair.of(
                  ResourceType.fromResource(e).getValue(),
                  ResourceType.fromResource(e).getTypeConverter().stringify(e.getRawValue())))
              .collect(GuavaUtils.toImmutableList()));
    }

    if (!config.getRequestedPorts().isEmpty()) {
      configMapper.insertRequestedPorts(configInsert.getId(), config.getRequestedPorts());
    }

    if (!config.getTaskLinks().isEmpty()) {
      configMapper.insertTaskLinks(
          configInsert.getId(),
          Pairs.fromMap(config.getTaskLinks()));
    }

    if (!config.getMetadata().isEmpty()) {
      configMapper.insertMetadata(configInsert.getId(), config.getMetadata());
    }

    if (config.getContainer().isSetDocker()) {
      IDockerContainer container = config.getContainer().getDocker();
      InsertResult containerInsert = new InsertResult();
      configMapper.insertContainer(configInsert.getId(), container, containerInsert);
      if (!container.getParameters().isEmpty()) {
        configMapper.insertDockerParameters(containerInsert.getId(), container.getParameters());
      }
    }

    if (config.isSetImage()) {
      IImage image = config.getImage();

      switch (image.getSetField()) {
        case DOCKER:
          IDockerImage dockerImage = image.getDocker();
          configMapper.insertDockerImage(
              configInsert.getId(),
              dockerImage.getName(),
              dockerImage.getTag());
          break;
        case APPC:
          IAppcImage appcImage = image.getAppc();
          configMapper.insertAppcImage(
              configInsert.getId(),
              appcImage.getName(),
              appcImage.getImageId());
          break;
        default:
          throw new IllegalStateException("Unexpected image type: " + image.getSetField());
      }
    }

    return configInsert.getId();
  }
}
