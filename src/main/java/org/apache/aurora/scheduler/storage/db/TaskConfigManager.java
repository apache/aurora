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

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.scheduler.storage.db.views.TaskConfigRow;
import org.apache.aurora.scheduler.storage.db.views.TaskLink;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.IValueConstraint;

import static java.util.Objects.requireNonNull;

class TaskConfigManager {
  private final TaskConfigMapper configMapper;

  @Inject
  TaskConfigManager(TaskConfigMapper configMapper) {
    this.configMapper = requireNonNull(configMapper);
  }

  long insert(ITaskConfig config) {
    InsertResult configInsert = new InsertResult();
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

    if (!config.getRequestedPorts().isEmpty()) {
      configMapper.insertRequestedPorts(configInsert.getId(), config.getRequestedPorts());
    }

    if (!config.getTaskLinks().isEmpty()) {
      configMapper.insertTaskLinks(
          configInsert.getId(),
          FluentIterable.from(config.getTaskLinks().entrySet())
              .transform(TO_LINK)
              .toList());
    }

    if (!config.getMetadata().isEmpty()) {
      configMapper.insertMetadata(configInsert.getId(), config.getMetadata());
    }

    // TODO(wfarner): It would be nice if this generalized to different Container types.
    if (config.getContainer().isSetDocker()) {
      configMapper.insertContainer(configInsert.getId(), config.getContainer().getDocker());
    }

    return configInsert.getId();
  }

  Map<String, String> getTaskLinks(long configId) {
    ImmutableMap.Builder<String, String> links = ImmutableMap.builder();
    for (TaskLink link : configMapper.selectTaskLinks(configId)) {
      links.put(link.getLabel(), link.getUrl());
    }
    return links.build();
  }

  List<TaskConfigRow> getConfigs(IJobKey job) {
    requireNonNull(job);
    return configMapper.selectConfigsByJob(job);
  }

  List<Long> getTaskConfigIds(Set<String> scheduledTaskIds) {
    requireNonNull(scheduledTaskIds);
    return configMapper.selectConfigsByTaskId(scheduledTaskIds);
  }

  /**
   * Performs reference counting on configurations.  If there are no longer any references to
   * these configuration rows, they will be deleted.
   * TODO(wfarner): Should we rely on foreign key constraints and opportunistically delete?
   *
   * @param configRowIds Configurations to delete if no references are found.
   */
  void maybeExpungeConfigs(Set<Long> configRowIds) {
    if (configMapper.selectTasksByConfigId(configRowIds).isEmpty()) {
      configMapper.delete(configRowIds);

      // TODO(wfarner): Need to try removal from other tables as well, e.g. job keys.
    }
  }

  private static final Function<Map.Entry<String, String>, TaskLink> TO_LINK =
      new Function<Map.Entry<String, String>, TaskLink>() {
        @Override
        public TaskLink apply(Map.Entry<String, String> entry) {
          return new TaskLink(entry.getKey(), entry.getValue());
        }
      };
}
