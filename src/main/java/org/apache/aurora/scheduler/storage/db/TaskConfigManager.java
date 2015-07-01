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
import com.google.common.base.Functions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.scheduler.storage.db.views.TaskConfigRow;
import org.apache.aurora.scheduler.storage.db.views.TaskLink;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
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

  /**
   * Replaces the shimmed {@link org.apache.thrift.TUnion} instances with the base thrift types.
   * This is necessary because TUnion, as of thrift 0.9.1, restricts subclassing.  The copy
   * constructor checks for equality on {@link Object#getClass()} rather than the subclass-friendly
   * {@link Class#isInstance(Object).
   */
  private static final Function<TaskConfig, TaskConfig> REPLACE_UNION_TYPES =
      new Function<TaskConfig, TaskConfig>() {
        @Override
        public TaskConfig apply(TaskConfig config) {
          ImmutableSet.Builder<Constraint> constraints = ImmutableSet.builder();
          for (Constraint constraint : config.getConstraints()) {
            Constraint replacement = new Constraint()
                .setName(constraint.getName());
            replacement.setConstraint(
                new TaskConstraint(
                    constraint.getConstraint().getSetField(),
                    constraint.getConstraint().getFieldValue()));
            constraints.add(replacement);
          }
          config.setConstraints(constraints.build());

          config.setContainer(
              new Container(
                  config.getContainer().getSetField(),
                  config.getContainer().getFieldValue()));

          return config;
        }
      };

  /**
   * Creates an instance of a caching function that will fill all relations in a task config.
   *
   * @return A function to populate relations in task configs.
   */
  Function<TaskConfigRow, TaskConfig> getConfigSaturator() {
    // It appears that there is no way in mybatis to populate a field of type Map.  To work around
    // this, we need to manually perform the query and associate the elements.
    final LoadingCache<Long, Map<String, String>> taskLinkCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<Long, Map<String, String>>() {
          @Override
          public Map<String, String> load(Long configId) {
            return getTaskLinks(configId);
          }
        });
    Function<TaskConfigRow, TaskConfig> linkPopulator = new Function<TaskConfigRow, TaskConfig>() {
      @Override
      public TaskConfig apply(TaskConfigRow row) {
        return row.getConfig().setTaskLinks(taskLinkCache.getUnchecked(row.getId()));
      }
    };

    return Functions.compose(REPLACE_UNION_TYPES, linkPopulator);
  }

  long insert(ITaskConfig config) {
    InsertResult configInsert = new InsertResult();
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

  Set<Long> getTaskConfigIds(Set<String> scheduledTaskIds) {
    requireNonNull(scheduledTaskIds);
    return configMapper.selectConfigsByTaskId(scheduledTaskIds);
  }

  private static final Function<Map.Entry<String, String>, TaskLink> TO_LINK =
      new Function<Map.Entry<String, String>, TaskLink>() {
        @Override
        public TaskLink apply(Map.Entry<String, String> entry) {
          return new TaskLink(entry.getKey(), entry.getValue());
        }
      };
}
