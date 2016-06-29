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

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.scheduler.storage.db.views.DbTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IDockerContainer;
import org.apache.aurora.scheduler.storage.entities.IDockerParameter;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ILimitConstraint;
import org.apache.aurora.scheduler.storage.entities.IMesosFetcherURI;
import org.apache.aurora.scheduler.storage.entities.IMetadata;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.IValueConstraint;
import org.apache.ibatis.annotations.Param;

/**
 * MyBatis mapper for task config objects.
 */
interface TaskConfigMapper extends GarbageCollectedTableMapper {

  /**
   * Inserts fields from a task config into the {@code task_configs} table.
   *
   * @param config Configuration to insert.
   * @param result Container for auto-generated ID of the inserted row.
   */
  void insert(
      @Param("config") ITaskConfig config,
      @Param("result") InsertResult result);

  /**
   * Gets all task config rows referenced by a job.
   *
   * @param job Job to look up.
   * @return Task config row container.
   */
  List<DbTaskConfig> selectConfigsByJob(IJobKey job);

  /**
   * Inserts the constraint association within an {@link ITaskConfig}.
   *
   * @param configId Task config ID.
   * @param constraint Constraint to insert.
   * @param result Container for auto-generated ID of the inserted row.
   */
  void insertConstraint(
      @Param("configId") long configId,
      @Param("constraint") IConstraint constraint,
      @Param("result") InsertResult result);

  /**
   * Inserts the limit constraint association within an {@link IConstraint}.
   *
   * @param constraintId Constraint ID.
   * @param constraint Constraint to insert.
   */
  void insertLimitConstraint(
      @Param("constraintId") long constraintId,
      @Param("constraint") ILimitConstraint constraint);

  /**
   * Inserts the value constraint association within an {@link IConstraint}.
   *
   * @param constraintId Constraint ID.
   * @param constraint Constraint to insert.
   * @param result Container for auto-generated ID of the inserted row.
   */
  void insertValueConstraint(
      @Param("constraintId") long constraintId,
      @Param("constraint") IValueConstraint constraint,
      @Param("result") InsertResult result);

  /**
   * Inserts the values association within an {@link IValueConstraint}.
   *
   * @param valueConstraintId Value constraint ID.
   * @param values Values to insert.
   */
  void insertValueConstraintValues(
      @Param("valueConstraintId") long valueConstraintId,
      @Param("values") Set<String> values);

  /**
   * Inserts the requested ports association within an {@link ITaskConfig}.
   *
   * @param configId Task config ID.
   * @param ports Port names to insert.
   */
  void insertRequestedPorts(
      @Param("configId") long configId,
      @Param("ports") Set<String> ports);

  /**
   * Inserts the task links association within an {@link ITaskConfig}.
   *
   * @param configId Task config ID.
   * @param links Task links to insert.
   */
  void insertTaskLinks(@Param("configId") long configId, @Param("links") Map<String, String> links);

  /**
   * Inserts the container association within an {@link ITaskConfig}.
   *
   * @param configId Task config ID.
   * @param container Container to insert.
   */
  void insertContainer(
      @Param("configId") long configId,
      @Param("container") IDockerContainer container,
      @Param("result") InsertResult result);

  /**
   * Inserts docker parameters in association with an {@link IDockerContainer}.
   *
   * @param containerId Docker container row ID.
   * @param parameters Parameters to insert.
   */
  void insertDockerParameters(
      @Param("containerId") long containerId,
      @Param("parameters") List<IDockerParameter> parameters);

  /**
   * Inserts the metadata association within an {@link ITaskConfig}.
   *
   * @param configId Task config ID.
   * @param metadata Metadata associated with the task config.
   */
  void insertMetadata(
      @Param("configId") long configId,
      @Param("metadata") Set<IMetadata> metadata);

  /**
   * Inserts the Mesos Fetcher URIs in association with an {@link ITaskConfig}.
   *
   * @param configId Task config ID.
   * @param uris Resources Mesos Fetcher should place in sandbox.
   */
  void insertMesosFetcherUris(
      @Param("configId") long configId,
      @Param("uris") Set<IMesosFetcherURI> uris);

  /**
   * Deletes task configs.
   *
   * @param configIds Configs to delete.
   */
  void delete(@Param("configIds") Set<Long> configIds);

  /**
   * Inserts an AppC image association with an {@link ITaskConfig}.
   *
   * @param configId Task config ID.
   * @param name The name of the image.
   * @param imageId The image's identifier.
   */
  void insertAppcImage(
      @Param("configId") long configId,
      @Param("name") String name,
      @Param("imageId") String imageId);

  /**
   * Inserts a Docker image association with an {@link ITaskConfig}.
   *
   * @param configId Task config ID.
   * @param name The name of the image.
   * @param tag The image's tag.
   */
  void insertDockerImage(
      @Param("configId") long configId,
      @Param("name") String name,
      @Param("tag") String tag);

  /**
   * Inserts task resources.
   *
   * @param configId Task config ID.
   * @param values Resources to insert.
   */
  void insertResources(
      @Param("configId") long configId,
      @Param("values") List<Pair<Integer, String>> values);
}
