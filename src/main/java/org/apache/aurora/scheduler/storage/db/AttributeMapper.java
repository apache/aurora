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

import javax.annotation.Nullable;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.ibatis.annotations.Param;

/**
 * MyBatis mapper interface for Attribute.xml.
 */
interface AttributeMapper {
  /**
   * Saves attributes for a host, based on {@link IHostAttributes#getHost()}.
   *
   * @param attributes Host attributes to save.
   */
  void insert(IHostAttributes attributes);

  /**
   * Deletes all attributes and attribute values associated with a slave.
   *
   * @param host Host to delete associated values from.
   */
  void deleteAttributeValues(@Param("host") String host);

  /**
   * Updates the mode and slave ID associated with a host.
   *
   * @param host Host to update.
   * @param mode New host maintenance mode.
   * @param slaveId New host slave ID.
   */
  void updateHostModeAndSlaveId(
      @Param("host") String host,
      @Param("mode") MaintenanceMode mode,
      @Param("slaveId") String slaveId);

  /**
   * Inserts values in {@link IHostAttributes#getAttributes()}, associating them with
   * {@link IHostAttributes#getSlaveId()}.
   *
   * @param attributes Attributes containing values to insert.
   */
  void insertAttributeValues(IHostAttributes attributes);

  /**
   * Retrieves the host attributes associated with a host.
   *
   * @param host Host to fetch attributes for.
   * @return Attributes associated with {@code host}, or {@code null} if no association exists.
   */
  @Nullable
  HostAttributes select(@Param("host") String host);

  /**
   * Retrieves all stored host attributes.
   *
   * @return All host attributes.
   */
  List<HostAttributes> selectAll();

  /**
   * Deletes all stored attributes and values.
   */
  void truncate();
}
