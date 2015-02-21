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

import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.ibatis.annotations.Param;

/**
 * MyBatis mapper class for JobInstanceUpdateEventMapper.xml
 *
 * See http://mybatis.github.io/mybatis-3/sqlmap-xml.html for more details.
 */
interface JobInstanceUpdateEventMapper {

  /**
   * Inserts a new job instance update event into the database.
   *
   * @param key Update key of the event.
   * @param event Event to insert.
   */
  void insert(@Param("key") IJobUpdateKey key, @Param("event") JobInstanceUpdateEvent event);
}
