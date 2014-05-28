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

import org.apache.aurora.gen.JobKey;

/**
 * MyBatis mapper class for JobKeyMapper.xml
 *
 * See http://mybatis.github.io/mybatis-3/sqlmap-xml.html for more details.
 */
interface JobKeyMapper {
  /**
   * Inserts a new job key into the database.
   */
  void insert(JobKey key);

  /**
   * Permanently deletes a job key from the database.
   */
  void delete(JobKey key);

  /**
   * Selects all job keys from the database.
   */
  List<JobKey> selectAll();
}
