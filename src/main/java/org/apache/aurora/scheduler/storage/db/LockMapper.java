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

import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.scheduler.storage.db.views.LockRow;

/**
 * MyBatis mapper class for LockMapper.xml
 *
 * See http://mybatis.github.io/mybatis-3/sqlmap-xml.html for more details.
 */
interface LockMapper {

  /**
   * Inserts a lock into the database.
   */
  void insert(Lock lock);

  /**
   * Deletes all locks from the database with the given lockKey.
   */
  void delete(LockKey lockKey);

  /**
   * Deletes all locks from the database.
   */
  void truncate();

  /**
   * Selects all locks from the database as {@link LockRow} instances.
   */
  List<LockRow> selectAll();

  /**
   * Fetches the lock with the given lock key.
   */
  LockRow select(LockKey lockKey);
}
