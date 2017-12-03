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
package org.apache.aurora.scheduler.storage.durability;

import java.util.stream.Stream;

import org.apache.aurora.gen.storage.Op;

/**
 * Persistence layer for storage operations.
 */
public interface Persistence {

  /**
   * Prepares the persistence layer.  The implementation may use this, for example, to advertise as
   * a replica to cohort schedulers, or begin syncing state for warm standby.
   */
  void prepare();

  /**
   * Recovers previously-persisted records.
   *
   * @return All persisted records.
   * @throws PersistenceException If recovery failed.
   */
  Stream<Op> recover() throws PersistenceException;

  /**
   * Saves new records.  No records may be considered durably saved until this method returns
   * successfully.
   *
   * @param records Records to save.
   * @throws PersistenceException If the records could not be saved.
   */
  void persist(Stream<Op> records) throws PersistenceException;

  /**
   * Thrown when a persistence operation fails.
   */
  class PersistenceException extends Exception {
    public PersistenceException(String msg) {
      super(msg);
    }

    public PersistenceException(Throwable cause) {
      super(cause);
    }

    public PersistenceException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
