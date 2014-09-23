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
package org.apache.aurora.scheduler.storage.log;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.scheduler.log.Log;

/**
 * Manages a single log stream append transaction.  Local storage ops can be added to the
 * transaction and then later committed as an atomic unit.
 */
interface StreamTransaction {
  /**
   * Appends any ops that have been added to this transaction to the log stream in a single
   * atomic record.
   *
   * @return The position of the log entry committed in this transaction, if any.
   * @throws CodingException If there was a problem encoding a log entry for commit.
   */
  Log.Position commit() throws ThriftBinaryCodec.CodingException;

  /**
   * Adds a local storage operation to this transaction.
   *
   * @param op The local storage op to add.
   */
  void add(Op op);
}
