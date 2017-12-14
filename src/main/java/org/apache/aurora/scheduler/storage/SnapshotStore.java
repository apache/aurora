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
package org.apache.aurora.scheduler.storage;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.storage.Storage.StorageException;

/**
 * A storage component that applies full-state snapshots.
 */
public interface SnapshotStore {

  /**
   * Clean up the underlying storage by optimizing internal data structures. Does not change
   * externally-visible state but might not run concurrently with write operations.
   */
  void snapshot() throws StorageException;

  /**
   * Identical to {@link #snapshot()}, using a custom {@link Snapshot} rather than an
   * internally-generated one based on the current state.
   *
   * @param snapshot Snapshot to write.
   * @throws CodingException If the snapshot could not be serialized.
   */
  void snapshotWith(Snapshot snapshot) throws CodingException;
}
