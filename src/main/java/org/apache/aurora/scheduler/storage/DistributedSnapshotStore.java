/**
 * Copyright 2013 Apache Software Foundation
 *
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

/**
 * A distributed snapshot store that supports persisting globally-visible snapshots.
 */
public interface DistributedSnapshotStore {
  /**
   * Writes a snapshot to the distributed storage system.
   * TODO(William Farner): Currently we're hiding some exceptions (which happen to be
   * RuntimeExceptions).  Clean these up to be checked, and throw another exception type here.
   *
   * @param snapshot Snapshot to write.
   * @throws CodingException If the snapshot could not be serialized.
   */
  void persist(Snapshot snapshot) throws CodingException;
}
